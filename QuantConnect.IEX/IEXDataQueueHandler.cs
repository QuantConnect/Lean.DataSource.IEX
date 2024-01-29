/*
 * QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
 * Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

using NodaTime;
using RestSharp;
using System.Net;
using Newtonsoft.Json;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Packets;
using QuantConnect.Logging;
using Newtonsoft.Json.Linq;
using System.Globalization;
using QuantConnect.Interfaces;
using QuantConnect.Data.Market;
using QuantConnect.IEX.Response;
using QuantConnect.Configuration;
using QuantConnect.IEX.Constants;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using QuantConnect.Lean.Engine.DataFeeds;
using static QuantConnect.StringExtensions;
using QuantConnect.Lean.Engine.HistoricalData;

namespace QuantConnect.IEX
{
    /// <summary>
    /// IEX live data handler.
    /// See more at https://iexcloud.io/docs/api/
    /// </summary>
    public class IEXDataQueueHandler : SynchronizingHistoryProvider, IDataQueueHandler
    {
        /// <summary>
        /// Flag indicating whether a warning about unsupported data types in user history should be suppressed to prevent spam.
        /// </summary>
        private static bool _invalidHistoryDataTypeWarningFired;

        /// <summary>
        /// Represents two clients: one for the trade channel and another for the top-of-book channel.
        /// </summary>
        /// <see cref="IEXDataStreamChannels"/>
        private readonly List<IEXEventSourceCollection> _clients = new(2);

        /// <summary>
        /// Represents a reset event that facilitates updating user Subscriptions or UnSubscriptions on symbols.
        /// </summary>
        private readonly ManualResetEvent _refreshEvent = new ManualResetEvent(false);

        /// <summary>
        /// Represents an API key that is read-only once assigned.
        /// </summary>
        private readonly string _apiKey = Config.Get("iex-cloud-api-key");

        /// <summary>
        /// Object used as a synchronization lock for thread-safe operations.
        /// </summary>
        private object _lock = new object();

        /// <summary>
        /// Client instance to translate RestRequests into Http requests and process response result
        /// </summary>
        private readonly RestClient _restClient = new();

        private readonly ConcurrentDictionary<string, Symbol> _symbols = new ConcurrentDictionary<string, Symbol>(StringComparer.InvariantCultureIgnoreCase);
        private readonly ConcurrentDictionary<string, long> _iexLastTradeTime = new ConcurrentDictionary<string, long>();

        private readonly CancellationTokenSource _cancellationTokenSource = new CancellationTokenSource();
        private int _dataPointCount;

        private readonly IDataAggregator _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"));
        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        public bool IsConnected => _clients.All(client => client.IsConnected);

        /// <summary>
        /// Initializes a new instance of the <see cref="IEXDataQueueHandler"/> class.
        /// </summary>
        public IEXDataQueueHandler()
        {
            if (string.IsNullOrWhiteSpace(_apiKey))
            {
                throw new ArgumentException("Invalid or missing IEX API key. Please ensure that the API key is set and not empty.");
            }

            _subscriptionManager = new EventBasedDataQueueHandlerSubscriptionManager();

            _subscriptionManager.SubscribeImpl += Subscribe;
            _subscriptionManager.UnsubscribeImpl += Unsubscribe;

            // Set the sse-clients collection
            foreach (var channelName in IEXDataStreamChannels.Subscriptions)
            {
                _clients.Add(new IEXEventSourceCollection(
                    (o, message) => ProcessJsonObject(message.Item1.Message.Data, message.Item2),
                    _apiKey,
                    channelName));
            }

            // Initiates the stream listener task.
            StreamAction();
        }

        private void StreamAction()
        {
            // In this thread, we check at each interval whether the client needs to be updated
            // Subscription renewal requests may come in dozens and all at relatively same time - we cannot update them one by one when work with SSE
            Task.Factory.StartNew(() =>
            {
                while (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    try
                    {
                        _refreshEvent.WaitOne(-1, _cancellationTokenSource.Token);

                        foreach (var client in _clients)
                        {
                            client.UpdateSubscription(_symbols.Keys.ToArray());
                        }

                        _refreshEvent.Reset();
                    }
                    catch (Exception ex)
                    {
                        throw new Exception($"{nameof(IEXDataQueueHandler)}.{nameof(StreamAction)}: {ex}");
                    }
                }
                Log.Debug($"{nameof(IEXDataQueueHandler)}.{nameof(StreamAction)}: End");
            }, _cancellationTokenSource.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private bool Subscribe(IEnumerable<Symbol> symbols, TickType _)
        {
            foreach (var symbol in symbols)
            {
                if (!_symbols.TryAdd(symbol.Value, symbol))
                {
                    throw new InvalidOperationException($"Invalid logic, SubscriptionManager tried to subscribe to existing symbol : {symbol.Value}");
                }
            }

            Refresh();
            return true;
        }

        private bool Unsubscribe(IEnumerable<Symbol> symbols, TickType _)
        {
            foreach (var symbol in symbols)
            {
                _symbols.TryRemove(symbol.Value, out var _);
            }

            Refresh();
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ProcessJsonObject(string json, string channelName)
        {
            // Log.Debug($"{nameof(IEXDataQueueHandler)}.{nameof(ProcessJsonObject)}: Channel:{channelName}, Data:{json}");

            if (json == "[]")
            {
                return;
            }

            switch (channelName)
            {
                case IEXDataStreamChannels.LastSale:
                    HandleLastSaleResponse(JsonConvert.DeserializeObject<IEnumerable<DataStreamLastSale>>(json));
                    break;
                case IEXDataStreamChannels.TopOfBook:
                    HandleTopOfBookResponse(JsonConvert.DeserializeObject<IEnumerable<DataStreamTopOfBook>>(json));
                    break;
            }
        }

        private void HandleTopOfBookResponse(IEnumerable<DataStreamTopOfBook>? topOfBooks)
        {
            if (topOfBooks == null || !topOfBooks.Any())
            {
                return;
            }

            foreach (var topOfBook in topOfBooks)
            {
                if (topOfBook.AskPrice == 0 || topOfBook.BidPrice == 0)
                {
                    continue;
                }

                if (!_symbols.TryGetValue(topOfBook.Symbol, out var symbol))
                {
                    // Symbol is no loner in dictionary, it may be the stream not has been updated yet,
                    // and the old client is still sending messages for unsubscribed symbols -
                    // so there can be residual messages for the symbol, which we must skip
                    continue;
                }

                // -1 if IEX has not quoted the symbol in the trading day
                if (topOfBook.LastUpdated <= 0)
                {
                    continue;
                }

                var time = Time.UnixMillisecondTimeStampToDateTime(topOfBook.LastUpdated).ConvertFromUtc(TimeZones.NewYork);

                var quoteTick = new Tick(
                    time,
                    symbol, saleCondition: string.Empty, symbol.ID.Market, topOfBook.BidSize, topOfBook.BidPrice, topOfBook.AskSize, topOfBook.AskPrice);

                lock (_lock)
                {
                    Log.Debug($"{nameof(IEXDataQueueHandler)}.{nameof(HandleTopOfBookResponse)}: in lock timeUpdate:{time}");

                    _aggregator.Update(quoteTick);
                }
            }
        }

        private void HandleLastSaleResponse(IEnumerable<DataStreamLastSale>? lastSales)
        {
            if (lastSales == null || !lastSales.Any())
            {
                return;
            }

            foreach (var lastSale in lastSales)
            {
                if (!_symbols.TryGetValue(lastSale.Symbol, out var symbol))
                {
                    // Symbol is no loner in dictionary, it may be the stream not has been updated yet,
                    // and the old client is still sending messages for unsubscribed symbols -
                    // so there can be residual messages for the symbol, which we must skip
                    continue;
                }

                var tradeTick = new Tick(
                    Time.UnixMillisecondTimeStampToDateTime(lastSale.Time).ConvertFromUtc(TimeZones.NewYork),
                    symbol, saleCondition: string.Empty, symbol.ID.Market, lastSale.Size, lastSale.Price);

                lock (_lock)
                {
                    _aggregator.Update(tradeTick);
                }
            }
        }

        /// <summary>
        /// Subscribe to the specified configuration
        /// </summary>
        /// <param name="dataConfig">defines the parameters to subscribe to a data feed</param>
        /// <param name="newDataAvailableHandler">handler to be fired on new data available</param>
        /// <returns>The new enumerator for this subscription request</returns>
        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, EventHandler newDataAvailableHandler)
        {
            if (dataConfig.ExtendedMarketHours)
            {
                Log.Error("IEXDataQueueHandler.Subscribe(): Unfortunately no updates could be received from IEX outside the regular exchange open hours. " +
                          "Please be aware that only regular hours updates will be submitted to an algorithm.");
            }

            if (dataConfig.Resolution < Resolution.Second)
            {
                Log.Error($"IEXDataQueueHandler.Subscribe(): Selected data resolution ({dataConfig.Resolution}) " +
                          "is not supported by current implementation of  IEXDataQueueHandler. Sorry. Please try the higher resolution.");
            }

            if (!CanSubscribe(dataConfig.Symbol))
            {
                return null;
            }

            var enumerator = _aggregator.Add(dataConfig, newDataAvailableHandler);
            _subscriptionManager.Subscribe(dataConfig);

            return enumerator;
        }

        /// <summary>
        /// Sets the job we're subscribing for
        /// </summary>
        /// <param name="job">Job we're subscribing for</param>
        public void SetJob(LiveNodePacket job)
        {
        }

        /// <summary>
        /// Checks if this brokerage supports the specified symbol
        /// </summary>
        /// <param name="symbol">The symbol</param>
        /// <returns>returns true if brokerage supports the specified symbol; otherwise false</returns>
        private static bool CanSubscribe(Symbol symbol)
        {
            return symbol.SecurityType == SecurityType.Equity;
        }

        private void Refresh()
        {
            _refreshEvent.Set();
        }

        /// <summary>
        /// Removes the specified configuration
        /// </summary>
        /// <param name="dataConfig">Subscription config to be removed</param>
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptionManager.Unsubscribe(dataConfig);
            _aggregator.Remove(dataConfig);
        }

        /// <summary>
        /// Dispose connection to IEX
        /// </summary>
        public void Dispose()
        {
            _aggregator.DisposeSafely();
            _cancellationTokenSource.Cancel();

            if (!_symbols.IsEmpty)
            {
                foreach (var client in _clients)
                {
                    client.UpdateSubscription(_symbols.Keys.ToArray());
                }
            }

            Log.Trace("IEXDataQueueHandler.Dispose(): Disconnected from IEX data provider");

            GC.SuppressFinalize(this);
        }

        #region IHistoryProvider implementation

        /// <summary>
        /// Gets the total number of data points emitted by this history provider
        /// </summary>
        public override int DataPointCount => _dataPointCount;

        /// <summary>
        /// Initializes this history provider to work for the specified job
        /// </summary>
        /// <param name="parameters">The initialization parameters</param>
        public override void Initialize(HistoryProviderInitializeParameters parameters)
        {
        }

        /// <summary>
        /// Gets the history for the requested securities
        /// </summary>
        /// <param name="requests">The historical data requests</param>
        /// <param name="sliceTimeZone">The time zone used when time stamping the slice instances</param>
        /// <returns>An enumerable of the slices of data covering the span specified in each request</returns>
        public override IEnumerable<Slice> GetHistory(IEnumerable<Data.HistoryRequest> requests, DateTimeZone sliceTimeZone)
        {
            if (string.IsNullOrWhiteSpace(_apiKey))
            {
                Log.Error($"{nameof(IEXDataQueueHandler)}.{nameof(GetHistory)}: History calls for IEX require an API key.");
                return Enumerable.Empty<Slice>();
            }

            // Create subscription objects from the configs
            var subscriptions = new List<Subscription>();
            foreach (var request in requests)
            {
                // IEX does return historical TradeBar - give one time warning if inconsistent data type was requested
                if (request.TickType != TickType.Trade)
                {
                    if (!_invalidHistoryDataTypeWarningFired)
                    {
                        Log.Error($"{nameof(IEXDataQueueHandler)}.{nameof(GetHistory)}: Not supported data type - {request.DataType.Name}. " +
                            "Currently available support only for historical of type - TradeBar");
                        _invalidHistoryDataTypeWarningFired = true;
                    }
                    return Enumerable.Empty<Slice>();
                }

                if (request.Symbol.SecurityType != SecurityType.Equity)
                {
                    Log.Trace($"{nameof(IEXDataQueueHandler)}.{nameof(GetHistory)}: Unsupported SecurityType '{request.Symbol.SecurityType}' for symbol '{request.Symbol}'");
                    return Enumerable.Empty<Slice>();
                }

                var history = ProcessHistoryRequests(request);
                var subscription = CreateSubscription(request, history);
                subscriptions.Add(subscription);
            }

            return CreateSliceEnumerableFromSubscriptions(subscriptions, sliceTimeZone);
        }

        /// <summary>
        /// Populate request data
        /// </summary>
        private IEnumerable<BaseData> ProcessHistoryRequests(Data.HistoryRequest request)
        {
            var ticker = request.Symbol.ID.Symbol;
            var start = request.StartTimeUtc.ConvertFromUtc(TimeZones.NewYork);
            var end = request.EndTimeUtc.ConvertFromUtc(TimeZones.NewYork);

            if (request.Resolution == Resolution.Minute && start <= DateTime.Today.AddDays(-30))
            {
                Log.Error("IEXDataQueueHandler.GetHistory(): History calls with minute resolution for IEX available only for trailing 30 calendar days.");
                yield break;
            }

            if (request.Resolution != Resolution.Daily && request.Resolution != Resolution.Minute)
            {
                Log.Error("IEXDataQueueHandler.GetHistory(): History calls for IEX only support daily & minute resolution.");
                yield break;
            }

            Log.Trace("IEXDataQueueHandler.ProcessHistoryRequests(): Submitting request: " +
                Invariant($"{request.Symbol.SecurityType}-{ticker}: {request.Resolution} {start}->{end}") +
                ". Please wait..");

            const string baseUrl = "https://cloud.iexapis.com/stable/stock";
            var now = DateTime.UtcNow.ConvertFromUtc(TimeZones.NewYork);
            var span = now - start;
            var urls = new List<string>();

            switch (request.Resolution)
            {
                case Resolution.Minute:
                    {
                        var begin = start;
                        while (begin < end)
                        {
                            var url =
                                $"{baseUrl}/{ticker}/chart/date/{begin.ToStringInvariant("yyyyMMdd")}?token={_apiKey}";
                            urls.Add(url);
                            begin = begin.AddDays(1);
                        }

                        break;
                    }
                case Resolution.Daily:
                    {
                        string suffix;
                        if (span.Days < 30)
                        {
                            suffix = "1m";
                        }
                        else if (span.Days < 3 * 30)
                        {
                            suffix = "3m";
                        }
                        else if (span.Days < 6 * 30)
                        {
                            suffix = "6m";
                        }
                        else if (span.Days < 12 * 30)
                        {
                            suffix = "1y";
                        }
                        else if (span.Days < 24 * 30)
                        {
                            suffix = "2y";
                        }
                        else if (span.Days < 60 * 30)
                        {
                            suffix = "5y";
                        }
                        else
                        {
                            suffix = "max";   // max is 15 years
                        }

                        var url =
                            $"{baseUrl}/{ticker}/chart/{suffix}?token={_apiKey}";
                        urls.Add(url);

                        break;
                    }
            }

            // Download and parse data
            var responses = new List<string>();

            foreach (var url in urls)
            {
                var response = ExecuteGetRequest(url);
                responses.Add(response);
            }

            foreach (var response in responses)
            {
                var parsedResponse = JArray.Parse(response);

                // Parse
                foreach (var item in parsedResponse.Children())
                {
                    DateTime date;
                    TimeSpan period;
                    if (item["minute"] != null)
                    {
                        date = DateTime.ParseExact(item["date"].Value<string>(), "yyyy-MM-dd", CultureInfo.InvariantCulture);
                        var minutes = TimeSpan.ParseExact(item["minute"].Value<string>(), "hh\\:mm", CultureInfo.InvariantCulture);
                        date += minutes;
                        period = TimeSpan.FromMinutes(1);
                    }
                    else
                    {
                        date = Parse.DateTime(item["date"].Value<string>());
                        period = TimeSpan.FromDays(1);
                    }

                    if (date < start || date > end)
                    {
                        continue;
                    }

                    Interlocked.Increment(ref _dataPointCount);

                    if (item["open"].Type == JTokenType.Null)
                    {
                        continue;
                    }

                    decimal open, high, low, close, volume;

                    if (request.Resolution == Resolution.Daily &&
                        request.DataNormalizationMode == DataNormalizationMode.Raw)
                    {
                        open = item["uOpen"].Value<decimal>();
                        high = item["uHigh"].Value<decimal>();
                        low = item["uLow"].Value<decimal>();
                        close = item["uClose"].Value<decimal>();
                        volume = item["uVolume"].Value<int>();
                    }
                    else
                    {
                        open = item["open"].Value<decimal>();
                        high = item["high"].Value<decimal>();
                        low = item["low"].Value<decimal>();
                        close = item["close"].Value<decimal>();
                        volume = item["volume"].Value<int>();
                    }

                    var tradeBar = new TradeBar(date, request.Symbol, open, high, low, close, volume, period);

                    yield return tradeBar;
                }
            }
        }

        /// <summary>
        /// Executes a REST GET request and retrieves the response content.
        /// </summary>
        /// <param name="url">The full URI, including scheme, host, path, and query parameters (e.g., {scheme}://{host}/{path}?{query}).</param>
        /// <returns>The response content as a string.</returns>
        /// <exception cref="Exception">Thrown when the GET request fails or returns a non-OK status code.</exception>
        private string ExecuteGetRequest(string url)
        {
            var response = _restClient.Execute(new RestRequest(url, Method.GET));

            if (response.StatusCode != HttpStatusCode.OK)
            {
                throw new Exception($"{nameof(IEXDataQueueHandler)}.{nameof(ProcessHistoryRequests)} failed: [{(int)response.StatusCode}] {response.StatusDescription}, Content: {response.Content}, ErrorMessage: {response.ErrorMessage}");
            }

            return response.Content;
        }

        #endregion
    }
}
