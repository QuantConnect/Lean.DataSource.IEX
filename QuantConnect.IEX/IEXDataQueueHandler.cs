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
using System.Text;
using Newtonsoft.Json;
using QuantConnect.Api;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Packets;
using QuantConnect.Logging;
using Newtonsoft.Json.Linq;
using System.Globalization;
using QuantConnect.Interfaces;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using QuantConnect.IEX.Response;
using QuantConnect.Configuration;
using QuantConnect.IEX.Constants;
using System.Security.Cryptography;
using System.Net.NetworkInformation;
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
        /// Represents an API key that is read-only once assigned.
        /// </summary>
        private readonly string _apiKey = Config.Get("iex-cloud-api-key");

        /// <summary>
        /// Object used as a synchronization lock for thread-safe operations.
        /// </summary>
        private object _lock = new object();

        /// <summary>
        /// Client instance to translate RestRequests into Http requests and process response result.
        /// </summary>
        private readonly RestClient _restClient = new();

        /// <summary>
        /// Represents a concurrent dictionary that stores <see cref="Symbol"/> with unique ticker keys.
        /// </summary>
        private readonly ConcurrentDictionary<string, Symbol> _symbols = new ConcurrentDictionary<string, Symbol>(StringComparer.InvariantCultureIgnoreCase);

        /// <summary>
        /// Represents a mapping of symbols to their corresponding time zones for exchange information.
        /// </summary>
        private readonly Dictionary<Symbol, DateTimeZone> _symbolExchangeTimeZones = new Dictionary<Symbol, DateTimeZone>();

        /// <summary>
        /// Gets the total number of data points emitted by this history provider.
        /// </summary>
        private int _dataPointCount;

        /// <summary>
        /// Aggregates ticks and bars based on given subscriptions.
        /// </summary>
        private readonly IDataAggregator _aggregator = Composer.Instance.GetExportedValueByTypeName<IDataAggregator>(
            Config.Get("data-aggregator", "QuantConnect.Lean.Engine.DataFeeds.AggregationManager"), forceTypeNameOnExisting: false);

        /// <summary>
        /// Handle subscription/unSubscription processes 
        /// </summary>
        private readonly EventBasedDataQueueHandlerSubscriptionManager _subscriptionManager;

        /// <summary>
        /// Returns whether the data provider is connected
        /// True if the data provider is connected
        /// </summary>
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

            ValidateSubscription();
        }

        private bool Subscribe(IEnumerable<Symbol> symbols, TickType _)
        {
            var tickers = new List<string>();
            foreach (var symbol in symbols)
            {
                if (!_symbols.TryAdd(symbol.Value, symbol))
                {
                    throw new InvalidOperationException($"Invalid logic, SubscriptionManager tried to subscribe to existing symbol : {symbol.Value}");
                }

                tickers.Add(symbol.Value);
            }

            foreach (var client in _clients)
            {
                client.UpdateSubscription(tickers);
            }

            return true;
        }

        private bool Unsubscribe(IEnumerable<Symbol> symbols, TickType _)
        {
            var tickers = new List<string>();
            foreach (var symbol in symbols)
            {
                _symbols.TryRemove(symbol.Value, out var _);
                tickers.Add(symbol.Value);
            }

            foreach (var client in _clients)
            {
                client.UpdateSubscription(tickers);
            }

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
            if (topOfBooks == null)
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

                var quoteTick = new Tick(
                    ConvertTickTimeBySymbol(symbol, topOfBook.LastUpdated),
                    symbol, saleCondition: string.Empty, symbol.ID.Market, topOfBook.BidSize, topOfBook.BidPrice, topOfBook.AskSize, topOfBook.AskPrice);

                lock (_lock)
                {
                    _aggregator.Update(quoteTick);
                }
            }
        }

        private void HandleLastSaleResponse(IEnumerable<DataStreamLastSale>? lastSales)
        {
            if (lastSales == null)
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
                    ConvertTickTimeBySymbol(symbol, lastSale.Time),
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
                throw new InvalidOperationException($"{nameof(IEXDataQueueHandler)}.{nameof(Subscribe)}: " +
                    $"Unfortunately no updates could be received from IEX outside the regular exchange open hours. " +
                          "Please be aware that only regular hours updates will be submitted to an algorithm.");
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

                foreach (var client in _clients)
                {
                    client.Dispose();
                }

            Log.Trace("IEXDataQueueHandler.Dispose(): Disconnected from IEX data provider");
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
            var start = ConvertTickTimeBySymbol(request.Symbol, request.StartTimeUtc);
            var end = ConvertTickTimeBySymbol(request.Symbol, request.EndTimeUtc);

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
            var now = ConvertTickTimeBySymbol(request.Symbol, DateTime.UtcNow);
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

        /// <summary>
        /// Converts the provided tick timestamp, given in milliseconds since Epoch, to the exchange time zone associated with the specified Lean symbol.
        /// </summary>
        /// <param name="symbol">The Lean symbol for which the timestamp is associated.</param>
        /// <param name="millisecondsEpochTime">The timestamp in milliseconds since Epoch.</param>
        /// <returns>A DateTime object representing the converted timestamp in the exchange time zone.</returns>
        private DateTime ConvertTickTimeBySymbol(Symbol symbol, long millisecondsEpochTime)
        {
            return ConvertTickTimeBySymbol(symbol, Time.UnixMillisecondTimeStampToDateTime(millisecondsEpochTime));
        }

        /// <summary>
        /// Converts the provided tick timestamp, given in DateTime UTC Kind, to the exchange time zone associated with the specified Lean symbol.
        /// </summary>
        /// <param name="symbol">The Lean symbol for which the timestamp is associated.</param>
        /// <param name="dateTimeUtc">The DateTime in Utc format Kind</param>
        /// <returns>A DateTime object representing the converted timestamp in the exchange time zone.</returns>
        private DateTime ConvertTickTimeBySymbol(Symbol symbol, DateTime dateTimeUtc)
        {
            if (!_symbolExchangeTimeZones.TryGetValue(symbol, out var exchangeTimeZone))
            {
                // read the exchange time zone from market-hours-database
                exchangeTimeZone = MarketHoursDatabase.FromDataFolder().GetExchangeHours(symbol.ID.Market, symbol, symbol.SecurityType).TimeZone;
                _symbolExchangeTimeZones.Add(symbol, exchangeTimeZone);
            }

            return dateTimeUtc.ConvertFromUtc(exchangeTimeZone);
        }

        private class ModulesReadLicenseRead : Api.RestResponse
        {
            [JsonProperty(PropertyName = "license")]
            public string License;

            [JsonProperty(PropertyName = "organizationId")]
            public string OrganizationId;
        }

        /// <summary>
        /// Validate the user of this project has permission to be using it via our web API.
        /// </summary>
        private static void ValidateSubscription()
        {
            try
            {
                const int productId = 333;
                var userId = Config.GetInt("job-user-id");
                var token = Config.Get("api-access-token");
                var organizationId = Config.Get("job-organization-id", null);
                // Verify we can authenticate with this user and token
                var api = new ApiConnection(userId, token);
                if (!api.Connected)
                {
                    throw new ArgumentException("Invalid api user id or token, cannot authenticate subscription.");
                }
                // Compile the information we want to send when validating
                var information = new Dictionary<string, object>()
                {
                    {"productId", productId},
                    {"machineName", Environment.MachineName},
                    {"userName", Environment.UserName},
                    {"domainName", Environment.UserDomainName},
                    {"os", Environment.OSVersion}
                };
                // IP and Mac Address Information
                try
                {
                    var interfaceDictionary = new List<Dictionary<string, object>>();
                    foreach (var nic in NetworkInterface.GetAllNetworkInterfaces().Where(nic => nic.OperationalStatus == OperationalStatus.Up))
                    {
                        var interfaceInformation = new Dictionary<string, object>();
                        // Get UnicastAddresses
                        var addresses = nic.GetIPProperties().UnicastAddresses
                            .Select(uniAddress => uniAddress.Address)
                            .Where(address => !IPAddress.IsLoopback(address)).Select(x => x.ToString());
                        // If this interface has non-loopback addresses, we will include it
                        if (!addresses.IsNullOrEmpty())
                        {
                            interfaceInformation.Add("unicastAddresses", addresses);
                            // Get MAC address
                            interfaceInformation.Add("MAC", nic.GetPhysicalAddress().ToString());
                            // Add Interface name
                            interfaceInformation.Add("name", nic.Name);
                            // Add these to our dictionary
                            interfaceDictionary.Add(interfaceInformation);
                        }
                    }
                    information.Add("networkInterfaces", interfaceDictionary);
                }
                catch (Exception)
                {
                    // NOP, not necessary to crash if fails to extract and add this information
                }
                // Include our OrganizationId if specified
                if (!string.IsNullOrEmpty(organizationId))
                {
                    information.Add("organizationId", organizationId);
                }
                var request = new RestRequest("modules/license/read", Method.POST) { RequestFormat = DataFormat.Json };
                request.AddParameter("application/json", JsonConvert.SerializeObject(information), ParameterType.RequestBody);
                api.TryRequest(request, out ModulesReadLicenseRead result);
                if (!result.Success)
                {
                    throw new InvalidOperationException($"Request for subscriptions from web failed, Response Errors : {string.Join(',', result.Errors)}");
                }

                var encryptedData = result.License;
                // Decrypt the data we received
                DateTime? expirationDate = null;
                long? stamp = null;
                bool? isValid = null;
                if (encryptedData != null)
                {
                    // Fetch the org id from the response if it was not set, we need it to generate our validation key
                    if (string.IsNullOrEmpty(organizationId))
                    {
                        organizationId = result.OrganizationId;
                    }
                    // Create our combination key
                    var password = $"{token}-{organizationId}";
                    var key = SHA256.HashData(Encoding.UTF8.GetBytes(password));
                    // Split the data
                    var info = encryptedData.Split("::");
                    var buffer = Convert.FromBase64String(info[0]);
                    var iv = Convert.FromBase64String(info[1]);
                    // Decrypt our information
                    using var aes = new AesManaged();
                    var decryptor = aes.CreateDecryptor(key, iv);
                    using var memoryStream = new MemoryStream(buffer);
                    using var cryptoStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read);
                    using var streamReader = new StreamReader(cryptoStream);
                    var decryptedData = streamReader.ReadToEnd();
                    if (!decryptedData.IsNullOrEmpty())
                    {
                        var jsonInfo = JsonConvert.DeserializeObject<JObject>(decryptedData);
                        expirationDate = jsonInfo["expiration"]?.Value<DateTime>();
                        isValid = jsonInfo["isValid"]?.Value<bool>();
                        stamp = jsonInfo["stamped"]?.Value<int>();
                    }
                }
                // Validate our conditions
                if (!expirationDate.HasValue || !isValid.HasValue || !stamp.HasValue)
                {
                    throw new InvalidOperationException("Failed to validate subscription.");
                }

                var nowUtc = DateTime.UtcNow;
                var timeSpan = nowUtc - Time.UnixTimeStampToDateTime(stamp.Value);
                if (timeSpan > TimeSpan.FromHours(12))
                {
                    throw new InvalidOperationException("Invalid API response.");
                }
                if (!isValid.Value)
                {
                    throw new ArgumentException($"Your subscription is not valid, please check your product subscriptions on our website.");
                }
                if (expirationDate < nowUtc)
                {
                    throw new ArgumentException($"Your subscription expired {expirationDate}, please renew in order to use this product.");
                }
            }
            catch (Exception e)
            {
                Log.Error($"PolygonDataQueueHandler.ValidateSubscription(): Failed during validation, shutting down. Error : {e.Message}");
                throw;
            }
        }
    }
}
