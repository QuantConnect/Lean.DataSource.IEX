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

using QuantConnect.Util;
using QuantConnect.Logging;
using LaunchDarkly.EventSource;
using QuantConnect.IEX.Constants;
using System.Collections.Concurrent;

namespace QuantConnect.IEX
{
    /// <summary>
    /// Class wraps a collection of clients for getting data on SSE.
    /// SSE endpoints are limited to 50 symbols per connection. To consume more than 50 symbols we need multiple connections .
    /// </summary>
    public class IEXEventSourceCollection : IDisposable
    {
        /// <summary>
        /// Full url to subscription on event updates
        /// </summary>
        private readonly string dataStreamSubscriptionUrl;

        /// <summary>
        /// Message Event Handler to return update externally
        /// Depend from <see cref="dataStreamChannelName"/> to easy handle different event updates.
        /// </summary>
        private readonly EventHandler<(MessageReceivedEventArgs message, string channelName)> _messageAction;

        /// <summary>
        /// Represents an API key that is read-only once assigned.
        /// </summary>
        private readonly string _apiKey;

        /// <summary>
        /// Dictionary that associates EventSource instances with corresponding HashSet of ticker symbols, designed for concurrent access.
        /// The value, represented by a HashSet of strings, contains unique ticker symbols associated with each EventSource.
        /// The maximum limit of available ticker symbols per connection is determined by <see cref="MaximumSymbolsPerConnectionLimit"/>.
        /// </summary>
        protected readonly ConcurrentQueue<EventSource> EventSourceClients = new();

        /// <summary>
        /// Represents an AutoResetEvent used for synchronizing the context, waiting for the client to be open and successfully subscribed.
        /// </summary>
        public AutoResetEvent _subscriptionSyncEvent = new(false);

        /// <summary>
        /// Represents a RateGate instance used to control the rate of certain operations.
        /// </summary>
        private readonly RateGate _rateGate;

        /// <summary>
        /// Indicates whether a client is connected - i.e delivers any data.
        /// </summary>
        public bool IsConnected { get; private set; }

        /// <summary>
        /// The specific channel name for current instance of <see cref="IEXEventSourceCollection"/>
        /// </summary>
        public readonly string dataStreamChannelName;

        /// <summary>
        /// Occurs when the client returns an exception during the subscription process.
        /// </summary>
        public event EventHandler<string> SubscriptionFailure;

        /// <summary>
        /// Creates a new instance of <see cref="IEXEventSourceCollection"/>
        /// </summary>
        /// <param name="messageAction">Message Event handler by channel name.</param>
        /// <param name="apiKey">The Api-key of IEX cloud platform</param>
        /// <param name="subscriptionChannelName">The name of channel to subscription in current instance of <see cref="IEXEventSourceCollection"/></param>
        /// <param name="rateGate">RateGate instance used to control the rate of certain operations.</param>
        /// <param name="subscriptionFailure">the event indicates a problem with the subscription process, either in the form of interruption or error.</param>
        public IEXEventSourceCollection(EventHandler<(MessageReceivedEventArgs, string)> messageAction, string apiKey, string subscriptionChannelName,
            RateGate rateGate, EventHandler<string> subscriptionFailure)
        {
            _messageAction = messageAction;
            _apiKey = apiKey;
            dataStreamChannelName = subscriptionChannelName;
            dataStreamSubscriptionUrl = IEXDataStreamChannels.BaseDataStreamUrl + subscriptionChannelName;
            _rateGate = rateGate;
            SubscriptionFailure = subscriptionFailure;
        }

        /// <summary>
        /// Updates the data subscription to align with the current set of symbols to which the user is subscribed or unsubscribed.
        /// </summary>
        /// <param name="newSymbols">The symbols the user is currently subscribing to or unsubscribing from.</param>
        /// <exception cref="TimeoutException">Thrown when the operation times out.</exception>
        public void UpdateSubscription(IEnumerable<string> newSymbols)
        {
            // Remove and dispose old event source clients
            RemoveOldClient(EventSourceClients);

            // Create new client for every package (make sure that we do not exceed the rate-gate-limit while creating)
            foreach (var tickerChunk in newSymbols.Chunk(IEXDataStreamChannels.MaximumSymbolsPerConnectionLimit))
            {
                var client = CreateNewSubscription(tickerChunk);

                if (!_subscriptionSyncEvent.WaitOne(TimeSpan.FromSeconds(30)))
                {
                    throw new TimeoutException($"{nameof(IEXEventSourceCollection)}.{nameof(UpdateSubscription)}: Could not update subscription within a timeout");
                }

                // Add to the queue
                EventSourceClients.Enqueue(client);
            }

            Log.Debug($"{nameof(IEXEventSourceCollection)}.{nameof(UpdateSubscription)}.{dataStreamChannelName}: client amount = {EventSourceClients.Count}");

            IsConnected = true;
        }

        protected EventSource CreateNewSubscription(string[] symbols)
        {
            _rateGate.WaitToProceed();

            var client = CreateNewClient(symbols);

            // Set up the handlers
            client.Opened += (sender, args) =>
            {
                _subscriptionSyncEvent.Set();
                Log.Debug($"{nameof(IEXEventSourceCollection)}.{nameof(CreateNewSubscription)}.Event.Opened: Subscription to '{dataStreamChannelName}' was successfully");
            };

            client.MessageReceived += (sender, args) => _messageAction(sender, (args, dataStreamChannelName));

            // Error Codes dock: https://iexcloud.io/docs/api-basics/error-codes
            client.Error += (_, exceptionEventArgs) =>
            {
                SubscriptionFailure(_, exceptionEventArgs.Exception.Message);
                Log.Trace($"{nameof(IEXEventSourceCollection)}.{nameof(CreateNewSubscription)}.Event.Error: EventSource encountered an error. Details: {exceptionEventArgs.Exception.Message}");
            };

            client.Closed += (_, __) =>
                Log.Debug($"{nameof(IEXEventSourceCollection)}.{nameof(CreateNewSubscription)}.Event.Closed: The event source client has been closed. Initiating cleanup and closing procedures.");

            // Client start call will block until Stop() is called (!) - runs continuously in a background
            Task.Run(async () => await client.StartAsync());

            return client;
        }

        protected EventSource CreateNewClient(string[] symbols)
        {
            var url = $"{dataStreamSubscriptionUrl}?token={_apiKey}&symbols={string.Join(",", symbols)}";

            Log.Debug($"{nameof(IEXEventSourceCollection)}.{nameof(CreateNewClient)}: client built subscription URL: '{url}'");

            var client = new EventSource(LaunchDarkly.EventSource.Configuration.Builder(new Uri(url)).Build());
            return client;
        }

        private void RemoveOldClient(ConcurrentQueue<EventSource> eventSourceClients)
        {
            while (eventSourceClients.Count > 0)
            {
                if (eventSourceClients.TryDequeue(out var client))
                {
                    client.Close();
                    client.DisposeSafely();
                }
            }
        }

        public void Dispose()
        {
            RemoveOldClient(EventSourceClients);
            IsConnected = false;
        }
    }
}
