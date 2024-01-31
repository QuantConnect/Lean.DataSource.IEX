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
 *
*/

using Newtonsoft.Json;

namespace QuantConnect.IEX.Response
{
    /// <summary>
    /// Last provides trade data for executions on IEX. It is a near real time, intraday API that provides IEX last sale price, size and time. Last is ideal for developers that need a lightweight stock quote.
    /// </summary>
    /// <example>response: JSON: [{"symbol":"GOOG","price":153.5100,"size":100,"time":1706287632225,"seq":1629}]</example>
    /// <see href="https://iexcloud.io/docs/core/iex-last-sale"/>
    public readonly struct DataStreamLastSale
    {
        /// <summary>
        /// Refers to the stock ticker.
        /// </summary>
        [JsonProperty("symbol")]
        public string Symbol { get; }

        /// <summary>
        /// Refers to last sale price of the stock on IEX.
        /// </summary>
        [JsonProperty("price")]
        public decimal Price { get; }

        /// <summary>
        /// Refers to last sale size of the stock on IEX.
        /// </summary>
        [JsonProperty("size")]
        public decimal Size { get; }

        /// <summary>
        /// Refers to last sale time in milliseconds since Epoch of the stock on IEX.
        /// </summary>
        [JsonProperty("time")]
        public long Time { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataStreamLastSale"/> struct.
        /// </summary>
        /// <param name="symbol">The stock ticker symbol.</param>
        /// <param name="price">The last sale price of the stock on IEX.</param>
        /// <param name="size">The last sale size of the stock on IEX.</param>
        /// <param name="time">The last sale time in milliseconds since Epoch of the stock on IEX.</param>
        [JsonConstructor]
        public DataStreamLastSale(string symbol, decimal price, decimal size, long time)
        {
            Symbol = symbol;
            Price = price;
            Size = size;
            Time = time;
        }
    }
}
