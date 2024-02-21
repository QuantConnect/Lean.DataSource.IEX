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

namespace QuantConnect.Lean.DataSource.IEX.Response
{
    /// <summary>
    /// TOPS provides aggregated best-quoted bid and offer position in near real time for all securities on displayed limit order book from IEX. TOPS is ideal for developers needing both quote and trade data.
    /// <see href="https://iexcloud.io/docs/core/IEX_TOPS"/>
    /// </summary>
    public readonly struct DataStreamTopOfBook
    {
        /// <summary>
        /// Stock ticker.
        /// </summary>
        [JsonProperty("symbol")]
        public string Symbol { get; }

        /// <summary>
        /// Sector the security belongs to.
        /// </summary>
        [JsonProperty("sector")]
        public string Sector { get; }

        /// <summary>
        /// Common issue type.
        /// </summary>
        [JsonProperty("securityType")]
        public string SecurityType { get; }

        /// <summary>
        /// Best bid price on IEX.
        /// </summary>
        [JsonProperty("bidPrice")]
        public decimal BidPrice { get; }

        /// <summary>
        /// Number of shares on the bid on IEX.
        /// </summary>
        [JsonProperty("bidSize")]
        public decimal BidSize { get; }

        /// <summary>
        /// Best ask price on IEX.
        /// </summary>
        [JsonProperty("askPrice")]
        public decimal AskPrice { get; }

        /// <summary>
        /// Number of shares on the ask on IEX.
        /// </summary>
        [JsonProperty("askSize")]
        public decimal AskSize { get; }

        /// <summary>
        /// Last update time of the data in milliseconds since Epoch or -1 if IEX has not quoted the symbol in the trading day.
        /// </summary>
        [JsonProperty("lastUpdated")]
        public long LastUpdated { get; }

        /// <summary>
        /// Last sale price of the stock on IEX.
        /// </summary>
        [JsonProperty("lastSalePrice")]
        public decimal LastSalePrice { get; }

        /// <summary>
        /// Last sale size of the stock on IEX.
        /// </summary>
        [JsonProperty("lastSaleSize")]
        public decimal LastSaleSize { get; }

        /// <summary>
        /// Last sale time in milliseconds since Epoch of the stock on IEX.
        /// </summary>
        [JsonProperty("lastSaleTime")]
        public long LastSaleTime { get; }

        /// <summary>
        /// Shares traded in the stock on IEX.
        /// </summary>
        [JsonProperty("volume")]
        public long Volume { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataStreamTopOfBook"/> struct.
        /// </summary>
        /// <param name="symbol">Stock ticker.</param>
        /// <param name="sector">Sector the security belongs to.</param>
        /// <param name="securityType">Common issue type.</param>
        /// <param name="bidPrice">Best bid price on IEX.</param>
        /// <param name="bidSize">Number of shares on the bid on IEX.</param>
        /// <param name="askPrice">Best ask price on IEX.</param>
        /// <param name="askSize">Number of shares on the ask on IEX.</param>
        /// <param name="lastUpdated">Last update time of the data in milliseconds since Epoch or -1 if IEX has not quoted the symbol in the trading day.</param>
        /// <param name="lastSalePrice">Last sale price of the stock on IEX. </param>
        /// <param name="lastSaleSize">Last sale size of the stock on IEX.</param>
        /// <param name="lastSaleTime">Last sale time in milliseconds since Epoch of the stock on IEX.</param>
        /// <param name="volume">Shares traded in the stock on IEX.</param>
        [JsonConstructor]
        public DataStreamTopOfBook(string symbol, string sector, string securityType, decimal bidPrice, decimal bidSize, decimal askPrice, decimal askSize,
            long lastUpdated, decimal lastSalePrice, decimal lastSaleSize, long lastSaleTime, long volume)
        {
            Symbol = symbol;
            Sector = sector;
            SecurityType = securityType;
            BidPrice = bidPrice;
            BidSize = bidSize;
            AskPrice = askPrice;
            AskSize = askSize;
            LastUpdated = lastUpdated;
            LastSalePrice = lastSalePrice;
            LastSaleSize = lastSaleSize;
            LastSaleTime = lastSaleTime;
            Volume = volume;
        }
    }
}
