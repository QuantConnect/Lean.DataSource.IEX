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

namespace QuantConnect.Lean.DataSource.IEX.Constants
{
    /// <summary>
    /// Represents constants for accessing IEX Cloud data streams channels, specifically related to last sale and top-of-book information.
    /// The base URL for the data streams is: https://cloud-sse.iexapis.com/v1/
    /// For more details, refer to the official documentation:
    /// Last Sale: https://iexcloud.io/docs/data-streams#iex-last-sale
    /// Top of Book: https://iexcloud.io/docs/data-streams#iex-top-of-book
    /// </summary>
    public sealed class IEXDataStreamChannels
    {
        /// <summary>
        /// Maximum limit of available symbols allowed per connection.
        /// </summary>
        public const int MaximumSymbolsPerConnectionLimit = 50;

        /// <summary>
        /// Base URL for IEX Cloud data streams.
        /// </summary>
        public const string BaseDataStreamUrl = "https://cloud-sse.iexapis.com/v1/";

        /// <summary>
        /// Channel for last sale information.
        /// </summary>
        public const string LastSale = "last";

        /// <summary>
        /// Channel for top-of-book information.
        /// </summary>
        public const string TopOfBook = "tops";

        /// <summary>
        /// Provides an array of constants for accessing IEX Cloud data streams related to last sale and top-of-book information.
        /// </summary>
        public static readonly string[] Subscriptions = new string[2] { LastSale, TopOfBook };
    }
}
