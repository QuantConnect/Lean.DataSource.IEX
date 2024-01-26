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

using System;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Tests;
using QuantConnect.Logging;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Generic;

namespace QuantConnect.IEX.Tests
{
    [TestFixture]
    public class IEXDataHistoryTests
    {
        private IEXDataQueueHandler iexDataQueueHandler;

        [SetUp]
        public void SetUp()
        {
            iexDataQueueHandler = new IEXDataQueueHandler();
        }

        [TearDown]
        public void TearDown()
        {
            iexDataQueueHandler.Dispose();
        }

        /// <summary>
        /// Provides test parameters for the TestMethod.
        /// </summary>
        /// <remarks>
        /// The test parameters include valid and invalid combinations of input data.
        /// </remarks>
        internal static IEnumerable<TestCaseData> TestParameters
        {
            get
            {
                // Valid parameters
                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(15), true)
                    .SetDescription("Valid parameters - Daily resolution, 15 days period.")
                    .SetCategory("Valid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(5), true)
                    .SetDescription("Valid parameters - Minute resolution, 5 days period.")
                    .SetCategory("Valid");

                // Invalid resolution - empty result
                yield return new TestCaseData(Symbols.SPY, Resolution.Tick, TickType.Trade, TimeSpan.FromSeconds(15), false)
                    .SetDescription("Invalid resolution - Tick resolution, 15 seconds period.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Second, TickType.Trade, Time.OneMinute, false)
                    .SetDescription("Invalid resolution - Second resolution, 1 minute period.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Hour, TickType.Trade, Time.OneDay, false)
                    .SetDescription("Invalid resolution - Hour resolution, 1 day period.")
                    .SetCategory("Invalid");

                // Invalid period - empty result
                yield return new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(45), false)
                    .SetDescription("Invalid period - Beyond 30 days, Minute resolution.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(-15), false)
                    .SetDescription("Invalid period - Date in the future, Daily resolution.")
                    .SetCategory("Invalid");

                // Invalid data type - empty result
                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Quote, TimeSpan.FromDays(15), false)
                    .SetDescription("Invalid data type - Daily resolution, QuoteBar data type.")
                    .SetCategory("Invalid");

                // Invalid security type, no exception, empty result
                yield return new TestCaseData(Symbols.EURUSD, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(15), false)
                    .SetDescription("Invalid security type - EURUSD symbol, Daily resolution.")
                    .SetCategory("Invalid");
            }
        }

        [Test, TestCaseSource(nameof(TestParameters))]
        public void IEXCouldGetHistory(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period, bool received)
        {
            var slices = GetHistory(symbol, resolution, tickType, period);

            if (!received)
            {
                Assert.IsEmpty(slices);
                return;
            }

            Assert.IsNotEmpty(slices);

            foreach (var slice in slices)
            {
                foreach (var data in slice)
                {
                    AssertTradeBar(symbol, resolution, data.Value, data.Key);
                }
            }

            // And are ordered by time
            Assert.That(slices, Is.Ordered.By("Time"));
        }

        internal static void AssertTradeBar(Symbol expectedSymbol, Resolution resolution, BaseData baseData, Symbol actualSymbol = null)
        {
            if (actualSymbol != null)
            {
                Assert.That(actualSymbol, Is.EqualTo(expectedSymbol));
            }

            Assert.That(baseData.DataType, Is.EqualTo(MarketDataType.TradeBar));

            var tradeBar = baseData as TradeBar;
            Assert.IsNotNull(tradeBar);
            Assert.Greater(tradeBar.Open, 0);
            Assert.Greater(tradeBar.High, 0);
            Assert.Greater(tradeBar.Close, 0);
            Assert.Greater(tradeBar.Low, 0);
            Assert.That(tradeBar.Period.ToHigherResolutionEquivalent(true), Is.EqualTo(resolution));
        }

        /// <summary>
        /// Provides test data for scenarios involving an invalid symbol.
        /// </summary>
        /// <remarks>
        /// The test case includes an attempt to create a symbol ("XYZ") with an invalid combination of SecurityType and Market.
        /// </remarks>
        public static IEnumerable<TestCaseData> InvalidSymbolTestCaseData
        {
            get
            {
                yield return new TestCaseData(Symbol.Create("XYZ", SecurityType.Equity, Market.FXCM), Resolution.Daily, TickType.Trade, TimeSpan.FromDays(15))
                    .SetDescription("Invalid symbol - Attempt to create a symbol with an invalid combination of SecurityType and Market.")
                    .SetCategory("Invalid");
            }
        }

        [Test, TestCaseSource(nameof(InvalidSymbolTestCaseData))]
        public void GetHistoryInvalidSymbolThrowException(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            Assert.Throws<Exception>(() => GetHistory(symbol, resolution, tickType, period));
        }

        private Slice[] GetHistory(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var requests = new[] { CreateHistoryRequest(symbol, resolution, tickType, period) };

            var slices = iexDataQueueHandler.GetHistory(requests, TimeZones.Utc).ToArray();
            Log.Trace("Data points retrieved: " + iexDataQueueHandler.DataPointCount);
            Log.Trace("tick Type: " + tickType);
            return slices;
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var now = DateTime.UtcNow;

            var dataType = LeanData.GetDataType(resolution, tickType);

            return new HistoryRequest(now.Add(-period),
                                       now,
                                       dataType,
                                       symbol,
                                       resolution,
                                       SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork),
                                       TimeZones.NewYork,
                                       resolution,
                                       true,
                                       false,
                                       DataNormalizationMode.Raw,
                                       tickType);
        }
    }
}
