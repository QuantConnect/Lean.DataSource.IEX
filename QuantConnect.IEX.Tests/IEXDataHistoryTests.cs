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
using NodaTime;
using System.Linq;
using NUnit.Framework;
using QuantConnect.Data;
using QuantConnect.Util;
using QuantConnect.Tests;
using QuantConnect.Logging;
using System.Threading.Tasks;
using QuantConnect.Securities;
using QuantConnect.Data.Market;
using System.Collections.Generic;

namespace QuantConnect.Lean.DataSource.IEX.Tests
{
    [TestFixture]
    public class IEXDataHistoryTests
    {
        private static MarketHoursDatabase _marketHoursDatabase = MarketHoursDatabase.FromDataFolder();

        private IEXDataProvider iexDataProvider;

        [SetUp]
        public void SetUp()
        {
            iexDataProvider = new IEXDataProvider();
        }

        [TearDown]
        public void TearDown()
        {
            iexDataProvider.Dispose();
        }

        /// <summary>
        /// Provides test parameters for the TestMethod.
        /// </summary>
        /// <remarks>
        /// The test parameters include valid and invalid combinations of input data.
        /// </remarks>
        internal static IEnumerable<TestCaseData> ValidDataTestParameters
        {
            get
            {
                // Valid parameters
                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(15))
                    .SetDescription("Valid parameters - Daily resolution, 15 days period.")
                    .SetCategory("Valid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(5))
                    .SetDescription("Valid parameters - Minute resolution, 5 days period.")
                    .SetCategory("Valid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Minute, TickType.Trade, TimeSpan.FromDays(45))
                    .SetDescription("Valid parameters - Beyond 45 days, Minute resolution.")
                    .SetCategory("Valid");
            }
        }

        internal static IEnumerable<TestCaseData> InvalidDataTestParameters
        {
            get
            {
                // Invalid resolution - empty result
                yield return new TestCaseData(Symbols.SPY, Resolution.Tick, TickType.Trade, TimeSpan.FromSeconds(15))
                    .SetDescription("Invalid resolution - Tick resolution, 15 seconds period.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Second, TickType.Trade, Time.OneMinute)
                    .SetDescription("Invalid resolution - Second resolution, 1 minute period.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Hour, TickType.Trade, Time.OneDay)
                    .SetDescription("Invalid resolution - Hour resolution, 1 day period.")
                    .SetCategory("Invalid");

                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(-15))
                    .SetDescription("Invalid period - Date in the future, Daily resolution.")
                    .SetCategory("Invalid");

                // Invalid data type - empty result
                yield return new TestCaseData(Symbols.SPY, Resolution.Daily, TickType.Quote, TimeSpan.FromDays(15))
                    .SetDescription("Invalid data type - Daily resolution, QuoteBar data type.")
                    .SetCategory("Invalid");

                // Invalid security type, no exception, empty result
                yield return new TestCaseData(Symbols.EURUSD, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(15))
                    .SetDescription("Invalid security type - EURUSD symbol, Daily resolution.")
                    .SetCategory("Invalid");
            }
        }

        internal static IEnumerable<TestCaseData> SymbolDaysBeforeCaseData
        {
            get
            {
                yield return new TestCaseData(Symbols.SPY, 25);
                yield return new TestCaseData(Symbols.SPY, 30);
                yield return new TestCaseData(Symbols.SPY, 50);
                yield return new TestCaseData(Symbols.SPY, 90);
                yield return new TestCaseData(Symbols.SPY, 150);
                yield return new TestCaseData(Symbols.SPY, 175);
                yield return new TestCaseData(Symbols.SPY, 1826);
                yield return new TestCaseData(Symbols.SPY, 1799);
                yield return new TestCaseData(Symbols.SPY, 4383);
                yield return new TestCaseData(Symbols.SPY, 7305);
            }
        }

        [Explicit("This tests require a iexcloud.io api key")]
        [Test, TestCaseSource(nameof(SymbolDaysBeforeCaseData))]
        public void IEXCloudGetHistoryDailyForYears(Symbol symbol, int amountDaysBefore)
        {
            var slices = GetHistory(symbol, Resolution.Daily, TickType.Trade, TimeSpan.FromDays(amountDaysBefore)).ToList();

            Assert.IsNotNull(slices);
            Assert.Greater(slices.Count, 1);
        }

        [Test, TestCaseSource(nameof(InvalidDataTestParameters))]
        public void IEXCloudGetHistoryWithInvalidDataTestParameters(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var slices = GetHistory(symbol, resolution, tickType, period);

            Assert.IsNull(slices);
        }

        [Explicit("This tests require a iexcloud.io api key")]
        [Test, TestCaseSource(nameof(ValidDataTestParameters))]
        public void IEXCloudGetHistoryWithValidDataTestParameters(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var slices = GetHistory(symbol, resolution, tickType, period);

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
            Assert.Throws<ArgumentException>(() => GetHistory(symbol, resolution, tickType, period));
        }

        [Explicit("This tests require a iexcloud.io api key")]
        [TestCase(10)]
        [TestCase(20)]
        public void GetHistoryReturnsValidDataForMultipleConcurrentRequests(int amountOfTask)
        {
            var symbol = Symbols.SPY;
            var tickType = TickType.Trade;
            var resolution = Resolution.Minute;
            var period = TimeSpan.FromDays(10);

            var taskArray = new Task<List<Slice>>[amountOfTask];
            for (int i = 0; i < taskArray.Length; i++)
            {
                taskArray[i] = Task.Factory.StartNew(() => GetHistory(symbol, resolution, tickType, period).ToList());
            }

            Task.WaitAll(taskArray);

            foreach (var task in taskArray)
            {
                Assert.IsNotEmpty(task.Result);
            }
        }

        private Slice[] GetHistory(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var requests = new[] { CreateHistoryRequest(symbol, resolution, tickType, period) };

            var slices = iexDataProvider.GetHistory(requests, TimeZones.Utc)?.ToArray();
            Log.Trace("Data points retrieved: " + iexDataProvider.DataPointCount);
            Log.Trace("tick Type: " + tickType);
            return slices;
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, TimeSpan period)
        {
            var end = new DateTime(2024, 3, 15, 16, 0, 0);

            if (resolution == Resolution.Daily)
            {
                end = end.Date.AddDays(1);
            }

            return CreateHistoryRequest(symbol, resolution, tickType, end.Subtract(period), end);
        }

        internal static HistoryRequest CreateHistoryRequest(Symbol symbol, Resolution resolution, TickType tickType, DateTime startDateTime, DateTime endDateTime,
            SecurityExchangeHours exchangeHours = null, DateTimeZone dataTimeZone = null)
        {
            if (exchangeHours == null)
            {
                exchangeHours = SecurityExchangeHours.AlwaysOpen(TimeZones.NewYork);
            }

            if (dataTimeZone == null)
            {
                dataTimeZone = TimeZones.NewYork;
            }

            var dataType = LeanData.GetDataType(resolution, tickType);
            return new HistoryRequest(
                startTimeUtc: startDateTime,
                endTimeUtc: endDateTime,
                dataType: dataType,
                symbol: symbol,
                resolution: resolution,
                exchangeHours: exchangeHours,
                dataTimeZone: dataTimeZone,
                fillForwardResolution: null,
                includeExtendedMarketHours: true,
                isCustomData: false,
                dataNormalizationMode: DataNormalizationMode.Adjusted,
                tickType: tickType
                );
        }
    }
}
