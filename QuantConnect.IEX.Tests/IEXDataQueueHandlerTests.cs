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
using System.Threading;
using QuantConnect.Data;
using QuantConnect.Tests;
using QuantConnect.Logging;
using Microsoft.CodeAnalysis;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using System.Collections.Generic;
using System.Collections.Concurrent;

namespace QuantConnect.IEX.Tests
{
    [TestFixture]
    public class IEXDataQueueHandlerTests
    {
        private IEXDataQueueHandler iexDataQueueHandler;

        private static readonly string[] HardCodedSymbolsSNP = {
            "AAPL", "MSFT", "AMZN", "FB", "GOOGL", "GOOG", "BRK.B", "JNJ", "PG", "NVDA", "V", "JPM", "HD", "UNH", "MA", "VZ",
            "PYPL", "NFLX", "ADBE", "DIS", "CRM", "CMCSA", "PFE", "WMT", "MRK", "T", "INTC", "TMO", "ABT", "KO", "PEP", "BAC",
            "COST", "MCD", "NKE", "CSCO", "DHR", "NEE", "QCOM", "ABBV", "AVGO", "XOM", "ACN", "MDT", "TXN", "CVX", "BMY",
            "AMGN", "LOW", "UNP", "HON", "LIN", "UPS", "PM", "ORCL", "LLY", "SBUX", "AMT", "NOW", "IBM", "AMD", "MMM", "WFC",
            "C", "LMT", "CHTR", "BLK", "INTU", "CAT", "RTX", "ISRG", "BA", "SPGI", "FIS", "TGT", "ZTS", "MDLZ", "PLD", "GILD",
            "CVS", "DE", "ANTM", "MS", "MO", "ADP", "D", "DUK", "BDX", "SYK", "BKNG", "CCI", "EQIX", "CL", "GS", "FDX", "GE",
            "TMUS", "TJX", "SO", "APD", "ATVI", "CB", "CI", "SCHW", "CSX", "AXP", "REGN", "SHW", "ITW", "TFC", "MU", "AMAT",
            "VRTX", "ICE", "CME", "ADSK", "FISV", "PGR", "DG", "NSC", "HUM", "USB", "MMC", "LRCX", "EL", "NEM", "BSX", "GPN",
            "PNC", "ECL", "ILMN", "EW", "NOC", "KMB", "AEP", "GM", "ADI", "AON", "DD", "MCO", "WM", "ETN", "TWTR", "DLR",
            "BAX", "EXC", "BIIB", "CTSH", "EMR", "ROP", "IDXX", "XEL", "SRE", "EA", "GIS", "LHX", "PSA", "CMG", "DOW", "CNC",
            "APH", "COF", "SNPS", "HCA", "SBAC", "EBAY", "ORLY", "CMI", "DXCM", "TEL", "TT", "JCI", "A", "WEC", "KLAC", "COP",
            "ALGN", "TRV", "ROST", "CDNS", "F", "GD", "PPG", "INFO", "XLNX", "PEG", "ES", "TROW", "IQV", "PCAR", "BLL", "VRSK",
            "MSCI", "MET", "YUM", "MNST", "SYY", "CTAS", "BK", "ADM", "CARR", "STZ", "ALL", "MSI", "ZBH", "AWK", "ROK", "AIG",
            "MCHP", "PH", "APTV", "ANSS", "ED", "AZO", "SWK", "PAYX", "CLX", "ALXN", "TDG", "BBY", "RMD", "FCX", "KR", "PRU",
            "MAR", "OTIS", "FAST", "GLW", "HPQ", "SWKS", "CTVA", "WBA", "MTD", "HLT", "WLTW", "DTE", "LUV", "KMI", "MCK", "WMB",
            "WELL", "CPRT", "AFL", "DHI", "MKC", "AME", "VFC", "DLTR", "CERN", "EIX", "CHD", "PPL", "FRC", "WY", "FTV", "MKTX",
            "STT", "HSY", "WST", "ETR", "PSX", "O", "SLB", "AEE", "EOG", "LEN", "DAL", "DFS", "AJG", "KEYS", "KHC", "SPG",
            "AMP", "VRSN", "LH", "AVB", "VMC", "MXIM", "MPC", "LYB", "FLT", "RSG", "PAYC", "HOLX", "TTWO", "ODFL", "CMS",
            "ARE", "CDW", "IP", "FE", "CAG", "CBRE", "TSN", "EFX", "AMCR", "KSU", "FITB", "MLM", "DGX", "NTRS", "INCY", "DOV",
            "FTNT", "VIAC", "COO", "EQR", "ETSY", "BR", "GWW", "LVS", "K", "VAR", "ZBRA", "XYL", "TYL", "AKAM", "TSCO", "VLO",
            "EXR", "STE", "TFX", "EXPD", "DPZ", "PEAK", "QRVO", "VTR", "TER", "CTLT", "SIVB", "GRMN", "KMX", "NUE", "POOL",
            "PKI", "MAS", "CTXS", "WAT", "TIF", "NDAQ", "NVR", "HIG", "DRE", "ABC", "SYF", "HRL", "OKE", "PXD", "CE", "FMC",
            "CAH", "LNT", "GPC", "IR", "EXPE", "ESS", "AES", "MAA", "MTB", "RF", "BF.B", "EVRG", "SJM", "IEX", "KEY", "URI",
            "J", "NLOK", "BIO", "CHRW", "DRI", "CNP", "WHR", "AVY", "ULTA", "ABMD", "CFG", "TDY", "JKHY", "FBHS", "EMN", "WDC",
            "PHM", "HPE", "ANET", "ATO", "IFF", "LDOS", "PKG", "CINF", "HAS", "STX", "WAB", "IT", "HBAN", "HAL", "JBHT",
            "HES", "BXP", "AAP", "PFG", "ALB", "OMC", "NTAP", "XRAY", "UAL", "RCL", "WRK", "CPB", "LW", "RJF", "PNW", "BKR",
            "ALLE", "HSIC", "LKQ", "FOXA", "UDR", "MGM", "BWA", "PWR", "CBOE", "WU", "WRB", "SNA", "NI", "CXO", "PNR",
            "ROL", "LUMN", "UHS", "L", "RE", "FFIV", "TXT", "GL", "NRG", "OXY", "AIZ", "IRM", "HST", "MYL", "LB", "COG",
            "AOS", "IPG", "LYV", "WYNN", "CCL", "HWM", "IPGP", "JNPR", "DVA", "NWL", "MOS", "TPR", "DISH", "SEE", "TAP",
            "LNC", "CMA", "HII", "PRGO", "HBI", "CF", "RHI", "REG", "MHK", "AAL", "DISCK", "LEG", "ZION", "IVZ", "BEN", "NWSA",
            "NLSN", "FRT", "VNO", "DXC", "FLIR", "AIV", "PBCT", "ALK", "PVH", "KIM", "FANG", "FOX", "NCLH", "GPS", "VNT", "FLS",
            "UNM", "RL", "XRX", "DVN", "MRO", "DISCA", "NOV", "APA", "SLG", "HFC", "UAA", "UA", "FTI", "NWS"
        };


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

        protected static IEnumerable<TestCaseData> TestCaseDataSymbolsConfigs
        {
            get
            {
                var resolution = Resolution.Second;
                var configs = new Queue<SubscriptionDataConfig>();
                foreach (var symbol in new[] { Symbols.GOOG, Symbols.AAPL, Symbols.MSFT, Symbols.SPY })
                {
                    foreach (var config in GetSubscriptionDataConfigs(symbol, resolution))
                    {
                        configs.Enqueue(config);
                    }
                }

                yield return new TestCaseData(configs);
            }
        }

        /// <summary>
        /// Subscribe to multiple symbols in a series of calls
        /// </summary>
        [TestCaseSource(nameof(TestCaseDataSymbolsConfigs))]
        public void IEXCouldSubscribeManyTimes(Queue<SubscriptionDataConfig> configs)
        {
            var ticks = new List<BaseData>();

            foreach (var config in configs)
            {
                ProcessFeed(
                    iexDataQueueHandler.Subscribe(config, (s, e) => { }),
                    tick =>
                    {
                        if (tick != null)
                        {
                            switch (tick)
                            {
                                case TradeBar tradeBar:
                                    Assert.Greater(tradeBar.Open, 0m);
                                    Assert.Greater(tradeBar.High, 0m);
                                    Assert.Greater(tradeBar.Low, 0m);
                                    Assert.Greater(tradeBar.Close, 0m);
                                    Assert.IsTrue(tradeBar.DataType == MarketDataType.TradeBar);
                                    Assert.IsTrue(tradeBar.Period.ToHigherResolutionEquivalent(true) == config.Resolution);
                                    Assert.That(tradeBar.Time, Is.Not.EqualTo(DateTime.UnixEpoch));
                                    Assert.That(tradeBar.EndTime, Is.Not.EqualTo(DateTime.UnixEpoch));
                                    break;
                                case QuoteBar quoteBar:
                                    Assert.IsTrue(quoteBar.DataType == MarketDataType.QuoteBar);
                                    Assert.That(quoteBar.Time, Is.Not.EqualTo(DateTime.UnixEpoch));
                                    Assert.Greater(quoteBar.Price, 0m);
                                    Assert.Greater(quoteBar.Value, 0m);
                                    Assert.IsTrue(quoteBar.Period.ToHigherResolutionEquivalent(true) == config.Resolution);
                                    break;
                            }

                            ticks.Add(tick);
                        }
                    });
            }

            Thread.Sleep(20_000);

            foreach (var config in configs)
            {
                iexDataQueueHandler.Unsubscribe(config);
            }

            Thread.Sleep(20000);

            Assert.Greater(ticks.Count, 0);
        }

        [TestCaseSource(nameof(TestCaseDataSymbolsConfigs))]
        public void IEXSubscribeToSeveralThenUnSubscribeExceptOne(Queue<SubscriptionDataConfig> configs)
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var resetEvent = new ManualResetEvent(false);

            var tempDictionary = new ConcurrentDictionary<Symbol, int>();
            foreach (var config in configs)
            {
                tempDictionary[config.Symbol] = 0;
            }

            foreach (var config in configs)
            {
                ProcessFeed(
                    iexDataQueueHandler.Subscribe(config, (s, e) => { }),
                    tick =>
                    {
                        if (tick != null)
                        {
                            Log.Debug($"{nameof(IEXDataQueueHandlerTests)}: tick: {tick}");

                            tempDictionary.AddOrUpdate(tick.Symbol, 1, (id, count) => count + 1);

                            if (tempDictionary.All(count => count.Value > 5))
                            {
                                resetEvent.Set();
                            }
                        }
                    });
            }

            Assert.IsTrue(resetEvent.WaitOne(TimeSpan.FromSeconds(30), cancellationTokenSource.Token));

            // Unsubscribe from all symbols except one. (Why 2, cuz we have subscribe on Quote And Trade)
            for (int i = configs.Count; i > 2; i--)
            {
                configs.TryDequeue(out var config);
                iexDataQueueHandler.Unsubscribe(config);
                tempDictionary.TryRemove(config.Symbol, out _);
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));
            Assert.That(configs.Count, Is.EqualTo(2));

            resetEvent.Reset();
            tempDictionary = new ConcurrentDictionary<Symbol, int>();

            Assert.IsTrue(resetEvent.WaitOne(TimeSpan.FromSeconds(30), cancellationTokenSource.Token));

            for (int i = configs.Count; i > 0; i--)
            {
                configs.TryDequeue(out var config);
                iexDataQueueHandler.Unsubscribe(config);
            }

            Thread.Sleep(TimeSpan.FromSeconds(1));
            cancellationTokenSource.Cancel();
        }

        [Test]
        public void IEXCouldSubscribeAndUnsubscribe()
        {
            // MBLY is the most liquid IEX instrument
            var unsubscribed = false;
            Action<BaseData> callback = (tick) =>
            {
                if (tick == null)
                    return;

                Log.Trace(tick.ToString());
                if (unsubscribed && tick.Symbol.Value == "MBLY")
                {
                    Assert.Fail("Should not receive data for unsubscribed symbol");
                }
            };

            var configs = new[] {
                GetSubscriptionDataConfig<TradeBar>(Symbol.Create("MBLY", SecurityType.Equity, Market.USA), Resolution.Second),
                GetSubscriptionDataConfig<TradeBar>(Symbol.Create("USO", SecurityType.Equity, Market.USA), Resolution.Second)
            };

            Array.ForEach(configs, (c) =>
            {
                ProcessFeed(
                    iexDataQueueHandler.Subscribe(c, (s, e) => { }),
                    callback);
            });

            Thread.Sleep(20000);
            
            iexDataQueueHandler.Unsubscribe(Enumerable.First(configs, c => string.Equals(c.Symbol.Value, "MBLY")));

            Log.Trace("Unsubscribing");
            Thread.Sleep(2000);
            // some messages could be inflight, but after a pause all MBLY messages must have beed consumed by ProcessFeed
            unsubscribed = true;

            Thread.Sleep(20000);
        }

        [Test, Explicit("Tests are dependent on network and are long")]
        public void IEXCouldSubscribeMoreThan100Symbols()
        {
            var cancellationTokenSource = new CancellationTokenSource();
            var resetEvent = new AutoResetEvent(false);

            var responseDataCounter = 0;
            foreach (var ticker in HardCodedSymbolsSNP.Take(100))
            {
                foreach (var config in GetSubscriptionDataConfigs(ticker, Resolution.Second))
                {
                    iexDataQueueHandler.Subscribe(config, (s, e) => { });
                    ProcessFeed(
                        iexDataQueueHandler.Subscribe(config, (s, e) => { }),
                        tick =>
                        {
                            if (tick != null)
                            {
                                Log.Trace($"Response: {tick}");
                                responseDataCounter++;
                            }

                            if (responseDataCounter > 100)
                            {
                                resetEvent.Set();
                            }
                        });
                }
            }

            resetEvent.WaitOne(TimeSpan.FromMinutes(2), cancellationTokenSource.Token);
            Assert.IsTrue(iexDataQueueHandler.IsConnected);
        }

        private void ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null)
        {
            Task.Run(() =>
            {
                try
                {
                    while (enumerator.MoveNext())
                    {
                        BaseData tick = enumerator.Current;
                        callback?.Invoke(tick);

                        Thread.Sleep(200);
                    }
                }
                catch (AssertionException)
                {
                    throw;
                }
                catch (Exception err)
                {
                    Log.Error(err.Message);
                }
            });
        }

        private static IEnumerable<SubscriptionDataConfig> GetSubscriptionDataConfigs(string ticker, Resolution resolution)
        {
            var symbol = Symbol.Create(ticker, SecurityType.Equity, Market.USA);
            foreach (var subscription in GetSubscriptionDataConfigs(symbol, resolution))
            {
                yield return subscription;
            }
        }
        private static IEnumerable<SubscriptionDataConfig> GetSubscriptionDataConfigs(Symbol symbol, Resolution resolution)
        {
            yield return GetSubscriptionDataConfig<TradeBar>(symbol, resolution);
            yield return GetSubscriptionDataConfig<QuoteBar>(symbol, resolution);
        }

        private static SubscriptionDataConfig GetSubscriptionDataConfig<T>(Symbol symbol, Resolution resolution)
        {
            return new SubscriptionDataConfig(
                typeof(T),
                symbol,
                resolution,
                TimeZones.Utc,
                TimeZones.Utc,
                true,
                true,
                false);
        }
    }
}
