﻿/*
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
using QuantConnect.Packets;
using QuantConnect.Logging;
using Microsoft.CodeAnalysis;
using System.Threading.Tasks;
using QuantConnect.Data.Market;
using QuantConnect.Configuration;
using System.Collections.Generic;
using System.Collections.Concurrent;
using QuantConnect.Data.UniverseSelection;

namespace QuantConnect.Lean.DataSource.IEX.Tests
{
    [TestFixture, Explicit("This tests require a iexcloud.io api key")]
    public class IEXDataProviderTests
    {
        private readonly string _apiKey = Config.Get("iex-cloud-api-key");
        private readonly string _pricePlan = Config.Get("iex-cloud-api-key");

        private CancellationTokenSource _cancellationTokenSource;
        private IEXDataProvider iexDataProvider;

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
            _cancellationTokenSource = new();
            iexDataProvider = new IEXDataProvider();
        }

        [TearDown]
        public void TearDown()
        {
            if (iexDataProvider != null)
            {
                iexDataProvider.Dispose();
            }

            _cancellationTokenSource.Dispose();
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
                    iexDataProvider.Subscribe(config, (s, e) => { }),
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
                    },
                    () => _cancellationTokenSource.Cancel());
            }

            Assert.IsFalse(_cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20)), "The cancellation token was cancelled block thread.");

            foreach (var config in configs)
            {
                iexDataProvider.Unsubscribe(config);
            }

            _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20));

            Assert.Greater(ticks.Count, 0);
        }

        [TestCaseSource(nameof(TestCaseDataSymbolsConfigs))]
        public void IEXSubscribeToSeveralThenUnSubscribeExceptOne(Queue<SubscriptionDataConfig> configs)
        {
            var resetEvent = new ManualResetEvent(false);

            var tempDictionary = new ConcurrentDictionary<Symbol, int>();
            foreach (var config in configs)
            {
                tempDictionary[config.Symbol] = 0;
            }

            foreach (var config in configs)
            {
                ProcessFeed(
                    iexDataProvider.Subscribe(config, (s, e) => { }),
                    tick =>
                    {
                        if (tick != null)
                        {
                            Log.Debug($"{nameof(IEXDataProviderTests)}: tick: {tick}");

                            tempDictionary.AddOrUpdate(tick.Symbol, 1, (id, count) => count + 1);

                            if (tempDictionary.All(count => count.Value > 5))
                            {
                                resetEvent.Set();
                            }
                        }
                    },
                    () => _cancellationTokenSource.Cancel());
            }

            Assert.IsTrue(resetEvent.WaitOne(TimeSpan.FromSeconds(30), _cancellationTokenSource.Token));

            // Unsubscribe from all symbols except one. (Why 2, cuz we have subscribe on Quote And Trade)
            for (int i = configs.Count; i > 2; i--)
            {
                configs.TryDequeue(out var config);
                iexDataProvider.Unsubscribe(config);
                tempDictionary.TryRemove(config.Symbol, out _);
            }

            Thread.Sleep(TimeSpan.FromSeconds(10));
            Assert.That(configs.Count, Is.EqualTo(2));

            resetEvent.Reset();
            tempDictionary = new ConcurrentDictionary<Symbol, int>();

            Assert.IsTrue(resetEvent.WaitOne(TimeSpan.FromSeconds(30), _cancellationTokenSource.Token));

            for (int i = configs.Count; i > 0; i--)
            {
                configs.TryDequeue(out var config);
                iexDataProvider.Unsubscribe(config);
            }

            Thread.Sleep(TimeSpan.FromSeconds(1));
            _cancellationTokenSource.Cancel();
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

            Action throwExceptionCallback = () =>
            {
                _cancellationTokenSource.Cancel();
            };

            var configs = new[] {
                GetSubscriptionDataConfig<TradeBar>(Symbol.Create("MBLY", SecurityType.Equity, Market.USA), Resolution.Second),
                GetSubscriptionDataConfig<TradeBar>(Symbol.Create("USO", SecurityType.Equity, Market.USA), Resolution.Second)
            };

            Array.ForEach(configs, (c) =>
            {
                ProcessFeed(
                    iexDataProvider.Subscribe(c, (s, e) => { }),
                    callback,
                    throwExceptionCallback);
            });

            Assert.IsFalse(_cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20)), "The cancellation token was cancelled block thread.");

            iexDataProvider.Unsubscribe(Enumerable.First(configs, c => string.Equals(c.Symbol.Value, "MBLY")));

            Log.Trace("Unsubscribing");
            _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(2));
            // some messages could be inflight, but after a pause all MBLY messages must have beed consumed by ProcessFeed
            unsubscribed = true;

            _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20));
        }

        [Test]
        public void IEXCouldSubscribeMoreThan100Symbols()
        {
            var resetEvent = new AutoResetEvent(false);

            foreach (var ticker in HardCodedSymbolsSNP.Take(250))
            {
                foreach (var config in GetSubscriptionDataConfigs(ticker, Resolution.Second))
                {
                    ProcessFeed(iexDataProvider.Subscribe(config, (s, e) => { }), throwExceptionCallback: () => _cancellationTokenSource.Cancel());
                }
            }

            Assert.IsFalse(resetEvent.WaitOne(TimeSpan.FromMinutes(2), _cancellationTokenSource.Token), "The cancellation token was cancelled block thread.");
            Assert.IsTrue(iexDataProvider.IsConnected);
        }

        [Test]
        public void SubscribeOnALotOfSymbolsThrowArgumentExceptionExceedsAllowedLimit()
        {
            int symbolCounter = HardCodedSymbolsSNP.Length;
            Assert.Throws<ArgumentException>(() =>
            {
                var cancellationTokenSource = new CancellationTokenSource();

                foreach (var ticker in HardCodedSymbolsSNP)
                {
                    foreach (var config in GetSubscriptionDataConfigs(ticker, Resolution.Second))
                    {
                        iexDataProvider.Subscribe(config, (s, e) => { });
                    }
                }
                cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20));
            });
            Assert.Less(iexDataProvider.maxAllowedSymbolLimit, symbolCounter);
        }

        [Test]
        public void NotSubscribeOnUniverseSymbol()
        {
            var spy = Symbols.SPY;
            var universeSymbol = UserDefinedUniverse.CreateSymbol(spy.SecurityType, spy.ID.Market);

            foreach (var config in GetSubscriptionDataConfigs(universeSymbol, Resolution.Second))
            {
                Assert.IsNull(iexDataProvider.Subscribe(config, (s, e) => { }));
            }
        }

        [Test]
        public void NotSubscribeOnCanonicalSymbol()
        {
            var spy = Symbols.SPY_Option_Chain;

            foreach (var config in GetSubscriptionDataConfigs(spy, Resolution.Second))
            {
                Assert.IsNull(iexDataProvider.Subscribe(config, (s, e) => { }));
            }
        }

        [Test]
        public void SubscribeWithWrongApiKeyThrowException()
        {
            Config.Set("iex-cloud-api-key", "wrong-api-key");

            var isSubscribeThrowException = false;

            var iexDataProvider = new IEXDataProvider();

            foreach (var config in GetSubscriptionDataConfigs(Symbols.SPY, Resolution.Second))
            {
                ProcessFeed(
                    iexDataProvider.Subscribe(config, (s, e) => { }),
                    tick =>
                    {
                        if (tick != null)
                        {
                            Log.Debug($"{nameof(IEXDataProviderTests)}: tick: {tick}");
                        }
                    },
                    () =>
                    {
                        isSubscribeThrowException = true;
                        _cancellationTokenSource.Cancel();
                    });
            }

            _cancellationTokenSource.Token.WaitHandle.WaitOne(TimeSpan.FromSeconds(20));
            Assert.IsTrue(isSubscribeThrowException);
        }

        [Test]
        public void CanInitializeUsingJobPacket()
        {
            Config.Set("iex-cloud-api-key", "");

            var job = new LiveNodePacket
            {
                BrokerageData = new Dictionary<string, string>() {
                    { "iex-cloud-api-key", "InvalidApiKeyThatWontBeUsed" },
                    { "iex-price-plan", "Launch" }
                }
            };

            using var iexDataProvider = new IEXDataProvider();

            Assert.Zero(iexDataProvider.maxAllowedSymbolLimit);

            iexDataProvider.SetJob(job);

            Assert.Greater(iexDataProvider.maxAllowedSymbolLimit, 0);

            Config.Set("iex-cloud-api-key", _apiKey);
        }

        [Test]
        public void JobPacketWontOverrideCredentials()
        {
            Config.Set("iex-cloud-api-key", "wrong_key");
            Config.Set("iex-cloud-api-key", "Launch");

            var job = new LiveNodePacket
            {
                BrokerageData = new Dictionary<string, string>() {
                    { "iex-cloud-api-key", "InvalidApiKeyThatWontBeUsed" },
                    { "iex-price-plan", "Enterprise" }
                }
            };

            using var iexDataProvider = new IEXDataProvider();

            var maxSymbolLimitBeforeSetJob = iexDataProvider.maxAllowedSymbolLimit;

            // it has initialized already
            Assert.Greater(maxSymbolLimitBeforeSetJob, 0);

            iexDataProvider.SetJob(job);

            // we use Enterprise plan in job variable => we must have unlimited maxAllowedSymbolLimit, but our config keep Launch
            Assert.That(maxSymbolLimitBeforeSetJob, Is.EqualTo(iexDataProvider.maxAllowedSymbolLimit));

            Config.Set("iex-cloud-api-key", _apiKey);
            Config.Set("iex-cloud-api-key", _pricePlan);
        }

        private void ProcessFeed(IEnumerator<BaseData> enumerator, Action<BaseData> callback = null, Action throwExceptionCallback = null)
        {
            Task.Factory.StartNew(() =>
            {
                try
                {
                    while (enumerator.MoveNext())
                    {
                        BaseData tick = enumerator.Current;
                        callback?.Invoke(tick);

                        Thread.Sleep(1000);
                    }
                }
                catch (AssertionException)
                {
                    throw;
                }
                catch (Exception err)
                {
                    throw;
                }
            }).ContinueWith(task =>
            {
                if (throwExceptionCallback != null)
                {
                    throwExceptionCallback();
                }
                Log.Error("The throwExceptionCallback is null.");
            }, TaskContinuationOptions.OnlyOnFaulted);
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
                extendedHours: false,
                false);
        }
    }
}
