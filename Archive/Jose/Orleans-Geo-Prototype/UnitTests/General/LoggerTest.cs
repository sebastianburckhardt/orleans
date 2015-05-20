using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using Orleans.Counters;

namespace UnitTests
{
    /// <summary>
    ///This is a test class for LoggerTest and is intended
    ///to contain all LoggerTest Unit Tests
    ///</summary>
    [TestClass]
    public class LoggerTest
    {
        private static readonly Random rand = new Random();
        private double timingFactor;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

        #region Additional test attributes
        // 
        //You can use the following additional attributes as you write your tests:
        //
        //Use ClassInitialize to run code before running the first test in the class
        //[ClassInitialize()]
        //public static void MyClassInitialize(TestContext testContext)
        //{
        //}
        //
        //Use ClassCleanup to run code after all tests in a class have run
        //[ClassCleanup()]
        //public static void MyClassCleanup()
        //{
        //}
        //
        //Use TestInitialize to run code before running each test
        //[TestInitialize()]
        //public void MyTestInitialize()
        //{
        //}
        //
        //Use TestCleanup to run code after each test has run
        //[TestCleanup()]
        //public void MyTestCleanup()
        //{
        //}
        //
        #endregion

        [TestInitialize]
        public void SetUpDefaults()
        {
            Logger.UnInitialize();
            Logger.SetRuntimeLogLevel(OrleansLogger.Severity.Verbose);
            Logger.SetAppLogLevel(OrleansLogger.Severity.Info);
            var overrides = new [] {
                new Tuple<string, OrleansLogger.Severity>("Runtime.One", OrleansLogger.Severity.Warning),
                new Tuple<string, OrleansLogger.Severity>("Grain.Two", OrleansLogger.Severity.Verbose3)
            };
            Logger.SetTraceLevelOverrides(overrides.ToList());
            timingFactor = UnitTestBase.CalibrateTimings();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            Logger.Flush();
            Logger.UnInitialize();
        }

        /// <summary>
        ///A test for Logger Constructor
        ///</summary>
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_ConstructorTest()
        {
            Logger target = Logger.GetLogger("Test", Logger.LoggerType.Runtime);
            Assert.AreEqual(OrleansLogger.Severity.Verbose, target.SeverityLevel,
                "Default severity level not calculated correctly for runtime logger with no overrides");

            target = Logger.GetLogger("One", Logger.LoggerType.Runtime);
            Assert.AreEqual(OrleansLogger.Severity.Warning, target.SeverityLevel,
                "Default severity level not calculated correctly for runtime logger with an override");

            target = Logger.GetLogger("Two", Logger.LoggerType.Grain);
            Assert.AreEqual(OrleansLogger.Severity.Verbose3, target.SeverityLevel,
                "Default severity level not calculated correctly for runtime logger with an override");
        }

        /// <summary>
        ///A test for Logger Constructor
        ///</summary>
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_ConstructorTest1()
        {
            Logger target = Logger.GetLogger("Test", Logger.LoggerType.Application);
            Assert.AreEqual(OrleansLogger.Severity.Info, target.SeverityLevel,
                "Default severity level not calculated correctly for application logger with no override");

            target = Logger.GetLogger("Two", Logger.LoggerType.Grain);
            Assert.AreEqual(OrleansLogger.Severity.Verbose3, target.SeverityLevel,
                "Default severity level not calculated correctly for grain logger with an override");
        }

        /// <summary>
        ///A test for SetRuntimeLogLevel
        ///</summary>
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_SetRuntimeLogLevelTest()
        {
            Logger target = Logger.GetLogger("Test");
            Assert.AreEqual(OrleansLogger.Severity.Verbose, target.SeverityLevel,
                "Default severity level not calculated correctly for runtime logger with no overrides");
            Logger.SetRuntimeLogLevel(OrleansLogger.Severity.Warning);
            Assert.AreEqual(OrleansLogger.Severity.Warning, target.SeverityLevel,
                "Default severity level not re-calculated correctly for runtime logger with no overrides");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_SeverityLevelTest()
        {
            Logger target = Logger.GetLogger("Test");
            Assert.AreEqual(OrleansLogger.Severity.Verbose, target.SeverityLevel,
                "Default severity level not calculated correctly for runtime logger with no overrides");
            target.SetSeverityLevel(OrleansLogger.Severity.Verbose2);
            Assert.AreEqual(OrleansLogger.Severity.Verbose2, target.SeverityLevel,
                "Custom severity level was not set properly");
            Logger.SetRuntimeLogLevel(OrleansLogger.Severity.Warning);
            Assert.AreEqual(OrleansLogger.Severity.Verbose2, target.SeverityLevel,
                "Severity level was re-calculated even though a custom value had been set");
        }


        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_GetLoggerTest()
        {
            Logger logger = Logger.GetLogger("GetLoggerTest", Logger.LoggerType.Runtime);
            Assert.IsNotNull(logger);

            Logger logger2 = Logger.GetLogger("GetLoggerTest", Logger.LoggerType.Runtime);
            Assert.IsNotNull(logger2);
            Assert.IsTrue(ReferenceEquals(logger, logger2));

            Logger logger3 = Logger.GetLogger("GetLoggerTest", Logger.LoggerType.Application);
            Assert.IsNotNull(logger3);
            Assert.IsTrue(ReferenceEquals(logger, logger3));
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_CreateMiniDump()
        {
            var dumpFile = Logger.CreateMiniDump();
            Console.WriteLine("Logger.CreateMiniDump dump file = " + dumpFile);
            Assert.IsNotNull(dumpFile);
            Assert.IsTrue(dumpFile.Exists, "Mini-dump file exists");
            Assert.IsTrue(dumpFile.Length > 0, "Mini-dump file has content");
            Console.WriteLine("Logger.CreateMiniDump dump file location = " + dumpFile.FullName);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_AddRemoveOverride()
        {
            const string name = "LoggerOverrideTest";
            const string fullName = "Runtime." + name;
            var logger = Logger.GetLogger(name, Logger.LoggerType.Runtime);
            var initialLevel = logger.SeverityLevel;

            Logger.AddTraceLevelOverride(fullName, OrleansLogger.Severity.Warning);
            Assert.AreEqual(OrleansLogger.Severity.Warning, logger.SeverityLevel, "Logger level not reset after override added");

            Logger.RemoveTraceLevelOverride(fullName);
            Assert.AreEqual(initialLevel, logger.SeverityLevel, "Logger level not reset after override removed");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_BulkMessageLimit_DifferentLoggers()
        {
            int n = 10000;
            Logger.BulkMessageInterval = TimeSpan.FromMilliseconds(50);

            int logCode1 = 1;
            int logCode2 = 2;
            int expectedLogMessages1 = Logger.BulkMessageLimit + 2 + 1;
            int expectedLogMessages2 = Logger.BulkMessageLimit + 2;
            int expectedBulkLogMessages1 = 0;
            int expectedBulkLogMessages2 = 1;

            TestLogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger logger1 = Logger.GetLogger("logger1");
            Logger logger2 = Logger.GetLogger("logger2");

            // Write log messages to logger #1
            for (int i = 0; i < Logger.BulkMessageLimit; i++)
            {
                logger1.Warn(logCode1, "Message " + (i + 1) + " to logger1");
            }

            // Write to logger#2 using same logCode -- This should NOT trigger the bulk message for logCode1
            logger2.Warn(logCode1, "Use same logCode to logger2");

            // Wait until the BulkMessageInterval time interval expires before writing the final log message - should cause any pending message flush);
            Thread.Sleep(Logger.BulkMessageInterval);
            Thread.Sleep(1);

            // Write log messages to logger #2
            for (int i = 0; i < n; i++)
            {
                logger2.Warn(logCode2, "Message " + (i+1) + " to logger2" );
            }

            // Wait until the BulkMessageInterval time interval expires before writing the final log message - should cause any pending message flush);
            Thread.Sleep(Logger.BulkMessageInterval);
            Thread.Sleep(1);

            logger1.Info(logCode1, "Penultimate message to logger1");
            logger2.Info(logCode2, "Penultimate message to logger2");

            logger1.Info(logCode1, "Final message to logger1");
            logger2.Info(logCode2, "Final message to logger2");

            Assert.AreEqual(expectedBulkLogMessages1, logConsumer.GetEntryCount(logCode1 + Logger.BulkMessageSummaryOffset),
                    "Should see {0} bulk message entries via logger#1", expectedBulkLogMessages1);
            Assert.AreEqual(expectedBulkLogMessages2, logConsumer.GetEntryCount(logCode2 + Logger.BulkMessageSummaryOffset),
                    "Should see {0} bulk message entries via logger#2", expectedBulkLogMessages2);
            Assert.AreEqual(expectedLogMessages1, logConsumer.GetEntryCount(logCode1),
                    "Should see {0} real entries in total via logger#1", expectedLogMessages1);
            Assert.AreEqual(expectedLogMessages2, logConsumer.GetEntryCount(logCode2),
                    "Should see {0} real entries in total via logger#2", expectedLogMessages2);
            Assert.AreEqual(0, logConsumer.GetEntryCount(logCode1 - 1), "Should not see any other entries -1");
            Assert.AreEqual(0, logConsumer.GetEntryCount(logCode2 + 1), "Should not see any other entries +1");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_BulkMessageLimit_VerboseNotFiltered()
        {
            const string testName = "Logger_BulkMessageLimit_InfoNotFiltered";
            int n = 10000;
            Logger.BulkMessageInterval = TimeSpan.FromMilliseconds(5);

            int logCode1 = 1;
            int expectedLogMessages1 = n + 2;
            int expectedBulkLogMessages1 = 0;

            TestLogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger logger1 = Logger.GetLogger(testName);

            // Write log messages to logger #1
            for (int i = 0; i < n; i++)
            {
                logger1.Verbose(logCode1, "Verbose message " + (i + 1) + " to logger1");
            }

            // Wait until the BulkMessageInterval time interval expires before writing the final log message - should cause any pending message flush);
            Thread.Sleep(Logger.BulkMessageInterval);
            Thread.Sleep(1);

            logger1.Info(logCode1, "Penultimate message to logger1");
            logger1.Info(logCode1, "Final message to logger1");

            Assert.AreEqual(expectedBulkLogMessages1, logConsumer.GetEntryCount(logCode1 + Logger.BulkMessageSummaryOffset),
                    "Should see {0} bulk message entries via logger#1", expectedBulkLogMessages1);
            Assert.AreEqual(expectedLogMessages1, logConsumer.GetEntryCount(logCode1),
                    "Should see {0} real entries in total via logger#1", expectedLogMessages1);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_BulkMessageLimit_DifferentFinalLogCode()
        {
            const string testName = "Logger_BulkMessageLimit_DifferentFinalLogCode";
            int n = 10000;
            Logger.BulkMessageInterval = TimeSpan.FromMilliseconds(50);

            int mainLogCode = rand.Next(100000);
            int finalLogCode = mainLogCode + 10;
            int expectedMainLogMessages = Logger.BulkMessageLimit;
            int expectedFinalLogMessages = 1;
            int expectedBulkLogMessages = 1;

            RunTestForLogFiltering(testName, n, mainLogCode, finalLogCode, expectedMainLogMessages, expectedFinalLogMessages, expectedBulkLogMessages);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_BulkMessageLimit_SameFinalLogCode()
        {
            const string testName = "Logger_BulkMessageLimit_SameFinalLogCode";
            int n = 10000;
            Logger.BulkMessageInterval = TimeSpan.FromMilliseconds(50);

            int mainLogCode = rand.Next(100000);
            int finalLogCode = mainLogCode; // Same as main code
            int expectedMainLogMessages = Logger.BulkMessageLimit + 1; // == Loop + 1 final
            int expectedFinalLogMessages = expectedMainLogMessages;
            int expectedBulkLogMessages = 1;

            RunTestForLogFiltering(testName, n, mainLogCode, finalLogCode, expectedMainLogMessages,
                                   expectedFinalLogMessages, expectedBulkLogMessages);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_BulkMessageLimit_Excludes_100000()
        {
            const string testName = "Logger_BulkMessageLimit_Excludes_100000";
            int n = 1000;
            Logger.BulkMessageInterval = TimeSpan.FromMilliseconds(1);

            int mainLogCode = 100000;
            int finalLogCode = mainLogCode + 1;
            int expectedMainLogMessages = n;
            int expectedFinalLogMessages = 1;
            int expectedBulkLogMessages = 0;
            
            RunTestForLogFiltering(testName, n, finalLogCode, mainLogCode, expectedMainLogMessages, expectedFinalLogMessages, expectedBulkLogMessages);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_MessageSizeLimit()
        {
            const string testName = "Logger_MessageSizeLimit";
            TestLogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger logger1 = Logger.GetLogger(testName);

            StringBuilder sb = new StringBuilder();
            while (sb.Length <= Logger.MAX_LOG_MESSAGE_SIZE)
            {
                sb.Append("1234567890");
            }
            string longString = sb.ToString();
            Assert.IsTrue(longString.Length > Logger.MAX_LOG_MESSAGE_SIZE);

            int logCode1 = 1;

            logger1.Info(logCode1, longString);

            Assert.AreEqual(1, logConsumer.GetEntryCount(logCode1), "Should see original log message entry");
            Assert.AreEqual(1, logConsumer.GetEntryCount((int)ErrorCode.Logger_LogMessageTruncated), "Should also see 'Message truncated' message");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Logger_Stats_MessageSizeLimit()
        {
            const string testName = "Logger_Stats_MessageSizeLimit";
            TestLogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger logger1 = Logger.GetLogger(testName);

            const string StatsCounterBaseName = "LoggerTest.Stats.Size";

            for (int i = 1; i <= 1000; i++)
            {
                StatName counterName = new StatName(StatsCounterBaseName + "." + i);
                CounterStatistic ctr = CounterStatistic.FindOrCreate(counterName);
                ctr.IncrementBy(i);
            }

            LogStatistics statsLogger = new LogStatistics(TimeSpan.Zero, true);
            statsLogger.DumpCounters();

            int count = logConsumer.GetEntryCount((int)ErrorCode.PerfCounterDumpAll);
            Console.WriteLine(count + " stats log message entries written");
            Assert.IsTrue(count > 1, "Should be some stats log message entries - saw " + count);
            Assert.AreEqual(0, logConsumer.GetEntryCount((int)ErrorCode.Logger_LogMessageTruncated), "Should not see any 'Message truncated' message");
        }

        [TestMethod]
        public void Logger_Perf_FileLogWriter()
        {
            const string testName = "Logger_Perf_FileLogWriter";
            TimeSpan target = TimeSpan.FromMilliseconds(1000);
            int n = 10000;
            int logCode = rand.Next(100000);

            var logFile = new FileInfo("log-" + testName + ".txt");
            ILogConsumer log = new LogWriterToFile(logFile);
            RunLogWriterPerfTest(testName, n, logCode, target, log);
        }

        [TestMethod]
        public void Logger_Perf_TraceLogWriter()
        {
            const string testName = "Logger_Perf_TraceLogWriter";
            TimeSpan target = TimeSpan.FromMilliseconds(360);
            int n = 10000;
            int logCode = rand.Next(100000);

            ILogConsumer log = new LogWriterToTrace();
            RunLogWriterPerfTest(testName, n, logCode, target, log);
        }

        [TestMethod]
        public void Logger_Perf_ConsoleLogWriter()
        {
            const string testName = "Logger_Perf_ConsoleLogWriter";
            TimeSpan target = TimeSpan.FromMilliseconds(100);
            int n = 10000;
            int logCode = rand.Next(100000);

            ILogConsumer log = new LogWriterToConsole();
            RunLogWriterPerfTest(testName, n, logCode, target, log);
        }

        //[TestMethod]
        //public void Logger_Perf_EtwLogWriter()
        //{
        //    const string testName = "Logger_Perf_EtwLogWriter";
        //    TimeSpan target = TimeSpan.FromSeconds(0.7);
        //    int n = 100000;
        //
        //    ILogConsumer log = new LogWriterToEtw();
        //    RunLogWriterPerfTest(testName, n, target, log);
        //}

        [TestMethod]
        public void Logger_Perf_Logger_Console()
        {
            const string testName = "Logger_Perf_Logger_Console";
            TimeSpan target = TimeSpan.FromMilliseconds(50);
            int n = 10000;
            int logCode = rand.Next(100000);

            ILogConsumer logConsumer = new LogWriterToConsole();
            Logger.LogConsumers.Add(logConsumer);
            Logger.BulkMessageInterval = target;
            Logger logger = Logger.GetLogger(testName);

            RunLoggerPerfTest(testName, n, logCode, target, logger);
        }

        [TestMethod]
        public void Logger_Perf_Logger_Dummy()
        {
            const string testName = "Logger_Perf_Logger_Dummy";
            TimeSpan target = TimeSpan.FromMilliseconds(30);
            int n = 10000;
            int logCode = rand.Next(100000);

            ILogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger.BulkMessageInterval = target;
            Logger logger = Logger.GetLogger(testName);

            RunLoggerPerfTest(testName, n, logCode, target, logger);
        }

        private void RunLogWriterPerfTest(string testName, int n, int logCode, TimeSpan target, ILogConsumer log)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            for (int i = 0; i < n; i++)
            {
                log.Log(OrleansLogger.Severity.Info, Logger.LoggerType.Runtime, testName, "msg " + i, null, null, logCode);
            }
            stopwatch.Stop();
            var elapsed = stopwatch.Elapsed;
            Console.WriteLine(testName + " : Elapsed time = " + elapsed);
            Assert.IsTrue(elapsed < target.Multiply(timingFactor), "{0}: Elapsed time {1} exceeds target time {2}", testName, elapsed, target);
        }

        private void RunLoggerPerfTest(string testName, int n, int logCode, TimeSpan target, Logger logger)
        {
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            for (int i = 0; i < n; i++)
            {
                logger.Warn(logCode, "msg " + i);
            }
            var elapsed = stopwatch.Elapsed;
            string msg = testName + " : Elapsed time = " + elapsed;

            // Wait until the BulkMessageInterval time interval expires before wring the final log message - should cause bulk message flush
            while (stopwatch.Elapsed <= Logger.BulkMessageInterval)
            {
                Thread.Sleep(10);
            }

            Console.WriteLine(msg);
            logger.Info(logCode, msg);
            Assert.IsTrue(elapsed < target.Multiply(timingFactor), "{0}: Elapsed time {1} exceeds target time {2}", testName, elapsed, target);
        }

        private static void RunTestForLogFiltering(string testName, int n, int finalLogCode, int mainLogCode, int expectedMainLogMessages, int expectedFinalLogMessages, int expectedBulkLogMessages)
        {
            Assert.IsTrue(finalLogCode != (mainLogCode - 1), "Test code constraint -1");

            TestLogConsumer logConsumer = new TestLogConsumer();
            Logger.LogConsumers.Add(logConsumer);
            Logger logger = Logger.GetLogger(testName);

            var stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < n; i++)
            {
                logger.Warn(mainLogCode, "msg " + i);
            }

            // Wait until the BulkMessageInterval time interval expires before wring the final log message - should cause bulk message flush);
            TimeSpan delay = Logger.BulkMessageInterval - stopwatch.Elapsed;
            if (delay > TimeSpan.Zero)
            {
                Console.WriteLine("Sleeping for " + delay);
                Thread.Sleep(delay);
            }
            Thread.Sleep(10);

            logger.Info(finalLogCode, "Final msg");

            Assert.AreEqual(expectedMainLogMessages, logConsumer.GetEntryCount(mainLogCode),
                    "Should see {0} real entries in total", expectedMainLogMessages);
            if (mainLogCode != finalLogCode)
            {
                Assert.AreEqual(expectedFinalLogMessages, logConsumer.GetEntryCount(finalLogCode),
                    "Should see {0} final entry", expectedFinalLogMessages);
            }
            Assert.AreEqual(expectedBulkLogMessages, logConsumer.GetEntryCount(mainLogCode + Logger.BulkMessageSummaryOffset),
                    "Should see {0} bulk message entries", expectedBulkLogMessages);
            Assert.AreEqual(0, logConsumer.GetEntryCount(mainLogCode - 1), "Should not see any other entries -1");
        }
    }

    class TestLogConsumer : ILogConsumer
    {
        private readonly Dictionary<int,int> entryCounts = new Dictionary<int, int>();
        private int lastLogCode;

        public void Log(OrleansLogger.Severity severity, Logger.LoggerType loggerType, string caller, string message, System.Net.IPEndPoint myIPEndPoint, Exception exception, int eventCode = 0)
        {
            lock (this)
            {
                if (entryCounts.ContainsKey(eventCode)) entryCounts[eventCode]++;
                else entryCounts.Add(eventCode, 1);

                if (eventCode != lastLogCode)
                {
                    Console.WriteLine("{0} {1} - {2}", severity, eventCode, message);
                    lastLogCode = eventCode;
                }
            }
        }

        internal int GetEntryCount(int eventCode)
        {
            lock (this)
            {
                int count = (entryCounts.ContainsKey(eventCode)) ? entryCounts[eventCode] : 0;
                Console.WriteLine("Count {0} = {1}", eventCode, count);
                return count;
            }
        }
    }
}
