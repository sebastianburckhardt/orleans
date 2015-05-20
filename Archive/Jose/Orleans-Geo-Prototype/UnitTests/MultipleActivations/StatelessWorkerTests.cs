using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;
using Orleans;
using UnitTestGrains;
using System.Threading;
using System.Diagnostics;
using System;

#pragma warning disable 618

namespace UnitTests
{
    [TestClass]
    public class StatelessWorkerTests : UnitTestBase
    {
        const int timeout = 10000;
        const int maxLocalActivations = 10;
        //ResultHandle result;

        public StatelessWorkerTests()
            : base(new Options { StartSecondary = false })
        {
            //
            // TODO: Add constructor logic here
            //
        }

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }

        //
        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod]
        public void StatelessWorker()
        {
            IStatelessWorkerGrain grain = StatelessWorkerGrainFactory.GetGrain(0);
            List<Task> promises = new List<Task>();

            for (int i = 0; i < maxLocalActivations; i++)
                promises.Add(grain.LongCall()); //trigger creation of 10 local activations (default MalLocal=10)
            Task.WhenAll(promises).Wait();

            Thread.Sleep(2000); // for just in case

            promises.Clear();
            var stopwatch = Stopwatch.StartNew();
            
            for (int i = 0; i < 100; i++)
                promises.Add(grain.LongCall()); //send 50 requests to 10 activations
            Task.WhenAll(promises).Wait();

            stopwatch.Stop();

            Assert.IsTrue(stopwatch.Elapsed > TimeSpan.FromSeconds(19.5), "50 requests with a 2 second processing time shouldn't take less than 20 seconds on 10 activations. But it took " + stopwatch.Elapsed);

            promises.Clear();
            for (int i = 0; i < 20; i++)
                promises.Add(grain.GetCallStats()); //trigger creation of 10 local activations (default MalLocal=10)
            Task.WhenAll(promises).Wait();

            HashSet<Guid> guids = new HashSet<Guid>();
            foreach (var promise in promises)
            {
                Tuple<Guid, List<Tuple<DateTime, DateTime>>> response =
                    ((Task<Tuple<Guid, List<Tuple<DateTime, DateTime>>>>) promise).Result;

                if (guids.Contains(response.Item1))
                    continue; // duplicate response from the same activation

                guids.Add(response.Item1);

                logger.Info(" {0}: Activation {1}", guids.Count, response.Item1);
                int count = 1;
                foreach(Tuple<DateTime,DateTime> call in response.Item2)
                    logger.Info("\t{0}: {1} - {2}", count++, call.Item1, call.Item2);
            }
        }
    }
}

#pragma warning restore 618
