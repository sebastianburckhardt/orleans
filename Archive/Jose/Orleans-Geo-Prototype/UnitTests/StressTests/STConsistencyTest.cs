using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using UnitTestGrainInterfaces;
using Orleans;


namespace UnitTests.StressTests
{
    /// <summary>
    /// Summary description for PersistenceTest
    /// </summary>
    [TestClass]
    public class STConsistencyTest : UnitTestBase
    {
        const int timeout = 10000;
        //static int basePort = 22220;

        public STConsistencyTest() : base(true)
        {
            Console.WriteLine("#### STConsistencyTest() is called.");
            //
            // TODO: Add constructor logic here
            //
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestCleanup()]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
            ResetDefaultRuntimes();
        }


        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void STConsistencyTest_InterleavingConsistencyTest()
        {
            IReentrantStressSelfManagedGrain grain = ReentrantStressSelfManagedGrainFactory.GetGrain(0);

            for (int i = 0; i < 50; i++)
            {
                AsyncCompletion promise1 = AsyncCompletion.FromTask(grain.InterleavingConsistencyTest(10000));
                promise1.Wait();
                Console.WriteLine("\n\n\n### Iteration " + i + " is done.\n\n\n");
            }
        }


        //[TestMethod]
        public void STConsistencyTestMigrationPersistent()
        {
            STConsistencyTestMigration(true);
        }

        //[TestMethod]
        public void STConsistencyTestMigrationNonPersistent()
        {
            STConsistencyTestMigration(false);
        }

        private void STConsistencyTestMigration(bool persistent)
        {
            //Logger logger = new Logger("STConsistencyTestMigration");
            //ResultHandle result = new ResultHandle();
            //const int nClients = 5;
            //const int nIncsPerClient = 3;

            //List<Silo> instances = StartAdditionalOrleansRuntimes(nClients);
            //IOrleansManagementGrain mgmtGrain = Orleans.SystemManagement;
            //AsyncCompletion promise = mgmtGrain.SuspendHosts(GetRuntimesIds(instances));
            //promise.Wait();

            //SimpleGrainSmartProxy grain = new SimpleGrainSmartProxy(persistent/*, "ConsistencyTestMigration-" + persistent + "-PER"*/);

            //AsyncCompletion[] clients = new AsyncCompletion[nClients];
            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i] = AsyncCompletion.StartNew((object obj) =>
            //    {
            //        for (int j = 0; j < nIncsPerClient; j++)
            //        {
            //            int waitTime = 100;
            //            AsyncCompletion p = grain.LongMethod(waitTime);
            //            p.Wait();
            //            logger.Info("Client " + obj + " done with call " + j);
            //        }
            //    }, i);
            //    mgmtGrain.ResumeHost(instances[i].SiloAddress).Wait();
            //    Thread.Sleep(100);
            //    logger.Info("Client " + i + " done with ResumeHost " + instances[i].SiloAddress);
            //}
            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i].Wait();
            //    logger.Info("Client " + i + " done with all calls.");
            //}
            //logger.Info("All Clients are done.");

            //result.Done = true;
            //Assert.IsTrue(result.WaitForFinished(timeout));
            //logger.Info("\n\nTEST STConsistencyTestMigration(" + persistent + ") ENDED SUCCESSFULLY\n\n");
            //Console.WriteLine("\n\nTEST ENDED SUCCESSFULLY\n\n");
        }

        public void STConsistencyTestPersistent()
        {
            STConsistencyTestPersistence(true);
        }
        public void STConsistencyTestNonPersistent()
        {
            STConsistencyTestPersistence(false);
        }
        public void STConsistencyTestPersistence(bool persistent)
        {
            //ResultHandle result = new ResultHandle();
            
            //SimpleGrainSmartProxy grain = new SimpleGrainSmartProxy(persistent/*, "ConsistencyTest-" + persistent + "-PER"*/);
            //grain.SetA(0).Wait();

            //const int nClients = 5;
            //const int nIncsPerClient = 3;
            //List<Silo> instances = StartAdditionalOrleansRuntimes(nClients);

            //IOrleansManagementGrain mgmtGrain = Orleans.SystemManagement;
            //mgmtGrain.SuspendHosts(GetRuntimesIds(instances)).Wait();
            ////SuspendAllOrleansRuntimes(instances);  

            //AsyncCompletion[] clients = new AsyncCompletion[nClients];
            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i] = AsyncCompletion.StartNew(() =>
            //    {
            //        for (int j = 0; j < nIncsPerClient; j++)
            //        {
            //            AsyncCompletion p = grain.IncrementA();
            //            p.Wait(timeout);
            //        }
            //    });
            //}
            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i].Wait(timeout);
            //}

            //mgmtGrain.SuspendHost(Orleans.SiloAddress).Wait(timeout);
            //mgmtGrain.ResumeHost(Orleans.SiloAddress).Wait(timeout);
            ////Orleans.Suspend(false);
            ////Orleans.Resume();

            //int all = grain.GetA().Result;
            //if (persistent)
            //{
            //    Assert.AreEqual(nClients * nIncsPerClient, all);
            //}
            //else
            //{
            //    Assert.IsTrue(all == 0, all.ToString());
            //}
            ////--------------------

            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i] = AsyncCompletion.StartNew(() =>
            //    {
            //        for (int j = 0; j < nIncsPerClient; j++)
            //        {
            //            AsyncCompletion p = grain.IncrementA(); ;
            //            p.Wait(timeout);
            //        }
            //    });
            //    mgmtGrain.ResumeHost(instances[i].SiloAddress).Wait(timeout);
            //    //instances[i].Resume();
            //}
            //for (int i = 0; i < clients.Length; i++)
            //{
            //    clients[i].Wait(timeout);
            //}
            //mgmtGrain.SuspendHost(Orleans.SiloAddress).Wait(timeout);
            ////Orleans.Suspend(false);
            //all = grain.GetA().Result;
            //if (persistent)
            //{
            //    Assert.AreEqual(2*nClients * nIncsPerClient, all);
            //}
            //else
            //{
            //    Assert.IsTrue(all <= 2*nClients * nIncsPerClient);
            //}
            //mgmtGrain.ResumeHost(Orleans.SiloAddress).Wait(timeout);
            ////Orleans.Resume();
            //result.Done = true;
            //Assert.IsTrue(result.WaitForFinished(timeout));
            //Console.WriteLine("\n\nTEST ENDED SUCCESSFULLY\n\n");
        }
    }
}
