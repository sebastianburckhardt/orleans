using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;


namespace UnitTests.General
{
    [TestClass]
    public class DeadlockDetectionTests : UnitTestBase
    {
        private const int numIterations = 30;

        public DeadlockDetectionTests()
            : base( new Options { PerformDeadlockDetection = true } )
        {
        }

        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize()]
        public static void MyClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }

        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        // 2 silos, loop across all cases (to force all grains to be local and remote):
        //      Non Reentrant A, B
        //      Reentrant C
        // 1) Deadlock A, A
        // 2) Deadlock A, B, A
        // 3) Deadlock C, A, C, A
        // 4) No Deadlock C, C
        // 5) No Deadlock C, A, C

        // 1) Deadlock A, A
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void DeadlockDetection_1()
        {
            for (int i = 0; i < numIterations; i++)
            {
                long grainId = i;
                IDeadlockNonReentrantGrain firstGrain = DeadlockNonReentrantGrainFactory.GetGrain(grainId);
                List<Tuple<long, bool>> callChain = new List<Tuple<long, bool>>();
                callChain.Add(new Tuple<long, bool>(grainId, true));
                callChain.Add(new Tuple<long, bool>(grainId, true));

                try
                {
                    firstGrain.CallNext_1(callChain, 1).Wait();
                }
                catch (Exception exc)
                {
                    Exception baseExc = exc.GetBaseException();
                    logger.Info(baseExc.Message);
                    Assert.AreEqual(typeof(OrleansDeadlockException), baseExc.GetType());
                    OrleansDeadlockException deadlockExc = (OrleansDeadlockException)baseExc;
                    Assert.AreEqual(callChain.Count, deadlockExc.CallChain.Count());
                }
            }
        }

        // 2) Deadlock A, B, A
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void DeadlockDetection_2()
        {
            long bBase = 100;
            for (int i = 0; i < numIterations; i++)
            {
                long grainId = i;
                IDeadlockNonReentrantGrain firstGrain = DeadlockNonReentrantGrainFactory.GetGrain(grainId);
                List<Tuple<long, bool>> callChain = new List<Tuple<long, bool>>();
                callChain.Add(new Tuple<long, bool>(grainId, true));
                callChain.Add(new Tuple<long, bool>(bBase + grainId, true));
                callChain.Add(new Tuple<long, bool>(grainId, true));

                try
                {
                    firstGrain.CallNext_1(callChain, 1).Wait();
                }
                catch (Exception exc)
                {
                    Exception baseExc = exc.GetBaseException();
                    logger.Info(baseExc.Message);
                    Assert.AreEqual(typeof(OrleansDeadlockException), baseExc.GetType());
                    OrleansDeadlockException deadlockExc = (OrleansDeadlockException)baseExc;
                    Assert.AreEqual(callChain.Count, deadlockExc.CallChain.Count());
                }
            }
        }

        // 3) Deadlock C, A, C, A
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void DeadlockDetection_3()
        {
            long cBase = 200;
            for (int i = 0; i < numIterations; i++)
            {
                long grainId = i;
                IDeadlockReentrantGrain firstGrain = DeadlockReentrantGrainFactory.GetGrain(grainId);
                List<Tuple<long, bool>> callChain = new List<Tuple<long, bool>>();
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));
                callChain.Add(new Tuple<long, bool>(grainId, true));
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));
                callChain.Add(new Tuple<long, bool>(grainId, true));

                try
                {
                    firstGrain.CallNext_1(callChain, 1).Wait();
                }
                catch (Exception exc)
                {
                    Exception baseExc = exc.GetBaseException();
                    logger.Info(baseExc.Message);
                    Assert.AreEqual(typeof(OrleansDeadlockException), baseExc.GetType());
                    OrleansDeadlockException deadlockExc = (OrleansDeadlockException)baseExc;
                    Assert.AreEqual(callChain.Count, deadlockExc.CallChain.Count());
                }
            }
        }

        // 4) No Deadlock C, C
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void DeadlockDetection_4()
        {
            long cBase = 200;
            for (int i = 0; i < numIterations; i++)
            {
                long grainId = i;
                IDeadlockReentrantGrain firstGrain = DeadlockReentrantGrainFactory.GetGrain(grainId);
                List<Tuple<long, bool>> callChain = new List<Tuple<long, bool>>();
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));

                firstGrain.CallNext_1(callChain, 1).Wait();
            }
        }

        // 5) No Deadlock C, A, C
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void DeadlockDetection_5()
        {
            long cBase = 200;
            for (int i = 0; i < numIterations; i++)
            {
                long grainId = i;
                IDeadlockReentrantGrain firstGrain = DeadlockReentrantGrainFactory.GetGrain(grainId);
                List<Tuple<long, bool>> callChain = new List<Tuple<long, bool>>();
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));
                callChain.Add(new Tuple<long, bool>(grainId, true));
                callChain.Add(new Tuple<long, bool>(cBase + grainId, false));

                firstGrain.CallNext_1(callChain, 1).Wait();
            }
        }
    }
}

