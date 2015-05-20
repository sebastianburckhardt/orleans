using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrains;

namespace UnitTests
{
    /// <summary>
    /// Summary description for PersistenceTest
    /// </summary>
    [TestClass]
    public class ConcurrencyTests : UnitTestBase
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        public ConcurrencyTests()
            : base(new Options { StartSecondary = false, MaxActiveThreads = 2 })
        {
            Console.WriteLine("#### ConcurrencyTests() is called.");
        }

        [TestCleanup()]
        public void Cleanup()
        {
            ResetAllAdditionalRuntimes();
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        //[TestMethod(), TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        [TestMethod, TestCategory("Failures"), TestCategory("ReadOnly")]
        public void ConcurrencyTest_ReadOnly()
        {
            ResultHandle result = new ResultHandle();
            IConcurrentGrain first = ConcurrentGrainFactory.GetGrain(GetRandomGrainId());
            first.Initialize(0).Wait();

            List<AsyncCompletion> promises = new List<AsyncCompletion>();
            for (int i = 0; i < 5; i++)
            {
                AsyncCompletion p = AsyncCompletion.FromTask(first.A());
                promises.Add(p);
            }
            AsyncCompletion.JoinAll(promises).ContinueWith(() => { result.Done = true; }).Ignore();
            Assert.IsTrue(result.WaitForFinished(timeout));
            Console.WriteLine("\n\nENDED TEST\n\n");
        }

        [TestMethod()]
        public void ConcurrencyTest_ModifyReturnList()
        {
            IConcurrentGrain grain = ConcurrentGrainFactory.GetGrain(GetRandomGrainId());
            
            Console.WriteLine("\n\nStarting TEST\n\n");

            AsyncValue<List<int>>[] ll = new AsyncValue<List<int>>[20];
            for (int i = 0; i < 2000; i++)
            {
                for (int j = 0; j < ll.Length; j++)
                    ll[j] = AsyncValue.FromTask(grain.ModifyReturnList_Test());

                AsyncValue<List<int>>.JoinAll(ll).Wait();
                Console.Write(".");
            }
            Console.WriteLine("\n\nENDED TEST\n\n");
        }

        [TestMethod]
        public void ConcurrencyTest_TailCall_1()
        {
            ResultHandle result = new ResultHandle();
            IConcurrentGrain grain1 = ConcurrentGrainFactory.GetGrain(GetRandomGrainId());
            IConcurrentReentrantGrain grain2 = ConcurrentReentrantGrainFactory.GetGrain(GetRandomGrainId());
            grain1.Initialize_2(1).Wait();
            grain2.Initialize_2(2).Wait();

            Console.WriteLine("\n\nStarting TEST\n\n");

            AsyncValue<int> retVal1 = AsyncValue.FromTask(grain1.TailCall_Caller(grain2, false));
            AsyncValue<int> retVal2 = AsyncValue.FromTask(grain1.TailCall_Resolver(grain2));

            AsyncCompletion.Join(retVal1, retVal2).ContinueWith(() => { result.Done = true; }).Ignore();
            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.AreEqual(7, retVal1.GetValue());
            Assert.AreEqual(8, retVal2.GetValue());
            Console.Write(".");
            
            Console.WriteLine("\n\nENDED TEST\n\n");
        }
    }
}
