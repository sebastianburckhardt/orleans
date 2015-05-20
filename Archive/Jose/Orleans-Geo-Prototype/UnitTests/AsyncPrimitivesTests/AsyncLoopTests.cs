using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using System.Threading;

namespace UnitTests
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class AsyncLoopTests
    {
        private const int numIterations = 10;
        public AsyncLoopTests(){}

        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            OrleansTask.Reset();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AsyncLoopTest_While_Simple()
        {
            int loopCounter = 0;
            int numInside = 0;
            Func<int, bool> predicate = (int iteration) => { return iteration < numIterations; };
            Func<int, AsyncCompletion> function = (int iteration) =>
            {
                return AsyncCompletion.StartNew(() =>
                    {
                        numInside++;
                        Assert.AreEqual(1, numInside);
                        Console.WriteLine("Iteration " + iteration);
                        Assert.AreEqual(iteration, loopCounter);
                        loopCounter++;
                        //Thread.Sleep(10);
                        numInside--;
                    });
            };
            AsyncCompletion loopTask = AsyncLoop.While(predicate, function);
            loopTask.Wait();
            Assert.AreEqual(numIterations, loopCounter);
            Console.WriteLine("\n\n--------------------------------------------\n");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AsyncLoopTest_While_Exception()
        {
            int numIterationsTillException = 1;
            AsyncLoopTest_While_Exception(true, numIterationsTillException);
            AsyncLoopTest_While_Exception(false, numIterationsTillException);

            numIterationsTillException = 5;
            AsyncLoopTest_While_Exception(true, numIterationsTillException);
            AsyncLoopTest_While_Exception(false, numIterationsTillException);
        }

        public void AsyncLoopTest_While_Exception(bool throwInPredicate, int numIterationsTillException)
        {
            int loopCounter = 0;
            int numInside = 0;
            Func<int, bool> predicateClean = (int iteration) => { return iteration < numIterations; };
            Func<int, bool> predicateWithException = (int iteration) => { if (iteration < numIterationsTillException) return true; else throw new Exception("Predicate throws premature exception"); };
            Func<int, bool> predicate = (throwInPredicate) ? predicateWithException : predicateClean;
            Func<int, AsyncCompletion> function = (int iteration) =>
            {
                return AsyncCompletion.StartNew(() =>
                    {
                        numInside++;
                        Assert.AreEqual(1, numInside);
                        Console.WriteLine("Iteration " + iteration);
                        Assert.AreEqual(iteration, loopCounter);
                        loopCounter++;
                        Thread.Sleep(10);
                        numInside--;
                        if (!throwInPredicate && loopCounter == numIterationsTillException)
                        {
                            throw new Exception("Action throws premature exception");
                        }
                    });
            };

            AsyncCompletion loopTask = AsyncLoop.While(predicate, function);
            try
            {
                loopTask.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc)
            {
                Assert.AreEqual(numIterationsTillException, loopCounter, "loopCounter == numIterationsTillException / 2");
                Console.WriteLine("Exception " + exc.GetBaseException().Message + " after " + loopCounter + " iterations.");
            }
            Console.WriteLine("\n\n--------------------------------------------\n");
        }

        //[TestMethod]
        //public void AsyncLoopTest_While2_Simple()
        //{
        //    int loopCounter = 0;
        //    int numInside = 0;
        //    Func<int, bool> predicate = (int iteration) => { return iteration < numIterations; } ;
        //    Action<int> action = (int iteration) =>
        //    {
        //        numInside++;
        //        Assert.AreEqual(1, numInside);
        //        Console.WriteLine("Iteration " + iteration);
        //        Assert.AreEqual(iteration, loopCounter);
        //        loopCounter++;
        //        numInside--;
        //    };
        //    AsyncCompletion loopTask = AsyncLoop.While2(predicate, action);
        //    loopTask.Wait();
        //    Assert.AreEqual(numIterations, loopCounter);
        //    Console.WriteLine("\n\n--------------------------------------------\n");
        //}

        //[TestMethod]
        //public void AsyncLoopTest_While2_Exception()
        //{
        //    int numIterationsTillException = 1;
        //    AsyncLoopTest_While2_Exception(true, numIterationsTillException);
        //    AsyncLoopTest_While2_Exception(false, numIterationsTillException);

        //    numIterationsTillException = 5;
        //    AsyncLoopTest_While2_Exception(true, numIterationsTillException);
        //    AsyncLoopTest_While2_Exception(false, numIterationsTillException);
        //}

        //public void AsyncLoopTest_While2_Exception(bool throwInPredicate, int numIterationsTillException)
        //{
        //    int loopCounter = 0;
        //    int numInside = 0;
        //    Func<int, bool> predicateClean = (int iteration) => { return iteration < numIterations; };
        //    Func<int, bool> predicateWithException = (int iteration) => { if (iteration < numIterationsTillException) return true; else throw new Exception("Predicate throws premature Exception"); };
        //    Func<int, bool> predicate = (throwInPredicate) ? predicateWithException : predicateClean;
        //    Action<int> action = (int iteration) =>
        //    {
        //        numInside++;
        //        Assert.AreEqual(1, numInside);
        //        Console.WriteLine("Iteration " + iteration);
        //        Assert.AreEqual(iteration, loopCounter);
        //        loopCounter++;
        //        Thread.Sleep(10);
        //        numInside--;
        //        if (!throwInPredicate && loopCounter == numIterationsTillException)
        //        {
        //            throw new Exception("Action throws premature Exception.");
        //        }
        //    };

        //    AsyncCompletion loopTask = AsyncLoop.While2(predicate, action);
        //    try
        //    {
        //        loopTask.Wait();
        //        Assert.Fail("Should have thrown");
        //    }
        //    catch (Exception)
        //    {
        //        Assert.AreEqual(numIterationsTillException, loopCounter, "loopCounter == numIterationsTillException / 2");
        //    }
        //    Console.WriteLine("\n\n--------------------------------------------\n");
        //}
    }
}

