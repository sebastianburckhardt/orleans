using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrains;

namespace UnitTests
{
    /// <summary>
    /// Summary description for ErrorHandlingGrainTest
    /// </summary>
    [TestClass]
    public class ErrorGrainTest : UnitTestBase
    {
        public static bool USE_SYNC_ORLEANS_TASK = false;
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        public ErrorGrainTest()
        {
            this.logger = Logger.GetLogger("ErrorGrainTest", Logger.LoggerType.Application);
        }

        public ErrorGrainTest(int dummy) : base(new Options
                    {
                        StartSecondary = false, 
                    })
        {
            this.logger = Logger.GetLogger("ErrorGrainTest", Logger.LoggerType.Application);
        }

        [ClassCleanup]
        public static void MyClassCleanup()
        {
            //ResetDefaultRuntimes();
        }

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext { get; set; }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
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
        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public async Task ErrorGrain_GetGrain()
        {
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");
            int ignored = await grain.GetA();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void ErrorHandlingLocalError()
        {
            ResultHandle result = new ResultHandle();
            LocalErrorGrain localGrain = new LocalErrorGrain();
            
            AsyncValue<int> intPromise = localGrain.GetAxBError();

            AsyncCompletion contPromise = intPromise.ContinueWith(x =>
            {
                Assert.Fail("Should not have executed");
            });
            try
            {
                contPromise.Wait();
            }
            catch (Exception exc2)
            {
                result.Result = 2;
                result.Exception = exc2;
                result.Done = true;
            }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(intPromise.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(contPromise.Status == AsyncCompletionStatus.Faulted);
            Assert.IsNotNull(result.Result);
            Assert.IsNotNull(result.Exception);
            Assert.AreEqual(2, result.Result);
            Assert.AreEqual(result.Exception.GetBaseException().Message, (new Exception("GetAxBError-Exception")).Message);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        // check that grain that throws an error breaks its promise and later Wait and GetValue on it will throw
        public void ErrorHandlingGrainError1()
        {
            ResultHandle result = new ResultHandle();
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");

            Task<int> intPromise = grain.GetAxBError();
            try
            {
                intPromise.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception)
            {
                Assert.IsTrue(intPromise.Status == TaskStatus.Faulted);
            }

            try
            {
                intPromise.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc2)
            {
                Assert.IsTrue(intPromise.Status == TaskStatus.Faulted);
                result.Result = 2;
                result.Exception = exc2;
                result.Done = true;
            }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(intPromise.Status == TaskStatus.Faulted);
            Assert.IsNotNull(result.Result);
            Assert.IsNotNull(result.Exception);
            Assert.AreEqual(2, result.Result);
            Assert.AreEqual((new Exception("GetAxBError-Exception")).Message, result.Exception.GetBaseException().Message);
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        // check that grain that throws an error breaks its promise and later ContinueWith will not execute its callback, but break the cont promise.
        public void ErrorHandlingGrainError2()
        {
            ResultHandle result = new ResultHandle();
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");

            AsyncValue<int> intPromise = AsyncValue.FromTask(grain.GetAxBError());

            AsyncCompletion contPromise = intPromise.ContinueWith(x =>
            {
                // since intPromise will be broken, ContinueWith will not execute and contPromise will be broken.
                Assert.Fail("Should not have executed");
            });
            try
            {
                contPromise.Wait();
            }
            catch (Exception exc2)
            {
                result.Result = 2;
                result.Exception = exc2;
                result.Done = true;
            }
            
            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(intPromise.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(contPromise.Status == AsyncCompletionStatus.Faulted);
            Assert.IsNotNull(result.Result);
            Assert.IsNotNull(result.Exception);
            Assert.AreEqual(2, result.Result);
            Assert.AreEqual((new Exception("GetAxBError-Exception")).Message, result.Exception.GetBaseException().Message);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        // check that premature wait finishes on time with false.
        public void ErrorHandlingTimedMethod()
        {
            ResultHandle result = new ResultHandle();
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");

            AsyncCompletion promise = AsyncCompletion.FromTask(grain.LongMethod(2000));

            // there is a race in the test here. If run in debugger, the invocation can actually finish OK
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            bool finished = promise.TryWait(TimeSpan.FromMilliseconds(1000));
            stopwatch.Stop();

            if (!USE_SYNC_ORLEANS_TASK)
            {
                // these asserts depend on timing issues and will be wrong for the sync version of OrleansTask
                Assert.IsTrue(!finished);
                Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 900, "Waited less than 900ms"); // check that we waited at least 0.9 second
                Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1100, "Waited longer than 1100ms");
            }

            promise.Wait(); // just wait for the server side grain invocation to finish
            result.Result = 1;
            result.Done = true;

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(promise.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsNotNull(result.Result);
            Assert.IsNull(result.Exception);
            Assert.AreEqual(1, result.Result);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        // check that premature wait finishes on time but does not throw with false and later wait throws.
        public void ErrorHandlingTimedMethodWithError()
        {
            ResultHandle result = new ResultHandle();
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");

            Task promise = grain.LongMethodWithError(2000);

            // there is a race in the test here. If run in debugger, the invocation can actually finish OK
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            Assert.IsFalse(promise.Wait(1000), "The task shouldn't have completed yet.");

            stopwatch.Stop();
            if (!USE_SYNC_ORLEANS_TASK)
            {
                Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 900, "Waited less than 900ms"); // check that we waited at least 0.9 second
                Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1100, "Waited longer than 1100ms");
            }

            try
            {
                promise.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc2)
            {
                result.Result = 1;
                result.Exception = exc2;
                result.Done = true;
            }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(promise.Status == TaskStatus.Faulted);
            Assert.IsNotNull(result.Result);
            Assert.IsNotNull(result.Exception);
            Assert.AreEqual(1, result.Result);
        }


        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void StressHandlingMultipleDelayedRequests()
        {
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId());
            bool once = true;
            List<Task> tasks = new List<Task>();
            for (int i = 0; i < 500; i++)
            {
                Task promise = grain.DelayMethod(1);
                tasks.Add(promise);
                if (once)
                {
                    once = false;
                    promise.Wait();
                }

            }
            Task.WhenAll(tasks).Wait();
            logger.Info(1, "DONE.");
        }

        //[TestMethod]
        public void ArgumentTypes_ListOfGrainReferences()
        {
            List<IErrorGrain> list = new List<IErrorGrain>();
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");
            list.Add(ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain"));
            list.Add(ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain"));
            bool ok = grain.AddChildren(list).Wait(timeout);
            if (!ok) throw new TimeoutException();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_DelayedExecutor_2()
        {
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");
            Task<bool> promise = grain.ExecuteDelayed(TimeSpan.FromMilliseconds(2000));
            bool result = promise.Result;
            Assert.AreEqual(true, result);
        }
    }
}
