using System;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrains;

namespace UnitTests
{
    /// <summary>
    /// Summary description for ErrorHandlingGrainTest
    /// </summary>
    [TestClass]
    public class TimeoutTests : UnitTestBase
    {
        private TimeSpan originalTimeout;

        public TimeoutTests()
        {
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            //ResetDefaultRuntimes();
        }

        [TestCleanup()]
        public void Cleanup()
        {
            ResetDefaultRuntimes();
            //GrainClient.Current.SetResponseTimeout(originalTimeout);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void Timeout_LongMethod()
        {
            originalTimeout = GrainClient.Current.GetResponseTimeout();
            ResultHandle result = new ResultHandle();
            bool finished = false;
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");
            TimeSpan timeout = TimeSpan.FromMilliseconds(1000);
            GrainClient.Current.SetResponseTimeout(timeout);

            AsyncCompletion promise = AsyncCompletion.FromTask(grain.LongMethod((int)timeout.Multiply(4).TotalMilliseconds));
            //promise = grain.LongMethodWithError(2000);

            // there is a race in the test here. If run in debugger, the invocation can actually finish OK
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                finished = promise.TryWait(timeout.Multiply(3));
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc)
            {
                stopwatch.Stop();
                Exception baseExc = exc.GetBaseException();
                if (!(baseExc is TimeoutException))
                {
                    Assert.Fail("Should not have got here " + exc);
                }
            }
            Console.WriteLine("Waited for " + stopwatch.Elapsed);
            Assert.IsTrue(!finished);
            Assert.IsTrue(stopwatch.Elapsed >= timeout.Multiply(0.9), "Waited less than " + timeout.Multiply(0.9) + ". Waited " + stopwatch.Elapsed);
            Assert.IsTrue(stopwatch.Elapsed <= timeout.Multiply(2), "Waited longer than " + timeout.Multiply(2) + ". Waited " + stopwatch.Elapsed);
            Assert.IsTrue(promise.Status == AsyncCompletionStatus.Faulted);

            // try to re-use the promise and should fail immideately.
            try
            {
                stopwatch = new Stopwatch();
                promise.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc)
            {
                stopwatch.Stop();
                Exception baseExc = exc.GetBaseException();
                if (!(baseExc is TimeoutException))
                {
                    Assert.Fail("Should not have got here " + exc);
                }
            }
            Assert.IsTrue(stopwatch.Elapsed <= timeout.Multiply(0.1), "Waited longer than " + timeout.Multiply(0.1) + ". Waited " + stopwatch.Elapsed);
            Assert.IsTrue(promise.Status == AsyncCompletionStatus.Faulted);
        }


        [TestMethod, TestCategory("Failures")]
        public void Timeout_FailedSilo()
        {
            originalTimeout = GrainClient.Current.GetResponseTimeout();
            ResultHandle result = new ResultHandle();
            TimeSpan timeout = TimeSpan.FromSeconds(1);
            GrainClient.Current.SetResponseTimeout(timeout);
            IErrorGrain grain = ErrorGrainFactory.GetGrain(GetRandomGrainId(), "UnitTestGrains.ErrorGrain");

            AsyncCompletion promise = AsyncCompletion.FromTask(grain.SetA(2));
            promise.Wait();
            Console.WriteLine(grain.GetA().Result);
            grain.SetA(3).Wait();

            ResetAllAdditionalRuntimes();
            StopRuntime(Primary);
            StopRuntime(Secondary);

            AsyncValue<int> promiseValue = AsyncValue.FromTask(grain.GetA());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            bool timeoutHappened = false;
            bool retryExceeded = false;
            try
            {
                int val = promiseValue.GetValue();
                Assert.Fail("Should have thrown " + val);
            }
            catch (Exception exc)
            {
                stopwatch.Stop();
                Exception baseExc = exc.GetBaseException();
                
                if (baseExc is TimeoutException)
                    timeoutHappened = true;
                if(baseExc is OrleansException)
                    if(baseExc.Message.StartsWith("Retry count exceeded"))
                        retryExceeded = true;

                if (!timeoutHappened && !retryExceeded)
                {
                    Assert.Fail("Should not have got here " + exc);
                }
                Console.WriteLine("Have thrown TimeoutException or Retry count exceeded correctly.");
            }
            if (timeoutHappened)
            {
                Assert.IsTrue(stopwatch.Elapsed >= timeout.Multiply(0.9), "Waited less than " + timeout.Multiply(0.9) + ". Waited " + stopwatch.Elapsed);
            }
            Assert.IsTrue(stopwatch.Elapsed <= timeout.Multiply(1.5), "Waited longer than " + timeout.Multiply(1.5) + ". Waited " + stopwatch.Elapsed);
        }
    }
}
