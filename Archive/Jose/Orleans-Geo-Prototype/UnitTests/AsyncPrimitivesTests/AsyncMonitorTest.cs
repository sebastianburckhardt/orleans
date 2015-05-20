using System;
using System.Threading;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

namespace UnitTests
{
    /// <summary>
    /// Summary description for PersistenceTest
    /// </summary>
    [TestClass]
    public class AsyncMonitorTest
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        public AsyncMonitorTest()
        {
            //
            // TODO: Add constructor logic here
            //
        }
        [TestInitialize]
        [TestCleanup]
        public void MyTestCleanup()
        {
            OrleansTask.Reset();
        }

        static private void onActivityCompletion(AsyncCompletion activity)
        {
            Debug.WriteLine("AsyncCompletion " + activity + " finished.");
        }

        [TestMethod]
        public void AsyncMonitorTestBasic()
        {
            ResultHandle result = new ResultHandle();
            int finished = 0;
            AsyncMonitor monitor = new AsyncMonitor();
            monitor.CompletionEvents += ((AsyncCompletion activity) =>
            {
                lock (result)
                {
                    Debug.WriteLine("TEST: AsyncCompletion " + activity + " finished.");
// ReSharper disable AccessToModifiedClosure
                    finished++;
// ReSharper restore AccessToModifiedClosure
                    if (finished == 3)
                    {
                        result.Done = true;
                    }
                }
            });

            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.SetA(3);
            AsyncCompletion promise2 = localGrain.SetB(4);
            AsyncValue<Int32> promise3 = localGrain.GetAxBError();
            monitor.AddActivity(promise1);
            monitor.AddActivity(promise2);
            monitor.AddActivity(promise3);

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise3.Status == AsyncCompletionStatus.Faulted);

            //-------------------
            finished = 0;
            AsyncCompletion[] promises = new AsyncCompletion[3];
            promises[0] = promise1;
            promises[1] = promise2;
            promises[2] = promise3;
            monitor.AddActivities(promises);

            try
            {
                promise3.Wait();
                Assert.Fail("Should have thrown");
            }
            catch (Exception)
            {
                Assert.IsTrue(promise3.Status == AsyncCompletionStatus.Faulted);
            }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise3.Status == AsyncCompletionStatus.Faulted);
        }

        [TestMethod]
        public void AsyncMonitorTestRemovals()
        {
            ResultHandle result = new ResultHandle();
            int finished = 0;
            AsyncMonitor monitor = new AsyncMonitor();
            monitor.CompletionEvents += ((AsyncCompletion activity) =>
            {
                lock (result)
                {
                    Debug.WriteLine("TEST: AsyncCompletion " + activity + " finished.");
                    finished++;
                    // will fire only twide, since removed 2 before they finished.
                    if (finished == 2)
                    {
                        result.Done = true;
                    }
                }
            });

            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.LongMethod(2000);
            AsyncCompletion promise2 = localGrain.LongMethod(2000);
            AsyncCompletion promise3 = localGrain.LongMethod(2000);
            AsyncCompletion promise4 = localGrain.LongMethodWithError(2000);
            monitor.AddActivity(promise1);
            monitor.AddActivity(promise2);
            monitor.AddActivity(promise3);
            monitor.AddActivity(promise4);

            if (!ErrorGrainTest.USE_SYNC_ORLEANS_TASK)
            {
                monitor.RemoveActivity(promise1);
                Thread.Sleep(1000);
                try
                {
                    monitor.RemoveActivity(promise1);
                    Assert.Fail("Should have thrown, since already removed before");
                }
                catch (Exception) { }

                monitor.RemoveActivity(promise2);
                Thread.Sleep(3000);
                try
                {
                    monitor.RemoveActivity(promise3); // its too late to remove it - it will fire any way
                    Assert.Fail("Should have thrown, since already finished");
                }
                catch (Exception) { }
            }
            else
            {
                //already finished and automatically removed
                try
                {
                    monitor.RemoveActivity(promise1);
                    Assert.Fail("Should have thrown, since already removed before");
                }
                catch (Exception) { }
            }

            promise1.Wait();
            promise2.Wait();
            promise3.Wait();
            try
            {
                promise4.Wait();
                Assert.Fail("Should have thrown, since already removed before");
            }
            catch (Exception) { }

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise4.Status == AsyncCompletionStatus.Faulted);
        }
    }
}
