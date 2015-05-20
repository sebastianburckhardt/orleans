using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using Orleans.Scheduler;
using UnitTests.SchedulerTests;

namespace UnitTests.AsyncPrimitivesTests
{
    // ReSharper disable ConvertToConstant.Local

    [TestClass]
    public class AC_FromTask_Tests
    {
        [TestCleanup]
        public void TestTeardown()
        {
            Logger.SetTraceLevelOverrides(new List<Tuple<string, OrleansLogger.Severity>>()); // Reset Log level overrides
            UnitTestBase.CheckForUnobservedPromises();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask()
        {
            bool done = false;
            Task t = new Task(() => { done = true; });
            AsyncCompletion ac = AsyncCompletion.FromTask(t);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(ac.IsCompleted, "AsyncCompletion wrapper should have completed");
            Assert.IsFalse(ac.IsFaulted, "AsyncCompletion wrapper should not thrown exception: " + ac.Exception);
            Assert.IsTrue(done, "Task should be done");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask()
        {
            int expected = 2;
            bool done = false;
            Task<int> t = new Task<int>(() => { done = true; return expected; });
            AsyncValue<int> av = AsyncValue.FromTask(t);
            int received = av.GetValue(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(av.IsCompleted, "AsyncValue wrapper should have completed");
            Assert.IsFalse(av.IsFaulted, "AsyncValue wrapper should not thrown exception: " + av.Exception);
            Assert.IsTrue(done, "Task should be done");
            Assert.AreEqual(expected, received, "Task did not return expected value " + expected);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_Error()
        {
            Exception caughtException = null;
            Task t = new Task(() => { throw new MyTestException(); });
            AsyncCompletion ac = AsyncCompletion.FromTask(t);
            try
            {
                ac.Wait(TimeSpan.FromSeconds(1));
                Assert.Fail("Exception should have been thrown");
            }
            catch (Exception ex)
            {
                caughtException = ex;
                while (caughtException is AggregateException) caughtException = caughtException.InnerException;
            }
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsNotNull(caughtException, "Task should have thrown exception");
            Assert.IsInstanceOfType(caughtException, typeof(MyTestException), "Task threw wrong exception type: " + caughtException);
            Assert.IsTrue(t.IsFaulted, "Task should have thrown exception");
            Exception taskException = t.Exception;
            while (taskException is AggregateException) taskException = taskException.InnerException;
            Assert.IsInstanceOfType(taskException, typeof(MyTestException), "Task threw wrong exception type: " + t.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_Error()
        {
            Exception caughtException = null;
            Task<int> t = new Task<int>(() => { throw new MyTestException(); });
            AsyncValue<int> av = AsyncValue.FromTask(t);
            try
            {
                int received = av.GetValue(TimeSpan.FromSeconds(1));
                Assert.Fail("Exception should have been thrown, rather than returned value=" + received);
            }
            catch (Exception ex)
            {
                caughtException = ex;
                while (caughtException is AggregateException)
                {
                    caughtException = caughtException.InnerException;
                }
            }
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsNotNull(caughtException, "Task should have thrown exception");
            Assert.IsInstanceOfType(caughtException, typeof(MyTestException), "Task threw wrong exception type: " + caughtException);
            Assert.IsTrue(t.IsFaulted, "Task should have thrown exception");
            Exception taskException = t.Exception;
            while (taskException is AggregateException) taskException = taskException.InnerException;
            Assert.IsInstanceOfType(taskException, typeof(MyTestException), "Task threw wrong exception type: " + t.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_WithStart()
        {
            bool done = false;
            Task t = new Task(() => { done = true; });
            t.Start();
            AsyncCompletion ac = AsyncCompletion.FromTask(t);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(done, "Task should be done");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_WithStart()
        {
            int expected = 2;
            bool done = false;
            Task<int> t = new Task<int>(() => { done = true; return expected; });
            t.Start();
            AsyncValue<int> av = AsyncValue.FromTask(t);
            int received = av.GetValue(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(done, "Task should be done");
            Assert.AreEqual(expected, received, "Task did not return expected value " + expected);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_WithStart_OrleansTaskScheduler()
        {
            TaskSchedulerTests.InitSchedulerLogging();
            UnitTestSchedulingContext cntx = new UnitTestSchedulingContext();
            OrleansTaskScheduler scheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(cntx);

            bool done = false;
            Task t = new Task(() => { done = true; });
            t.Start(scheduler);
            AsyncCompletion ac = AsyncCompletion.FromTask(t);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(done, "Task should be done");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_WithStart_OrleansTaskScheduler()
        {
            TaskSchedulerTests.InitSchedulerLogging();
            UnitTestSchedulingContext cntx = new UnitTestSchedulingContext();
            OrleansTaskScheduler scheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(cntx);

            int expected = 2;
            bool done = false;
            Task<int> t = new Task<int>(() => { done = true; return expected; });
            t.Start(scheduler);
            AsyncValue<int> av = AsyncValue.FromTask(t);
            int received = av.GetValue(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(done, "Task should be done");
            Assert.AreEqual(expected, received, "Task did not return expected value " + expected);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_WithStart_ActivationTaskScheduler()
        {
            TaskSchedulerTests.InitSchedulerLogging();
            UnitTestSchedulingContext cntx = new UnitTestSchedulingContext();
            OrleansTaskScheduler masterScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(cntx);
            ActivationTaskScheduler activationScheduler = masterScheduler.GetWorkItemGroup(cntx).TaskRunner;

            int expected = 2;
            bool done = false;
            Task<int> t = new Task<int>(() => { done = true; return expected; });
            t.Start(activationScheduler);
            AsyncValue<int> av = AsyncValue.FromTask(t)
                .ContinueWith(i =>
                {
                    Console.WriteLine("AV.FromTask.ContinueWith - SynchronizationContext.Current={0} TaskScheduler.Current={1}",
                        SynchronizationContext.Current, TaskScheduler.Current);

                    return new AsyncValue<int>(i);
                });
            int received = av.GetValue(TimeSpan.FromSeconds(1));
            Assert.IsTrue(t.IsCompleted, "Task should have completed");
            Assert.IsFalse(t.IsFaulted, "Task should not thrown exception: " + t.Exception);
            Assert.IsTrue(done, "Task should be done");
            Assert.AreEqual(expected, received, "Task did not return expected value " + expected);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_AC_TaskDone()
        {
            AsyncCompletion ac = AsyncCompletion.FromTask(AsyncCompletion.TaskDone);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(ac.IsCompleted, "AsyncCompletion wrapper should have completed");
            Assert.IsFalse(ac.IsFaulted, "AsyncCompletion wrapper should not thrown exception: " + ac.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_TaskDone_Done()
        {
            AsyncCompletion ac = AsyncCompletion.FromTask(TaskDone.Done);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(ac.IsCompleted, "AsyncValue wrapper should have completed");
            Assert.IsFalse(ac.IsFaulted, "AsyncValue wrapper should not thrown exception: " + ac.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_Null()
        {
            AsyncCompletion ac = AsyncCompletion.FromTask(null);
            ac.Wait(TimeSpan.FromSeconds(1));
            Assert.IsTrue(ac.IsCompleted, "AsyncCompletion wrapper should have completed");
            Assert.IsFalse(ac.IsFaulted, "AsyncCompletion wrapper should not thrown exception: " + ac.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_Null()
        {
            int expected = default(int);
            AsyncValue<int> av = AsyncValue<int>.FromTask(null);
            int received = av.GetValue(TimeSpan.FromSeconds(1));
            Assert.IsTrue(av.IsCompleted, "AsyncValue wrapper should have completed");
            Assert.IsFalse(av.IsFaulted, "AsyncValue wrapper should not thrown exception: " + av.Exception);
            Assert.AreEqual(expected, received, "AsyncValue wrapper did not return expected value " + expected);
        }

        [TestMethod]
        public void AsyncCompletionThrowTest()
        {
            bool thrown = false;
            try
            {
                AsyncCompletion.StartNew(() => { }).ContinueWith(() => { throw new ApplicationException("AKK!"); }).Wait();
            }
            catch (Exception)
            {
                thrown = true;
            }
            Assert.IsTrue(thrown);
        }

        [TestMethod]
        public void TaskThrowTest()
        {
            bool thrown = false;
            try
            {
                AsyncCompletion.StartNew(() => { }).ContinueWith(() => { throw new ApplicationException("AKK!"); }).AsTask().Wait();
            }
            catch (Exception)
            {
                thrown = true;
            }
            Assert.IsTrue(thrown);
        }

        [TestMethod]
        public async Task TaskThrowTest_await()
        {
            bool thrown = false;
            try
            {
                await AsyncCompletion.StartNew(() => { }).ContinueWith(() => { throw new ApplicationException("AKK!"); }).AsTask();
            }
            catch (Exception)
            {
                thrown = true;
            }
            Assert.IsTrue(thrown);
        }
    }

    [Serializable]
    public class MyTestException : Exception
    {
    }

    // ReSharper restore ConvertToConstant.Local
}
