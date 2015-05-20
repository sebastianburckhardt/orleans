using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Schedulers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Runtime.Scheduler;
using Orleans.Scheduler;
using UnitTests.SchedulerTests;

namespace UnitTests.AsyncPrimitivesTests
{
    /// <summary>
    /// NOTE: Checking these test cases in for future reference. 
    /// 
    /// I added these test cases while exploring the interaction 
    /// between .NET SynchronizationContext and AsyncValue promises. 
    /// 
    /// As it turns out, seems like SynchronisationContext itself does not cause 
    /// any particular problem, but what does is if TaskScheduler 
    /// being used chooses to apply some semi-strict rules to the order 
    /// that it will execute Task's in.
    /// </summary>
    [TestClass]
    public class AC_WithScheduler_Tests
    {
        private readonly AC_AsyncValueTests baseTest;
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan waitTime = TimeSpan.FromSeconds(1);

        public AC_WithScheduler_Tests()
        {
            baseTest = new AC_AsyncValueTests();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            SynchronizationContext.SetSynchronizationContext(null);
            baseTest.MyTestInitialize();
            TaskSchedulerTests.InitSchedulerLogging();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            baseTest.MyTestCleanup();
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncCompletionWait()
        {
            Action testAction = baseTest.AC_AsyncCompletionWait;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncCompletionStartNew()
        {
            Action testAction = baseTest.AC_AsyncCompletionStartNew;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncValueStartNew()
        {
            Action testAction = baseTest.AC_AsyncValueStartNew;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncCompletion_WaitForDone()
        {
            Action testAction = baseTest.AC_AsyncCompletion_WaitForDone;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncCompletion_DoubleWait()
        {
            Action testAction = baseTest.AC_AsyncCompletion_DoubleWait;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncValueWait()
        {
            Action testAction = baseTest.AC_AsyncValueWait;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ErrorBroken()
        {
            Action testAction = baseTest.AC_ErrorBroken;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ErrorException()
        {
            Action testAction = baseTest.AC_ErrorException;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ContinueWith()
        {
            Action testAction = baseTest.AC_ContinueWith;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ContinueWithException()
        {
            Action testAction = baseTest.AC_ContinueWithException;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ContinueWithValue()
        {
            Action testAction = baseTest.AC_ContinueWithValue;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ContinueWithAsyncValue()
        {
            Action testAction = baseTest.AC_ContinueWithAsyncValue;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_ContinueWithValueException()
        {
            Action testAction = baseTest.AC_ContinueWithValueException;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncValueContinueWith()
        {
            Action testAction = baseTest.AC_AsyncValueContinueWith;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncValueContinueWithException()
        {
            Action testAction = baseTest.AC_AsyncValueContinueWithException;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_Resolver()
        {
            Action testAction = baseTest.AC_Resolver;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncValueResolver()
        {
            Action testAction = baseTest.AC_AsyncValueResolver;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsTask_Wait()
        {
            Action testAction = baseTest.AC_AsTask_Wait;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsTask_ErrorBroken()
        {
            Action testAction = baseTest.AC_AsTask_ErrorBroken;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsTask_ContinueWith()
        {
            Action testAction = baseTest.AC_AsTask_ContinueWith;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsTask_ContinueWithException()
        {
            Func<Task> asyncTestAction = baseTest.AC_AsTask_ContinueWithException;
            RunTestWithSchedulers(asyncTestAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void AVS_AsTask_ErrorBroken()
        {
            Action testAction = baseTest.AV_AsTask_ErrorBroken;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutorWithRetriesTest_1()
        {
            Action testAction = baseTest.AC_AsyncExecutorWithRetriesTest_1;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutorWithRetriesTest_2()
        {
            Action testAction = baseTest.AC_AsyncExecutorWithRetriesTest_2;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutorWithRetriesTest_3()
        {
            Action testAction = baseTest.AC_AsyncExecutorWithRetriesTest_3;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutor_BackoffTest_1()
        {
            Action testAction = baseTest.AC_AsyncExecutor_BackoffTest_1;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutorWithRetriesTest_4()
        {
            Action testAction = baseTest.AC_AsyncExecutorWithRetriesTest_4;
            RunTestWithSchedulers(testAction);
        }

        // TODO: [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        [TestMethod, TestCategory("Failures"), TestCategory("AsynchronyPrimitives")]
        public void ACS_AsyncExecutorWithRetriesTest_5()
        {
            Action testAction = baseTest.AC_AsyncExecutorWithRetriesTest_5;
            RunTestWithSchedulers(testAction);
        }

        // ---------- Utility Methods ----------
        private void RunTestWithSchedulers(Func<Task> asyncTestAction)
        {
            Action testAction = async () =>
            {
                try
                {
                    await asyncTestAction();
                }
                catch (Exception exc)
                {
                    throw new AggregateException("Test failed in async action", exc);
                }
            };
            RunTestWithSchedulers(testAction);
        }

        internal static void RunTestWithSchedulers(Action testAction)
        {
            RunTestUnderScheduler(TaskScheduler.Default, testAction);

            // SynchronizationContextTaskScheduler
            SynchronizationContext syncContext = new SynchronizationContext();
            SynchronizationContext.SetSynchronizationContext(syncContext);
            TaskScheduler syncContextTaskScheduler = new SynchronizationContextTaskScheduler(syncContext);
            RunTestUnderScheduler(syncContextTaskScheduler, testAction);
            SynchronizationContext.SetSynchronizationContext(null);

            // FromCurrentSynchronizationContext
            syncContext = new SynchronizationContext();
            SynchronizationContext.SetSynchronizationContext(syncContext);
            TaskScheduler contextTaskScheduler = TaskScheduler.FromCurrentSynchronizationContext();
            RunTestUnderScheduler(contextTaskScheduler, testAction);
            SynchronizationContext.SetSynchronizationContext(null);

            //// FAILS: OrderedTaskScheduler
            //TaskScheduler orderedTaskScheduler = new OrderedTaskScheduler();
            //RunTestUnderScheduler(orderedTaskScheduler, testAction);

            // FAILS: OrleansTaskScheduler
            OrleansTaskScheduler orleansTaskScheduler = new OrleansTaskScheduler(2);
            RunTestUnderScheduler(orleansTaskScheduler, testAction);

            // FAILS: ActivationTaskScheduler
            ISchedulingContext context = new OrleansContext(0);
            WorkItemGroup wg = orleansTaskScheduler.RegisterWorkContext(context);
            TaskScheduler activationTaskScheduler = new ActivationTaskScheduler(wg);
            RunTestUnderScheduler(activationTaskScheduler, testAction);
        }

        internal static void RunTestUnderScheduler(TaskScheduler scheduler, Action testAction)
        {
            Console.WriteLine(TaskScheduler.Current);

            String syncContextInfo = SynchronizationContext.Current == null
                ? "null" : String.Format("{0}-{1}", SynchronizationContext.Current, SynchronizationContext.Current.GetHashCode());

            Console.WriteLine("RunTestUnderScheduler TargetScheduler={0} CurrentScheduler={1} SynchronizationContext={2}",
                scheduler, TaskScheduler.Current, syncContextInfo);

            Task testTask = new Task(testAction);

            Console.WriteLine("RunTestUnderScheduler Starting Task {0} with SynchronizationContext={1}",
                testTask.Id, syncContextInfo);

            testTask.Start(scheduler);

            Thread.Sleep(waitTime);

            bool ok = testTask.Wait(timeout);

            if (!ok)
            {
                string msg = string.Format(
                    "Test did not finish within timeout {0} with TaskScheduler={1}",
                    timeout, scheduler.GetType().FullName);
                Exception exc = new TimeoutException(msg);

                Console.WriteLine("**** ERROR {0}", exc);

                if (OrleansTaskScheduler.Instance != null)
                {
                    OrleansTaskScheduler.Instance.DumpSchedulerStatus(true);
                }

                throw exc;
            }
        }
    }
}