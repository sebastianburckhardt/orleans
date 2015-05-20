using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Scheduler;

using Orleans;
using System;

namespace UnitTests.SchedulerTests
{
    internal class UnitTestSchedulingContext : ISchedulingContext
    {
        public SchedulingContextType ContextType { get { return SchedulingContextType.Activation; } }

        public string Name { get { return "UnitTestSchedulingContext"; } }

        #region IEquatable<ISchedulingContext> Members

        public bool Equals(ISchedulingContext other)
        {
            return base.Equals(other);
        }

        #endregion
    }

    [TestClass]
    public class SchedulerBasicTests
    {
        [TestInitialize]
        public void MyTestInitialize()
        {
            TaskSchedulerTests.InitSchedulerLogging();
        }

        [TestCleanup]
        public void MyTestCleanup()
        {
            Logger.UnInitialize();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_SimpleFifoTest()
        {
            // This is not a great test because there's a 50/50 shot that it will work even if the scheduling
            // is completely and thoroughly broken and both closures are executed "simultaneously"
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            OrleansTaskScheduler orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            ActivationTaskScheduler scheduler = orleansTaskScheduler.GetWorkItemGroup(context).TaskRunner;

            int n = 0;
            // ReSharper disable AccessToModifiedClosure
            IWorkItem item1 = new ClosureWorkItem(() => { n = n + 5; });
            IWorkItem item2 = new ClosureWorkItem(() => { n = n * 3; });
            // ReSharper restore AccessToModifiedClosure
            orleansTaskScheduler.QueueWorkItem(item1, context);
            orleansTaskScheduler.QueueWorkItem(item2, context);

            // Pause to let things run
            Thread.Sleep(1000);

            // N should be 15, because the two tasks should execute in order
            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(15, n, "Work items executed out of order");
            Console.WriteLine("Test executed OK.");
            orleansTaskScheduler.Stop();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_Task_TplFifoTest()
        {
            // This is not a great test because there's a 50/50 shot that it will work even if the scheduling
            // is completely and thoroughly broken and both closures are executed "simultaneously"
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            OrleansTaskScheduler orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            ActivationTaskScheduler scheduler = orleansTaskScheduler.GetWorkItemGroup(context).TaskRunner;

            int n = 0;
            
            // ReSharper disable AccessToModifiedClosure
            Task task1 = new Task(() => { Thread.Sleep(1000); n = n + 5; });
            Task task2 = new Task(() => { n = n * 3; });
            // ReSharper restore AccessToModifiedClosure

            task1.Start(scheduler);
            task2.Start(scheduler);

            // Pause to let things run
            Thread.Sleep(2000);

            // N should be 15, because the two tasks should execute in order
            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(15, n, "Work items executed out of order");
            Console.WriteLine("Test executed OK.");
            orleansTaskScheduler.Stop();
        }

        private void Sched_ExampleRunOnOrleansScheduler()
        {
            UnitTestSchedulingContext context = new UnitTestSchedulingContext(); // context mimics an activation context - single threaded boundaries.
            OrleansTaskScheduler scheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            try
            {
                Task task1 = new Task((object x) =>
                    {
                        Console.WriteLine("#1 - new Task - SynchronizationContext.Current={0}. TaskScheduler.Current={1}",
                                SynchronizationContext.Current, TaskScheduler.Current);
                        Assert.AreEqual(scheduler, TaskScheduler.Current, "TaskScheduler.Current #1");


                        // put your code here - invoke any method and all its async activities will use OrleansTaskScheduler as its current scheduler
                        // if this code return AsyncCompletion or Task, make sure to Wait for it.

                    }, context);
                task1.Start(scheduler);
                task1.Wait();
            }finally
            {
                scheduler.Stop();
                OrleansTask.Reset();
            }
        }
    }
}
