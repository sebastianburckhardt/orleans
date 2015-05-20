using System;
using System.Collections.Generic;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Scheduler;

using Orleans;

namespace UnitTests.SchedulerTests
{
    [TestClass]
    public class SchedulerAdvancedTests : MarshalByRefObject
    {
        private OrleansTaskScheduler orleansTaskScheduler;

        private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan TwoSeconds = TimeSpan.FromSeconds(2);

        private static readonly int waitFactor = Debugger.IsAttached ? 100 : 1;

        [TestInitialize]
        public void MyTestInitialize()
        {
            OrleansTask.Reset();
            TaskSchedulerTests.InitSchedulerLogging();
        }

        [TestCleanup]
        public void MyTestCleanup()
        {
            if (orleansTaskScheduler != null)
            {
                orleansTaskScheduler.Stop();
            }
            OrleansTask.Reset();
            Logger.UnInitialize();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_AsyncCompletionTest()
        {
            int n = 0;
            bool insideTask = false;
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            Console.WriteLine("Running Main in Context=" + RuntimeContext.Current);
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
                {
                    for (int i = 0; i < 10; i++)
                    {
                        AsyncCompletion.StartNew(() => 
                        {
                            // ReSharper disable AccessToModifiedClosure
                            Console.WriteLine("Starting " + i + " in Context=" + RuntimeContext.Current); 
                            Assert.IsFalse(insideTask, "Starting new task when I am already inside task of iteration {0}", n);
                            insideTask = true;
                            int k = n; 
                            Thread.Sleep(100); 
                            n = k + 1;
                            insideTask = false;
                            // ReSharper restore AccessToModifiedClosure
                        }).Ignore();
                    }
                }), context);

            // Pause to let things run
            Thread.Sleep(1500);

            // N should be 10, because all tasks should execute serially
            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(10, n, "Work items executed concurrently");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_WaitTest()
        {
            ResultHandle resultHandle = new ResultHandle();
            int n = 0;
            bool insideTask = false;
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
           
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
                {
                    AsyncCompletion task1 = AsyncCompletion.StartNew(() => 
                    {
                        Console.WriteLine("Starting 1"); 
                        Assert.IsFalse(insideTask, "Starting new task when I am already inside task of iteration {0}", n);
                        insideTask = true;
                        Console.WriteLine("===> 1a"); 
                        Thread.Sleep(1000); n = n + 3; 
                        Console.WriteLine("===> 1b");
                        insideTask = false;
                    });
                    AsyncCompletion task2 = AsyncCompletion.StartNew(() =>
                    {
                        Console.WriteLine("Starting 2");
                        Assert.IsFalse(insideTask, "Starting new task when I am alraedy inside task of iteration {0}", n);
                        insideTask = true;
                        Console.WriteLine("===> 2a");
                        task1.Wait();
                        Console.WriteLine("===> 2b");
                        n = n * 5;
                        Console.WriteLine("===> 2c");
                        insideTask = false;
                        resultHandle.Done = true;
                    });
                    task1.Ignore();
                    task2.Ignore();
                }), context);

            TimeSpan timeoutLimit = TimeSpan.FromMilliseconds(1500);
            Assert.IsTrue(resultHandle.WaitForFinished(timeoutLimit), "Result did not arrive before timeout " + timeoutLimit);

            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(15, n, "Work items executed out of order");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_MainTurnWait_Test()
        {
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(new UnitTestSchedulingContext());
            AsyncCompletion promise = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });
            promise.Wait();
        }

        private bool mainDone;
        private int stageNum1;
        private int stageNum2;

        private void SubProcess1(int n)
        {
            string msg = string.Format("1-{0} MainDone={1} inside Task {2}", n, mainDone, Task.CurrentId);
            Console.WriteLine("1 ===> " + msg);
            Assert.IsTrue(mainDone, msg + " -- Main turn should be finished");
            stageNum1 = n;
        }
        private void SubProcess2(int n)
        {
            string msg = string.Format("2-{0} MainDone={1} inside Task {2}", n, mainDone, Task.CurrentId);
            Console.WriteLine("2 ===> " + msg);
            Assert.IsTrue(mainDone, msg + " -- Main turn should be finished");
            stageNum2 = n;
        }
    
        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_Turn_Execution_Order()
        {
            // Can we add a unit test that basicaly checks that any turn is indeed run till completion before any other turn? 
            // For example, you have a  long running main turn and in the middle it spawns a lot of short CWs (on Done promise) and StartNew. 
            // You test that no CW/StartNew runs until the main turn is fully done. And run in stress.

            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            ResultHandle resultHandle1 = new ResultHandle();
            ResultHandle resultHandle2 = new ResultHandle();

            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
            {
                mainDone = false;
                stageNum1 = stageNum2 = 0;

                AsyncCompletion task1 = AsyncCompletion.StartNew(() => SubProcess1(11));
                AsyncCompletion task2 = task1.ContinueWith(() => SubProcess1(12));
                AsyncCompletion task3 = task2.ContinueWith(() => SubProcess1(13));
                AsyncCompletion task4 = task3.ContinueWith(() => { SubProcess1(14); resultHandle1.Done = true; });
                task4.Ignore();

                AsyncCompletion task21 = AsyncCompletion.Done.ContinueWith(() => SubProcess2(21));
                AsyncCompletion task22 = task21.ContinueWith(() => { SubProcess2(22); resultHandle2.Done = true; });
                task22.Ignore();

                Thread.Sleep(TimeSpan.FromSeconds(1));
                mainDone = true;
            }), context);

            Assert.IsTrue(resultHandle1.WaitForFinished(TimeSpan.FromSeconds(3)), "Timeout-1");
            Assert.IsTrue(resultHandle2.WaitForFinished(TimeSpan.FromSeconds(3)), "Timeout-2");
            Assert.AreNotEqual(0, stageNum1, "Work items did not get executed-1");
            Assert.AreNotEqual(0, stageNum2, "Work items did not get executed-2");
            Assert.AreEqual(14, stageNum1, "Work items executed out of order-1");
            Assert.AreEqual(22, stageNum2, "Work items executed out of order-2");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Scheduler")]
        //[TestMethod, TestCategory("MultithreadingFailures"), TestCategory("Scheduler")]
        public void Sched_Task_Turn_Execution_Order()
        {
            // A unit test that checks that any turn is indeed run till completion before any other turn? 
            // For example, you have a long running main turn and in the middle it spawns a lot of short CWs (on Done promise) and StartNew. 
            // You test that no CW/StartNew runs until the main turn is fully done. And run in stress.

            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            OrleansTaskScheduler masterScheduler = orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            WorkItemGroup workItemGroup = orleansTaskScheduler.GetWorkItemGroup(context);
            ActivationTaskScheduler activationScheduler = workItemGroup.TaskRunner;

            mainDone = false;
            stageNum1 = stageNum2 = 0;

            ResultHandle resultHandle1 = new ResultHandle();
            ResultHandle resultHandle2 = new ResultHandle();

            Task wrapper = null;
            Task finalTask1 = null;
            AsyncCompletion finalPromise2 = null;
            masterScheduler.QueueWorkItem(new ClosureWorkItem(() =>
            {
                Log(1, "Outer ClosureWorkItem " + Task.CurrentId + " starting");
                Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #0");

                Log(2, "Starting wrapper Task");
                wrapper = Task.Factory.StartNew(() =>
                {
                    Log(3, "Inside wrapper Task Id=" + Task.CurrentId);
                    Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #1");

                    // Execution chain #1
                    Log(4, "Wrapper Task Id=" + Task.CurrentId + " creating Task chain");
                    Task task1 = Task.Factory.StartNew(() =>
                    {
                        Log(5, "#11 Inside sub-Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #11");
                        SubProcess1(11);
                    });
                    Task task2 = task1.ContinueWith(task =>
                    {
                        Log(6, "#12 Inside continuation Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #12");
                        if (task.IsFaulted) throw task.Exception.Flatten();
                        SubProcess1(12);
                    });
                    Task task3 = task2.ContinueWith(task =>
                    {
                        Log(7, "#13 Inside continuation Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #13");
                        if (task.IsFaulted) throw task.Exception.Flatten();
                        SubProcess1(13);
                    });
                    finalTask1 = task3.ContinueWith(task =>
                    {
                        Log(8, "#14 Inside final continuation Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #14");
                        if (task.IsFaulted) throw task.Exception.Flatten();
                        SubProcess1(14);
                        resultHandle1.Done = true;
                    });

                    // Execution chain #2
                    Log(9, "Wrapper Task " + Task.CurrentId + " creating AC chain");
                    AsyncCompletion promise2 = AsyncCompletion.StartNew(() =>
                    {
                        Log(10, "#21 Inside sub-Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #21");
                        SubProcess2(21);
                    });
                    finalPromise2 = promise2.ContinueWith(() =>
                    {
                        Log(11, "#22 Inside final continuation Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #22");
                        SubProcess2(22);
                        resultHandle2.Done = true;
                    });
                    finalPromise2.Ignore();

                    Log(12, "Wrapper Task Id=" + Task.CurrentId + " sleeping #2");
                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    Log(13, "Wrapper Task Id=" + Task.CurrentId + " finished");
                });

                Log(14, "Outer ClosureWorkItem Task Id=" + Task.CurrentId + " sleeping");
                Thread.Sleep(TimeSpan.FromSeconds(1));
                Log(15, "Outer ClosureWorkItem Task Id=" + Task.CurrentId + " awake");

                Log(16, "Finished Outer ClosureWorkItem Task Id=" + wrapper.Id);
                mainDone = true;
            }), context);

            Log(17, "Waiting for ClosureWorkItem to spawn wrapper Task");
            for (int i = 0; i < 5 * waitFactor; i++)
            {
                if (wrapper != null) break;
                Thread.Sleep(TimeSpan.FromSeconds(1).Multiply(waitFactor));
            }
            Assert.IsNotNull(wrapper, "Wrapper Task was not created");

            Log(18, "Waiting for wrapper Task Id=" + wrapper.Id + " to complete");
            bool finished = wrapper.Wait(TimeSpan.FromSeconds(2 * waitFactor));
            Log(19, "Done waiting for wrapper Task Id=" + wrapper.Id + " Finished=" + finished);
            if (!finished) throw new TimeoutException();
            Assert.IsFalse(wrapper.IsFaulted, "Wrapper Task faulted: " + wrapper.Exception);
            Assert.IsTrue(wrapper.IsCompleted, "Wrapper Task should be completed");

            Log(20, "Waiting for TaskWorkItem to complete");
            for (int i = 0; i < 15 * waitFactor; i++)
            {
                if (mainDone) break;
                Thread.Sleep(1000 * waitFactor);
            }
            Log(21, "Done waiting for TaskWorkItem to complete MainDone=" + mainDone);
            Assert.IsTrue(mainDone, "Main Task should be completed");
            Assert.IsNotNull(finalTask1, "Task chain #1 not created");
            Assert.IsNotNull(finalPromise2, "Task chain #2 not created");

            Log(22, "Waiting for final task #1 to complete");
            bool ok = finalTask1.Wait(TimeSpan.FromSeconds(4 * waitFactor));
            Log(23, "Done waiting for final task #1 complete Ok=" + ok);
            if (!ok) throw new TimeoutException();
            Assert.IsFalse(finalTask1.IsFaulted, "Final Task faulted: " + finalTask1.Exception);
            Assert.IsTrue(finalTask1.IsCompleted, "Final Task completed");
            Assert.IsTrue(resultHandle1.Done, "Timeout-1");

            Log(24, "Waiting for final promise #2 to complete");
            finalPromise2.Wait(TimeSpan.FromSeconds(4 * waitFactor));
            Log(25, "Done waiting for final promise #2");
            Assert.IsFalse(finalPromise2.IsFaulted, "Final Task faulted: " + finalPromise2.Exception);
            Assert.IsTrue(finalPromise2.IsCompleted, "Final Task completed");
            Assert.IsTrue(resultHandle2.Done, "Timeout-2");

            Assert.AreNotEqual(0, stageNum1, "Work items did not get executed-1");
            Assert.AreEqual(14, stageNum1, "Work items executed out of order-1");
            Assert.AreNotEqual(0, stageNum2, "Work items did not get executed-2");
            Assert.AreEqual(22, stageNum2, "Work items executed out of order-2");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        //[TestMethod, TestCategory("MultithreadingFailures"), TestCategory("Scheduler")]
        public void Sched_AC_Current_TaskScheduler()
        {
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            OrleansTaskScheduler orleansTaskScheduler = orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            ActivationTaskScheduler activationScheduler = orleansTaskScheduler.GetWorkItemGroup(context).TaskRunner;

            // TODO: AsyncCompletion should not always uses Orleans master task scheduler as it's Current
            // RuntimeContext.InitializeThread(masterScheduler);

            mainDone = false;

            ResultHandle resultHandle1 = new ResultHandle();

            Task wrapper = null;
            AsyncCompletion finalPromise = null;
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
            {
                Log(1, "Outer ClosureWorkItem " + Task.CurrentId + " starting");
                Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #0");

                Log(2, "Starting wrapper Task");
                wrapper = Task.Factory.StartNew(() =>
                {
                    Log(3, "Inside wrapper Task Id=" + Task.CurrentId);
                    Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #1");

                    // AsyncCompletion Execution chain
                    Log(4, "Wrapper Task " + Task.CurrentId + " creating AC chain");
                    AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
                    {
                        Log(5, "#1 Inside AC Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #1");
                        SubProcess1(1);
                    });
                    AsyncCompletion promise2 = promise1.ContinueWith(() =>
                    {
                        Log(6, "#2 Inside AC Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #2");
                        SubProcess1(2);
                        resultHandle1.Done = true;
                    });
                    finalPromise = promise2.ContinueWith(() =>
                    {
                        Log(7, "#3 Inside final AC Task Id=" + Task.CurrentId);
                        Assert.AreEqual(activationScheduler, TaskScheduler.Current, "TaskScheduler.Current #3");
                        SubProcess1(3);
                        resultHandle1.Done = true;
                    });
                    finalPromise.Ignore();

                    Log(8, "Wrapper Task Id=" + Task.CurrentId + " sleeping");
                    Thread.Sleep(TimeSpan.FromSeconds(1));

                    Log(9, "Wrapper Task Id=" + Task.CurrentId + " finished");
                });

                Log(10, "Outer ClosureWorkItem Task Id=" + Task.CurrentId + " sleeping");
                Thread.Sleep(TimeSpan.FromSeconds(1));
                Log(11, "Outer ClosureWorkItem Task Id=" + Task.CurrentId + " awake");

                Log(12, "Finished Outer TaskWorkItem Task Id=" + wrapper.Id);
                mainDone = true;
            }), context);

            Log(13, "Waiting for ClosureWorkItem to spawn wrapper Task");
            for (int i = 0; i < 5 * waitFactor; i++)
            {
                if (wrapper != null) break;
                Thread.Sleep(TimeSpan.FromSeconds(1).Multiply(waitFactor));
            }
            Assert.IsNotNull(wrapper, "Wrapper Task was not created");

            Log(14, "Waiting for wrapper Task Id=" + wrapper.Id + " to complete");
            bool finished = wrapper.Wait(TimeSpan.FromSeconds(2 * waitFactor));
            Log(15, "Done waiting for wrapper Task Id=" + wrapper.Id + " Finished=" + finished);
            if (!finished) throw new TimeoutException();
            Assert.IsFalse(wrapper.IsFaulted, "Wrapper Task faulted: " + wrapper.Exception);
            Assert.IsTrue(wrapper.IsCompleted, "Wrapper Task should be completed");

            Log(16, "Waiting for TaskWorkItem to complete");
            for (int i = 0; i < 15 * waitFactor; i++)
            {
                if (mainDone) break;
                Thread.Sleep(1000 * waitFactor);
            }
            Log(17, "Done waiting for TaskWorkItem to complete MainDone=" + mainDone);
            Assert.IsTrue(mainDone, "Main Task should be completed");
            Assert.IsNotNull(finalPromise, "AC chain not created");

            Log(18, "Waiting for final AC promise to complete");
            finalPromise.Wait(TimeSpan.FromSeconds(4 * waitFactor));
            Log(19, "Done waiting for final promise");
            Assert.IsFalse(finalPromise.IsFaulted, "Final AC faulted: " + finalPromise.Exception);
            Assert.IsTrue(finalPromise.IsCompleted, "Final AC completed");
            Assert.IsTrue(resultHandle1.Done, "Timeout-1");

            Assert.AreNotEqual(0, stageNum1, "Work items did not get executed-1");
            Assert.AreEqual(3, stageNum1, "Work items executed out of order-1");
        }
        
        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_ContinueWith_1_Test()
        {
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            ResultHandle handle = new ResultHandle();
            int n = 0;
            // ReSharper disable AccessToModifiedClosure
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
                {
                    AsyncCompletion task1 = AsyncCompletion.StartNew(() => { Console.WriteLine("===> 1a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 1b"); });
                    AsyncCompletion task2 = task1.ContinueWith(() => { n = n * 5; Console.WriteLine("===> 2"); });
                    AsyncCompletion task3 = task2.ContinueWith(() => { n = n / 5; Console.WriteLine("===> 3"); });
                    AsyncCompletion task4 = task3.ContinueWith(() => { n = n - 2; Console.WriteLine("===> 4"); handle.Done = true; });
                    task4.Ignore();
                }), context);
            // ReSharper restore AccessToModifiedClosure

            Assert.IsTrue(handle.WaitForFinished(TwoSeconds));
            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(1, n, "Work items executed out of order");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void Sched_AC_JoinAll_AV()
        {
            ResultHandle handle = new ResultHandle();
            int n = 0;
            AsyncValue<int>[] tasks = null;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            // ReSharper disable AccessToModifiedClosure
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
                {
                    AsyncValue<int> task1 = AsyncValue<int>.StartNew(() => { Console.WriteLine("===> 1a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 1b"); return 1; });
                    AsyncValue<int> task2 = AsyncValue<int>.StartNew(() => { Console.WriteLine("===> 2a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 2b"); return 2; });
                    AsyncValue<int> task3 = AsyncValue<int>.StartNew(() => { Console.WriteLine("===> 3a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 3b"); return 3; });
                    AsyncValue<int> task4 = AsyncValue<int>.StartNew(() => { Console.WriteLine("===> 4a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 4b"); return 4; });
                    tasks = new AsyncValue<int>[] { task1, task2, task3, task4 };
                    handle.Done = true;
                }),
            context);
            // ReSharper restore AccessToModifiedClosure
            Assert.IsTrue(handle.WaitForFinished(TwoSeconds)); // Wait for main (one that creates tasks) work item to finish.

            int[] results = AsyncValue<int>.JoinAll(tasks).GetValue(TimeSpan.FromMilliseconds(8000));

            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(12, n, "Work items executed out of order");
            long ms = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(4000 <= ms && ms <= 5500, "Wait time out of range, expected between 4000 and 5500 milliseconds, was " + ms);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void Sched_Task_JoinAll()
        {
            ResultHandle resultHandle = new ResultHandle();
            int n = 0;
            Task<int>[] tasks = null;

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);

            // ReSharper disable AccessToModifiedClosure
            orleansTaskScheduler.QueueWorkItem(new ClosureWorkItem(() =>
            {
                Task<int> task1 = Task<int>.Factory.StartNew(() => { Console.WriteLine("===> 1a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 1b"); return 1; });
                Task<int> task2 = Task<int>.Factory.StartNew(() => { Console.WriteLine("===> 2a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 2b"); return 2; });
                Task<int> task3 = Task<int>.Factory.StartNew(() => { Console.WriteLine("===> 3a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 3b"); return 3; });
                Task<int> task4 = Task<int>.Factory.StartNew(() => { Console.WriteLine("===> 4a"); Thread.Sleep(OneSecond); n = n + 3; Console.WriteLine("===> 4b"); return 4; });
                tasks = new Task<int>[] {task1, task2, task3, task4};
                resultHandle.Done = true;
            }),context);
            // ReSharper restore AccessToModifiedClosure
            Assert.IsTrue(resultHandle.WaitForFinished(TwoSeconds)); // Wait for main (one that creates tasks) work item to finish.

            var promise = Task<int[]>.Factory.ContinueWhenAll(tasks, (res) => 
            {
                List<int> output = new List<int>();
                int taskNum = 1;
                foreach (var t in tasks)
                {
                    Assert.IsTrue(t.IsCompleted, "Sub-Task completed");
                    Assert.IsFalse(t.IsFaulted, "Sub-Task faulted: " + t.Exception);
                    var val = t.Result;
                    Assert.AreEqual(taskNum, val, "Value returned by Task " + taskNum);
                    output.Add(val);
                    taskNum++;
                }
                int[] results = output.ToArray();
                return results;
            });
            bool ok = promise.Wait(TimeSpan.FromSeconds(8));
            if (!ok) throw new TimeoutException();

            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(12, n, "Not all work items executed");
            long ms = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(4000 <= ms && ms <= 5000, "Wait time out of range, expected between 4000 and 5000 milliseconds, was " + ms);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_AC_ContinueWith_2_OrleansSched()
        {
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(new UnitTestSchedulingContext());

            ResultHandle result1 = new ResultHandle();
            ResultHandle result2 = new ResultHandle();
            bool failed1 = false;
            bool failed2 = false;

            AsyncCompletion task1 = AsyncCompletion.StartNew(() => { Console.WriteLine("===> 1a"); Thread.Sleep(OneSecond); throw new ArgumentException(); });
            AsyncCompletion task2 = task1.ContinueWith(() => { Console.WriteLine("===> 2"); },
                                                            (Exception exc) => { Console.WriteLine("===> 3"); failed1 = true; result1.Done = true; });
            AsyncCompletion task3 = task1.ContinueWith(() => { Console.WriteLine("===> 4"); },
                                                         (Exception exc) => { Console.WriteLine("===> 5"); failed2 = true; result2.Done = true; });
            task1.Ignore();
            task2.Ignore();
            task3.Ignore();
            Assert.IsTrue(result1.WaitForFinished(TwoSeconds), "First ContinueWith did not fire.");
            Assert.IsTrue(result2.WaitForFinished(TwoSeconds), "Second ContinueWith did not fire.");
            Assert.AreEqual(true, failed1, "First ContinueWith did not fire error handler.");
            Assert.AreEqual(true, failed2, "Second ContinueWith did not fire error handler.");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Scheduler")]
        public void Sched_Task_SchedulingContext()
        {
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            orleansTaskScheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            ActivationTaskScheduler scheduler = orleansTaskScheduler.GetWorkItemGroup(context).TaskRunner;

            ResultHandle resultHandle = new ResultHandle();
            Task endOfChain = null;
            int n = 0;

            Task wrapper = new Task(() =>
            {
                CheckRuntimeContext(context);

                // ReSharper disable AccessToModifiedClosure
                Task task1 = Task.Factory.StartNew(() =>
                {
                    Console.WriteLine("===> 1a ");
                    CheckRuntimeContext(context);
                    Thread.Sleep(1000); 
                    n = n + 3; 
                    Console.WriteLine("===> 1b");
                    CheckRuntimeContext(context);
                });
                Task task2 = task1.ContinueWith(task =>
                {
                    Console.WriteLine("===> 2");
                    CheckRuntimeContext(context);
                    n = n * 5; 
                });
                Task task3 = task2.ContinueWith(task => 
                {
                    Console.WriteLine("===> 3");
                    n = n / 5;
                    CheckRuntimeContext(context);
                });
                Task task4 = task3.ContinueWith(task => 
                {
                    Console.WriteLine("===> 4"); 
                    n = n - 2;
                    resultHandle.Done = true;
                    CheckRuntimeContext(context);
                });
                // ReSharper restore AccessToModifiedClosure
                endOfChain = task4.ContinueWith(task =>
                {
                    Console.WriteLine("Done Faulted={0}", task.IsFaulted);
                    CheckRuntimeContext(context);
                    Assert.IsFalse(task.IsFaulted, "Faulted with Exception=" + task.Exception);
                });
            });
            wrapper.Start(scheduler);
            bool ok = wrapper.Wait(TimeSpan.FromSeconds(1));
            if (!ok) throw new TimeoutException();

            Assert.IsFalse(wrapper.IsFaulted, "Wrapper Task Faulted with Exception=" + wrapper.Exception);
            Assert.IsTrue(wrapper.IsCompleted, "Wrapper Task completed");
            bool finished = resultHandle.WaitForFinished(TimeSpan.FromSeconds(2));
            Assert.IsNotNull(endOfChain, "End of chain Task created successfully");
            Assert.IsFalse(endOfChain.IsFaulted, "Task chain Faulted with Exception=" + endOfChain.Exception);
            Assert.IsTrue(finished, "Wrapper Task completed ok");
            Assert.IsTrue(n != 0, "Work items did not get executed");
            Assert.AreEqual(1, n, "Work items executed out of order");
        }

        private void Log(int level, string what)
        {
            Console.WriteLine("#{0} - {1} -- Thread={2} Worker={3} TaskScheduler.Current={4}",
                level, what,
                Thread.CurrentThread.ManagedThreadId,
                WorkerPoolThread.CurrentWorkerThread == null ? "Null" : WorkerPoolThread.CurrentWorkerThread.Name,
                TaskScheduler.Current);

        }

        private static void CheckRuntimeContext(ISchedulingContext context)
        {
            Assert.IsNotNull(RuntimeContext.Current, "Runtime context should not be null");
            Assert.IsNotNull(RuntimeContext.Current.ActivationContext, "Activation context should not be null");
            Assert.AreEqual(context, RuntimeContext.Current.ActivationContext, "Activation context");
        }

        private class Flags
        {
            public bool Flag1 { get; set; }
            public bool Flag2 { get; set; }
        }

        //-----------------------------------------------------------------------------------------------//

        private static SchedulerAdvancedTests StartTestInSeparateAppDomain(out AppDomain outDomain)
        {
            AppDomainSetup setup = new AppDomainSetup();
            setup.ApplicationBase = System.Environment.CurrentDirectory;
            outDomain = AppDomain.CreateDomain("SchedulerAdvancedTestsDomain", null, setup);
            string assemblyName = System.Reflection.Assembly.GetAssembly(typeof(SchedulerAdvancedTests)).FullName;
            string typeName = typeof(SchedulerAdvancedTests).FullName;
            SchedulerAdvancedTests test = (SchedulerAdvancedTests)outDomain.CreateInstanceFromAndUnwrap("UnitTests.dll", typeName);
            return test;
        }

        //private class MyClass
        //{
        //    private int Id;
        //    public MyClass(int id)
        //    {
        //        Console.WriteLine("MyClass()");
        //        Id = id;
        //    }
        //    ~MyClass()
        //    {
        //        Console.WriteLine("~MyClass() " + Id);
        //    }
        //}

        //private void Sched_BlockingCollection_Test()
        //{
        //    List<MyClass> list = new List<MyClass>();
        //    list.Add(new MyClass(1));
        //    list.RemoveAt(0);

        //    BlockingCollection<MyClass> queue = new BlockingCollection<MyClass>();
        //    queue.Add(new MyClass(2));
        //    CancellationTokenSource cts = new CancellationTokenSource(); ;
        //    MyClass todo;
        //    queue.TryTake(out todo, timeout, cts.Token);
        //    todo = null;

        //    GC.Collect();
        //    GC.WaitForPendingFinalizers();
        //    Thread.Sleep(2000);
        //    GC.Collect();
        //    GC.WaitForPendingFinalizers();
        //    GC.Collect();
        //    GC.WaitForPendingFinalizers();
        //}
    }
}
