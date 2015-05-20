using System;
using System.Linq;
using System.Threading;
using System.Diagnostics;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace UnitTests
{
    /// <summary>
    /// Summary description for PersistenceTest
    /// </summary>
    [TestClass]
    public class AC_AggregateWaitsTest
    {
        public AC_AggregateWaitsTest()
        {
            ThreadPool.SetMinThreads(1000, 1000);
            ThreadPool.SetMaxThreads(1000, 1000);
        }

        [TestInitialize]
        public void TestInitialize()
        {
            OrleansTask.Reset();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            UnitTestBase.CheckForUnobservedPromises();
            OrleansTask.Reset();
        }

        /// <summary>
        /// Gets or sets the test context which provides information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

#if false //TODO - put back when WaitAll is implemented with new scheduler.

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_AllCorrect()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.SetA(3);
            AsyncCompletion promise2 = localGrain.SetB(4);
            AsyncCompletion promise3 = localGrain.LongMethod(1000);
            AsyncCompletion[] acs = {promise1, promise2, promise3};

            AsyncCompletion.WaitAll(acs, 2000);

            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited " + stopwatch.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited " + stopwatch.ElapsedMilliseconds);

            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise2.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise3.Status == AsyncCompletionStatus.CompletedSuccessfully);
        }

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_TimeoutException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.SetA(3);
            AsyncCompletion promise2 = localGrain.LongMethod(2000);
            AsyncCompletion[] acs = { promise1, promise2 };
            Exception exc = null;
            try
            {
                AsyncCompletion.WaitAll(acs, 1000);
                Assert.Fail("Should have thrown.");
            }
            catch (TimeoutException exc1)
            {
                exc = exc1;
            }
            catch (Exception)
            {
                Assert.Fail("Should have TimeoutException.");
            }

            stopwatch.Stop();
            Assert.IsTrue(exc != null);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited " + stopwatch.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1500);
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise2.Status == AsyncCompletionStatus.Running);
        }

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_AppException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.SetA(3);
            AsyncCompletion promise2 = localGrain.LongMethod(1000);
            AsyncCompletion promise3 = localGrain.GetAxBError();

            AsyncCompletion[] acs = { promise1, promise2, promise3 };
            Exception exc = null;
            try
            {
                AsyncCompletion.WaitAll(acs, 2000);
                Assert.Fail("Should have thrown.");
            }
            catch (TimeoutException exc1)
            {
                exc = exc1;
                Assert.Fail("Should NOT have thrown TimeoutException.");
            }
            catch (Exception exc2)
            {
                exc = exc2;
            }

            stopwatch.Stop();
            long elapsed = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(exc != null);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited " + stopwatch.ElapsedMilliseconds + "which is longer than 1000ms. GetAxBError() throws immideately");
            Assert.IsTrue(promise3.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(promise3.Exception.GetBaseException().Message.Equals("GetAxBError-Exception"), promise3.Exception.GetBaseException().Message);
            Assert.IsTrue(promise3.Exception.GetBaseException().Equals(exc.GetBaseException()), exc.GetBaseException().Message);
        }

        //[TestMethod]
        public void AC_WaitAll_TPL()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            Task task1 = new Task(() => { Console.WriteLine("task1"); });
            Task task2 = new Task(() => { Console.WriteLine("task2"); throw new ArgumentNullException("task2"); });
            Task[] acs = { task1, task2 };
            task1.Start();
            task2.Start();
            bool thrownCorrectly = false;
            try
            {
                bool finished = Task.WaitAll(acs, 1000);
                Assert.IsTrue(!finished);
            }
            catch (TimeoutException)
            {
                Assert.Fail("Should NOT have thrown TimeoutException.");
            }
            catch (ArgumentNullException)
            {
                thrownCorrectly = true;
            }
            catch (Exception exc3)
            {
                if (exc3.GetBaseException() is ArgumentNullException)
                {
                    thrownCorrectly = true;
                }
                else
                {
                    Assert.Fail("Thrown WRONG Exception.");
                }
            }
            Assert.IsTrue(thrownCorrectly);
        }

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_Many()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            int timeout = 1000;

            Func<object, bool> function1 = (object i) => { Console.WriteLine("Started task " + i); Thread.Sleep(timeout); Console.WriteLine("*Finished task " + i); return true; };
            Func<object, bool> function2 = (object i) => { Console.WriteLine("Started task " + i); Console.WriteLine("*Finished task " + i); return true; };
            Func<object, bool> function3 = (object i) => { Console.WriteLine("---Started task " + i); throw new ArgumentNullException("Throwing task " + i);};
            AsyncValue<bool> promise1 = AsyncValue<bool>.StartNew(function1, 1);
            AsyncValue<bool> promise2 = AsyncValue<bool>.StartNew(function2, 2);
            AsyncValue<bool> promise3 = AsyncValue<bool>.StartNew(function2, 3);
            AsyncValue<bool> promise4 = AsyncValue<bool>.StartNew(function3, 4);
            AsyncValue<bool> promise5 = AsyncValue<bool>.StartNew(function2, 5);
            AsyncValue<bool> promise6 = AsyncValue<bool>.StartNew(function3, 6);
            AsyncCompletion[] acs = { promise1, promise2, promise3, promise4, promise5, promise6 };

            bool thrownCorrectly = false;
            try
            {
                AsyncCompletion.WaitAll(acs, 100000);
                Assert.Fail("Should have thrown.");
            }
            catch (TimeoutException)
            {
                Assert.Fail("Should NOT have thrown TimeoutException.");
            }
            catch (ArgumentNullException)
            {
                thrownCorrectly = true;
            }
            catch (Exception exc3)
            {
                Exception exc4 = exc3.GetBaseException();
                Exception exc5 = exc3.InnerException;
                if (exc5 is ArgumentNullException)
                {
                    thrownCorrectly = true;
                }
                else
                {
                    Assert.Fail("Thrown WRONG Exception." + exc3);
                }
            }
            Assert.IsTrue(thrownCorrectly);
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise2.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise3.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise4.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(promise5.Status == AsyncCompletionStatus.CompletedSuccessfully);
            Assert.IsTrue(promise6.Status == AsyncCompletionStatus.Faulted);
        }

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_CheckIgnore_MultipleAppExceptions()
        {
            AC_WaitAll_MultipleAppExceptions_CheckIgnore(false);
        }

        //[TestMethod]
        //TODO - put back when WaitAll is implemented with new scheduler.
        public void AC_WaitAll_CheckIgnore_TimeoutException()
        {
            AC_WaitAll_MultipleAppExceptions_CheckIgnore(true);
        }

        private void AC_WaitAll_MultipleAppExceptions_CheckIgnore(bool checkTimeout)
        {
            //Stopwatch stopwatch = new Stopwatch();
            //stopwatch.Start();
            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.LongMethodWithError(1000);
            AsyncCompletion promise2 = localGrain.LongMethodWithError(1500);
            AsyncCompletion promise3 = localGrain.LongMethodWithError(2000);
            AsyncCompletion promise4 = localGrain.LongMethodWithError(2500);
            AsyncCompletion promise5 = localGrain.LongMethodWithError(3000);

            AsyncCompletion[] acs = { promise1, promise2, promise3, promise4, promise5 };
            Exception exc = null;
            int timeout = (checkTimeout ? 200 : 5000);

            try
            {
                AsyncCompletion.WaitAll(acs, timeout);
                //int num = AsyncCompletion.WaitAny(acs, 5000);
                if (!checkTimeout)
                {
                    Assert.Fail("Should have thrown.");
                }
            }
            catch (TimeoutException exc1)
            {
                exc = exc1;
                if (!checkTimeout)
                {
                    Assert.Fail("Should NOT have thrown TimeoutException.");
                }
            }
            catch (Exception exc2)
            {
                exc = exc2;
                if (checkTimeout)
                {
                    Assert.Fail("Should have thrown TimeoutException.");
                }
            }

            localGrain.SetA(2).Wait();
            localGrain.SetB(8).Wait();
            Assert.AreEqual(16, localGrain.GetAxB().Result);
            Thread.Sleep(5000);

            //stopwatch.Stop();
            //long elapsed = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(exc != null);
            //Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited " + stopwatch.ElapsedMilliseconds + "which is longer than 1000ms. GetAxBError() throws immideately");
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.Faulted);
            Assert.IsTrue(promise1.Exception.OriginalException().Message.StartsWith("LongMethodWithError"), promise3.Exception.OriginalException().Message);

            Exception exc3 = promise1.Exception.GetBaseException();
            Exception exc4 = exc.GetBaseException();
            Exception exc5 = promise1.Exception.OriginalException();
            Exception exc6 = exc.OriginalException();

            Console.WriteLine("Done.");            
           // Assert.IsTrue(promise1.Exception.GetBaseException().Message.Equals(exc.GetBaseException().Message), exc.GetBaseException().Message);
        }

#endif

#if false //TODO - put back when WaitAny is implemented with new scheduler.
        //[TestMethod]
        private void AC_WaitAny_CheckIgnore_MultipleAppExceptions()
        {
            AC_WaitAny_MultipleAppExceptions_CheckIgnore(false);
        }

        //[TestMethod]
        private void AC_WaitAny_CheckIgnore_TimeoutException()
        {
            AC_WaitAny_MultipleAppExceptions_CheckIgnore(true);
        }

        private void AC_WaitAny_MultipleAppExceptions_CheckIgnore(bool checkTimeout)
        {
            LocalErrorGrain localGrain = new LocalErrorGrain();
            AsyncCompletion promise1 = localGrain.LongMethodWithError(1000);
            AsyncCompletion promise2 = localGrain.LongMethodWithError(1500);
            AsyncCompletion promise3 = localGrain.LongMethodWithError(2000);
            AsyncCompletion promise4 = localGrain.LongMethodWithError(2500);
            AsyncCompletion promise5 = localGrain.LongMethodWithError(3000);

            AsyncCompletion[] acs = { promise1, promise2, promise3, promise4, promise5 };
            int timeout = (checkTimeout ? 2000 : 4000);

            try
            {
                int num = AsyncCompletion.WaitAny(acs, timeout);
                if (!checkTimeout)
                {
                    Assert.IsTrue(num>=0);
                }
            }
            catch (TimeoutException)
            {
                if (!checkTimeout)
                {
                    Assert.Fail("Should NOT have thrown TimeoutException.");
                }
            }
            catch (Exception)
            {
                Assert.Fail("Should NOT have thrown any excdeption rather than TimeoutException.");
            }
            Thread.Sleep(2000);
            Assert.IsTrue(promise1.Status == AsyncCompletionStatus.Faulted);
            Console.WriteLine("Done.");
        }
#endif

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_JoinAllTest()
        {
            var promises = Enumerable.Range(1, 5).Select(i => Delay(i*1000)).ToArray();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            AsyncCompletion.JoinAll(promises).Wait();
            stopwatch.Stop();
            var ms = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(4800 <= ms && ms <= 5200, "Wait time out of range");
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_JoinAllTestException()
        {
            var promises = Enumerable.Range(1, 5).Select(i => Delay(i * 1000, i == 3 ? "fail" : null)).ToArray();
            var stopwatch = new Stopwatch();
            stopwatch.Start();
            bool failed = false;
            try
            {
                AsyncCompletion.JoinAll(promises).Wait();
            }
            catch (Exception)
            {
                failed = true;
            }
            stopwatch.Stop();
            var ms = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(failed, "Did not propagate exception");
            Assert.IsTrue(4800 <= ms && ms <= 5200, "Wait time out of range, expected between 4800 and 5200 milliseconds, was " + ms);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_OrleansTask_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task task1 = Task.Factory.StartNew(() => { pause1.WaitOne(); Console.WriteLine("Task-1"); }, cancel1);
            Task task2 = Task.Factory.StartNew(() => Assert.Fail("Should not have got inside Task-2"), cancel2);

            var ot1 = new OrleansTask(task1);
            var ot2 = new OrleansTask(task2);
            AsyncCompletion ac1 = new AsyncCompletion(ot1);
            AsyncCompletion ac2 = new AsyncCompletion(ot2);
            ot1.Ignore(null);
            ot2.Ignore(null);

            AsyncCompletion join = AsyncCompletion.JoinAll(new[] { ac1, ac2 });

            bool cwFired = false;
            AsyncCompletion cw = join.ContinueWith(() =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
            });

            try
            {
                pause1.Set();
                cw.Wait(TimeSpan.FromSeconds(5));
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(AsyncCompletionStatus.Faulted, join.Status, "Join Status");
            Assert.AreEqual(AsyncCompletionStatus.Faulted, cw.Status, "ContinueWith Status");
            Assert.IsFalse(cwFired, "ContinueWith block was not ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_OrleansTask_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            }, cancel1);
            Task<int> task2 = Task<int>.Factory.StartNew(() =>
            {
                Assert.Fail("Should not have got inside Task-2");
                return 2;
            }, cancel2);

            var ot1 = new OrleansTask<int>(task1);
            var ot2 = new OrleansTask<int>(task2);
            AsyncValue<int> av1 = new AsyncValue<int>(ot1);
            AsyncValue<int> av2 = new AsyncValue<int>(ot2);
            ot1.Ignore(null);
            ot2.Ignore(null);

            AsyncValue<int[]> join = AsyncValue<int>.JoinAll(new[] { av1, av2 });

            bool cwFired = false;
            AsyncCompletion cw = join.ContinueWith(vals =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
                Assert.Fail("ContinueWith should not have been called");
            });

            try
            {
                pause1.Set();
                cw.Wait(TimeSpan.FromSeconds(5));
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(AsyncCompletionStatus.Faulted, join.Status, "Join Status");
            Assert.AreEqual(AsyncCompletionStatus.Faulted, cw.Status, "ContinueWith Status");
            Assert.IsFalse(cwFired, "ContinueWith block was not ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromTask_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task task1 = Task.Factory.StartNew(() => { pause1.WaitOne(); Console.WriteLine("Task-1"); }, cancel1);
            Task task2 = Task.Factory.StartNew(() => Assert.Fail("Should not have got inside Task-2"), cancel2);

            AsyncCompletion ac1 = AsyncCompletion.FromTask(task1);
            AsyncCompletion ac2 = AsyncCompletion.FromTask(task2);

            AsyncCompletion join = AsyncCompletion.JoinAll(new[] { ac1, ac2 });

            bool cwFired = false;
            AsyncCompletion cw = join.ContinueWith(() =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
            });

            try
            {
                pause1.Set();
                cw.Wait(TimeSpan.FromSeconds(5));
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(AsyncCompletionStatus.Faulted, join.Status, "Join Status");
            Assert.AreEqual(AsyncCompletionStatus.Faulted, cw.Status, "ContinueWith Status");
            Assert.IsFalse(cwFired, "ContinueWith block was not ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_FromTask_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            }, cancel1);
            Task<int> task2 = Task<int>.Factory.StartNew(() =>
            {
                Assert.Fail("Should not have got inside Task-2");
                return 2;
            }, cancel2);

            AsyncValue<int> av1 = AsyncValue.FromTask(task1);
            AsyncValue<int> av2 = AsyncValue.FromTask(task2);

            AsyncValue<int[]> join = AsyncValue<int>.JoinAll(new[] { av1, av2 });

            bool cwFired = false;
            AsyncCompletion cw = join.ContinueWith(vals =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
                Assert.Fail("ContinueWith should not have been called");
            });

            try
            {
                pause1.Set();
                cw.Wait(TimeSpan.FromSeconds(5));
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(AsyncCompletionStatus.Faulted, join.Status, "Join Status");
            Assert.AreEqual(AsyncCompletionStatus.Faulted, cw.Status, "ContinueWith Status");
            Assert.IsFalse(cwFired, "ContinueWith block was not ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task Task_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task task1 = Task.Factory.StartNew(() => { pause1.WaitOne(); Console.WriteLine("Task-1"); }, cancel1);
            Task task2 = Task.Factory.StartNew(() => Assert.Fail("Should not have got inside Task-2"), cancel2);

            Task join = Task.WhenAll(task1, task2);

            bool cwFired = false;
            Task cw = join.ContinueWith(t =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
                if (t.IsCanceled) return;
                if (t.IsFaulted) throw t.Exception;
                Assert.Fail("ContinueWith should not have been called successfully");
            });

            try
            {
                pause1.Set();
                await cw;
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(TaskStatus.Canceled, join.Status, "Join Status");
            Assert.AreEqual(TaskStatus.RanToCompletion, cw.Status, "ContinueWith Status");
            Assert.IsTrue(cwFired, "ContinueWith block was ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task TaskT_JoinAll_ContinueWith_Cancel()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            var cancel1 = new CancellationToken(false); // Not cancelled
            var cancel2 = new CancellationToken(true); // Cancelled
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            }, cancel1);
            Task<int> task2 = Task<int>.Factory.StartNew(() =>
            {
                Assert.Fail("Should not have got inside Task-2");
                return 2;
            }, cancel2);

            Task<int[]> join = Task.WhenAll(task1, task2);

            bool cwFired = false;
            Task cw = join.ContinueWith(t =>
            {
                cwFired = true;
                Console.WriteLine("After Join");
                Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
                Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
                Console.WriteLine("Join State = {0}", join.Status);
                if (t.IsCanceled) return;
                if (t.IsFaulted) throw t.Exception;
                Assert.Fail("ContinueWith should not have been called successfully");
            });

            try
            {
                pause1.Set();
                await cw;
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Console.WriteLine(baseExc);
                if (baseExc is TaskCanceledException)
                {
                    // OK
                }
                else
                {
                    throw;
                }
            }

            Console.WriteLine("After Wait");
            Console.WriteLine("Task {0} State = {1}", 1, task1.Status);
            Console.WriteLine("Task {0} State = {1}", 2, task2.Status);
            Console.WriteLine("Join State = {0}", join.Status);
            Console.WriteLine("CW State = {0}", cw.Status);
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            Assert.AreEqual(TaskStatus.Canceled, task2.Status, "Task-2 Status");

            Assert.AreEqual(TaskStatus.Canceled, join.Status, "Join Status");
            Assert.AreEqual(TaskStatus.RanToCompletion, cw.Status, "ContinueWith Status");
            Assert.IsTrue(cwFired, "ContinueWith block was ran");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task Task_WhenAny()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            ManualResetEvent pause2 = new ManualResetEvent(false);
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            });
            Task<int> task2 = Task<int>.Factory.StartNew(() =>
            {
                pause2.WaitOne();
                Console.WriteLine("Task-2");
                return 2;
            });

            Task join = Task.WhenAny(task1, task2, Task.Delay(TimeSpan.FromSeconds(1)));
            pause1.Set();
            await join;
            Assert.AreEqual(TaskStatus.RanToCompletion, join.Status, "Join Status");
            Assert.AreEqual(TaskStatus.RanToCompletion, task1.Status, "Task-1 Status");
            TaskStatus task2Status = task2.Status;
            Assert.IsTrue(task2Status == TaskStatus.Running || task2Status == TaskStatus.WaitingToRun, "Task-2 Status = " + task2Status);
            pause2.Set();
            task2.Ignore();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task Task_WhenAny_Timeout()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            ManualResetEvent pause2 = new ManualResetEvent(false);
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            });
            Task<int> task2 = Task<int>.Factory.StartNew(() =>
            {
                pause2.WaitOne();
                Console.WriteLine("Task-2");
                return 2;
            });

            Task join = Task.WhenAny(task1, task2, Task.Delay(TimeSpan.FromSeconds(1)));
            await join;
            Assert.AreEqual(TaskStatus.RanToCompletion, join.Status, "Join Status");
            TaskStatus task1Status = task1.Status;
            Assert.IsTrue(task1Status == TaskStatus.Running || task1Status == TaskStatus.WaitingToRun, "Task-1 Status = " + task1Status);
            TaskStatus task2Status = task2.Status;
            Assert.IsTrue(task2Status == TaskStatus.Running || task2Status == TaskStatus.WaitingToRun, "Task-2 Status = " + task2Status);
            pause1.Set();
            task1.Ignore();
            pause2.Set();
            task2.Ignore();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task Task_Timeout()
        {
            ManualResetEvent pause1 = new ManualResetEvent(false);
            Task<int> task1 = Task<int>.Factory.StartNew(() =>
            {
                pause1.WaitOne();
                Console.WriteLine("Task-1");
                return 1;
            });

            Task join = Task.WhenAny(task1, Task.Delay(TimeSpan.FromSeconds(1)));
            await join;
            Assert.AreEqual(TaskStatus.RanToCompletion, join.Status, "Join Status");
            Assert.AreEqual(TaskStatus.Running, task1.Status, "Task-1 Status");
            pause1.Set();
            task1.Ignore();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void JoinAllForZeroTasks()
        {
            AsyncCompletion[] promises = new AsyncCompletion[0];
            var result = AsyncCompletion.JoinAll(promises);
            result.Wait();
            Assert.AreEqual(AsyncCompletionStatus.CompletedSuccessfully, result.Status);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_JoinAll_AV_Test()
        {
            var promises = Enumerable.Range(1, 5).Select(i => Delay2(i * 1000)).ToArray();
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            int[] results = AsyncValue<int>.JoinAll(promises).GetValue();
            stopwatch.Stop();
            long ms = stopwatch.ElapsedMilliseconds;
            Assert.IsTrue(4800 <= ms && ms <= 5200, "Wait time out of range, expected between 4800 and 5200 milliseconds, was " + ms);
            int counter = 1;
            foreach (int result in results)
            {
                Assert.IsTrue(result == counter);
                counter++;
            }
        }

        private static AsyncCompletion Delay(int ms, string exception = null)
        {
            return AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(ms);
                if (exception != null)
                {
                    throw new Exception(exception);
                }
            });
        }

        private static AsyncValue<int> Delay2(int ms, string exception = null)
        {
            return AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(ms);
                if (exception != null)
                {
                    throw new Exception(exception);
                }
                return ms / 1000;
            });
        }

        public void ExceptionTest()
        {
            TaskFactory tf = new TaskFactory();
            Task[] tasks = new Task[] 
            {
                tf.StartNew(()=> 
                {
                    Assert.Fail();
                }),
                tf.StartNew(()=> 
                {
                    Assert.Fail();
                }),
                tf.StartNew( () =>
                    { 
                        while(true)
                        {
                            Console.Write(".");
                            Thread.Sleep(1000);
                        }
                    }
                )
            };

            // Never finishes
            //Task.WaitAll(tasks);
            WaitUntilAllCompletedOrOneFailed(tasks);
            Console.Write("*");
            GC.Collect();
        }

        private static void WaitUntilAllCompletedOrOneFailed(Task[] tasks)
        {
            var list = new List<Task>(tasks);
            while (list.Count > 0)
            {
                int i = Task.WaitAny(tasks.ToArray());
                if (list[i].IsFaulted) throw list[i].Exception.InnerException;
                else list.RemoveAt(i);
            }
        }
    }
}
