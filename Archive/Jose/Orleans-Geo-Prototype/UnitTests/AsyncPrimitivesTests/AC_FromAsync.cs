using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Scheduler;
using UnitTests.SchedulerTests;

namespace UnitTests.AsyncPrimitivesTests
{
    [TestClass]
    public class AC_FromAsync
    {
        internal class SimpleAsyncResult : IAsyncResult
        {
            public SimpleAsyncResult(object state, bool completed)
            {
                AsyncState = state;
                waitEvent = new ManualResetEvent(completed);
                IsCompleted = completed;
                CompletedSynchronously = completed;
            }

            public object AsyncState { get; private set; }

            public AsyncCallback Callback { get; private set; }

            private readonly ManualResetEvent waitEvent;

            public WaitHandle AsyncWaitHandle { get { return waitEvent; } }

            public bool CompletedSynchronously { get; private set; }

            public bool IsCompleted { get; private set; }

            public int Result { get; set; }

            public Exception Error { get; set; }

            public void Complete(int value)
            {
                Result = value;
                IsCompleted = true;
                waitEvent.Set();
                if (Callback != null)
                {
                    ThreadPool.QueueUserWorkItem((object o) => Callback((IAsyncResult)o), this);
                }
            }

            public void Fail(Exception ex)
            {
                Error = ex;
                IsCompleted = true;
                waitEvent.Set();
                if (Callback != null)
                {
                    ThreadPool.QueueUserWorkItem((object o) => Callback((IAsyncResult)o), this);
                }
            }

            static public IAsyncResult BeginOperation(object state, AsyncCallback callback = null)
            {
                return new SimpleAsyncResult(state, false) { Callback = callback };
            }

            static public IAsyncResult BeginSynchronousOperation(object state, int res)
            {
                return new SimpleAsyncResult(state, true) { Result = res };
            }

            static public int EndOperation(IAsyncResult asyncResult)
            {
                var sar = asyncResult as SimpleAsyncResult;
                if (sar == null)
                {
                    throw new ArgumentException("Argument must be a SimpleAsyncResult", "asyncResult");
                }
                if (!sar.IsCompleted)
                {
                    sar.AsyncWaitHandle.WaitOne();
                }
                if (sar.Error != null)
                {
                    throw sar.Error;
                }
                return sar.Result;
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncResult()
        {
            int val = -23;
            var result = SimpleAsyncResult.BeginOperation(null);
            var promise = AsyncCompletion.FromAsync(result, res => { val = SimpleAsyncResult.EndOperation(res); });
            Assert.AreEqual(-23, val, "Value was overwritten prematurely");
            Assert.AreEqual(AsyncCompletionStatus.Running, promise.Status, "Promise status is incorrect");
            ((SimpleAsyncResult)result).Complete(13);
            Assert.IsTrue(promise.TryWait(TimeSpan.FromMilliseconds(1000)), "Promise from async did not resolve in 1 second");
            Assert.AreEqual(13, val, "Value was not overwritten");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncResultFailure()
        {
            int val = -23;
            var result = SimpleAsyncResult.BeginOperation(null);
            var promise = AsyncCompletion.FromAsync(result, res => { val = SimpleAsyncResult.EndOperation(res); });
            Assert.AreEqual(-23, val, "Value was overwritten prematurely");
            Assert.AreEqual(AsyncCompletionStatus.Running, promise.Status, "Promise status is incorrect");
            ((SimpleAsyncResult)result).Fail(new Exception("Test exception"));
            try
            {
                Assert.IsTrue(promise.TryWait(TimeSpan.FromMilliseconds(1000)), "Promise from async did not resolve in 1 second");
            }
            catch (AggregateException)
            {
                Assert.AreEqual(AsyncCompletionStatus.Faulted, promise.Status, "Promise status is incorrect");
            }
            catch (Exception ex2)
            {
                Assert.Fail("Incorrect exception thrown: {0}", ex2);
            }
            Assert.AreEqual(-23, val, "Value was overwritten incorrectly");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncResultSynchronously()
        {
            int val = -23;
            var result = SimpleAsyncResult.BeginSynchronousOperation(null, 13);
            var promise = AsyncCompletion.FromAsync(result, res => { val = SimpleAsyncResult.EndOperation(res); });
            // TPL may guarantee that End is called inline (Synchronously) if begin has returned a resolved IAsyncResult
            // But Orleans does not want to guarantee that.
            //Assert.AreEqual(AsyncCompletionStatus.CompletedSuccessfully, promise.Status, "Promise status is incorrect");
            Assert.IsTrue(promise.TryWait(TimeSpan.FromMilliseconds(1000)), "Promise from async did not resolve in 1 second");
            Assert.AreEqual(13, val, "Value was not overwritten");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncMethods()
        {
            int val = -23;
            IAsyncResult result = null;
            var promise = AsyncCompletion.FromAsync((cb, o) =>
                                                        {
                                                            result = SimpleAsyncResult.BeginOperation(o, cb);
                                                            return result;
                                                        },
                res => { val = SimpleAsyncResult.EndOperation(res); }, null);
            Assert.AreEqual(-23, val, "Value was overwritten prematurely");
            Assert.AreEqual(AsyncCompletionStatus.Running, promise.Status, "Promise status is incorrect");
            Assert.IsNotNull(result, "Begin delegate was not called");
            ((SimpleAsyncResult)result).Complete(13);
            Assert.IsTrue(promise.TryWait(TimeSpan.FromMilliseconds(1000)), "Promise from async did not resolve in 1 second");
            Assert.AreEqual(13, val, "Value was not overwritten");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncMethodsWithArg()
        {
            int val = -23;
            IAsyncResult result = null;
            var promise = AsyncCompletion.FromAsync((n, cb, o) =>
            {
                result = SimpleAsyncResult.BeginOperation(o, cb);
                return result;
            },
                res => { val = SimpleAsyncResult.EndOperation(res); }, 3, null);
            Assert.AreEqual(-23, val, "Value was overwritten prematurely");
            Assert.AreEqual(AsyncCompletionStatus.Running, promise.Status, "Promise status is incorrect");
            Assert.IsNotNull(result, "Begin delegate was not called");
            ((SimpleAsyncResult)result).Complete(13);
            Assert.IsTrue(promise.TryWait(TimeSpan.FromMilliseconds(1000)), "Promise from async did not resolve in 1 second");
            Assert.AreEqual(13, val, "Value was not overwritten");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_FromAsyncResultScheduling()
        {
            UnitTestSchedulingContext context = new UnitTestSchedulingContext();
            var scheduler = OrleansTaskScheduler.InitializeSchedulerForTesting(context);
            int val = -23;
            var resolver = new AsyncValueResolver<IAsyncResult>();
            var resolver1 = new AsyncValueResolver<AsyncCompletion>();
            var task1 = new ClosureWorkItem(() =>
                                                {
                                                    var result = SimpleAsyncResult.BeginOperation(null);
                                                    resolver.Resolve(result);
                                                    resolver1.Resolve(AsyncCompletion.FromAsync(result, res =>
                                                                                                            {
                                                                                                                val =
                                                                                                                    SimpleAsyncResult
                                                                                                                        .
                                                                                                                        EndOperation
                                                                                                                        (res);
                                                                                                            }));
                                                });
            scheduler.QueueWorkItem(task1, context);
            Assert.AreEqual(-23, val, "Value was overwritten prematurely");
            var resolver2 = new AsyncCompletionResolver();
            var task2 = new ClosureWorkItem(() =>
                                                {
                                                    val = 0;
                                                    resolver2.Resolve();
                                                });
            scheduler.QueueWorkItem(task2, context);
            Assert.IsTrue(resolver.AsyncCompletion.TryWait(TimeSpan.FromMilliseconds(100)), "Initial closure didn't run in 100ms");
            Assert.IsTrue(resolver2.AsyncCompletion.TryWait(TimeSpan.FromMilliseconds(100)), "Second closure didn't run in 100ms");
            ((SimpleAsyncResult)resolver.AsyncValue.GetValue()).Complete(13);
            Assert.IsTrue(resolver1.AsyncValue.GetValue().TryWait(TimeSpan.FromMilliseconds(100)), "FromAsync promise didn't resolve in 100ms");
            Assert.AreEqual(13, val, "Value was not overwritten");
        }
    }
}
