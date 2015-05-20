using System;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using System.Collections.Generic;

// ReSharper disable ConvertToLambdaExpression

namespace UnitTests
{
    /// <summary>
    /// Summary description for UnitTest1
    /// </summary>
    [TestClass]
    public class AC_AsyncValueTests
    {
        const bool shouldTrackObservations = true;
        private bool prevTrackObservations;

        private readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);
        private readonly TimeSpan TwoSecond = TimeSpan.FromSeconds(2);

        [TestInitialize]
        public void MyTestInitialize()
        {
            List<string> unobservedPromises = AsyncCompletion.GetUnobservedPromises();

            prevTrackObservations = AsyncCompletion.TrackObservations;
            AsyncCompletion.TrackObservations = shouldTrackObservations;

            Console.WriteLine("Set AsyncCompletion.TrackObservations to {0}. There were {1} unobserved promise(s)", shouldTrackObservations, unobservedPromises.Count);
            OrleansTask.Reset();

            CheckUnobservedPromises(unobservedPromises);
        }

        [TestCleanup]
        public void MyTestCleanup()
        {
            List<string> unobservedPromises = AsyncCompletion.GetUnobservedPromises();

            AsyncCompletion.TrackObservations = prevTrackObservations;
            Console.WriteLine("Reset AsyncCompletion.TrackObservations to {0}. There were {1} unobserved promise(s)", prevTrackObservations, unobservedPromises.Count);
            OrleansTask.Reset();

            CheckUnobservedPromises(unobservedPromises);
        }

        internal static void CheckUnobservedPromises(List<string> unobservedPromises)
        {
            if (unobservedPromises.Count > 0)
            {
                for (int i = 0; i < unobservedPromises.Count; i++)
                {
                    string promise = unobservedPromises[i];
                    Console.WriteLine("Unobserved promise {0}: {1}", i, promise);
                }
                Assert.Fail("Found {0} unobserved promise(s) : [ {1} ]",
                                          unobservedPromises.Count,
                                          string.Join(" , \n", unobservedPromises));
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncCompletionWait()
        {
            //OrleansTask.InitializeScheduler(new object());
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });

            promise.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncCompletionStartNew()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                return AsyncCompletion.StartNew(() =>
                {
                    Thread.Sleep(2000);
                });
            });
            promise1.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);

            stopwatch.Restart();
            AsyncCompletion orphan = null;
            AsyncCompletion promise2 = AsyncCompletion.StartNew(() =>
            {
                orphan = AsyncCompletion.StartNew(() =>
                {
                    Thread.Sleep(2000);
                });
            });
            promise2.Wait();
            stopwatch.Stop();
            orphan.Wait();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 200, "Waited longer than 200ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncValueStartNew()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncValue<int> promise1 = AsyncValue<int>.StartNew(() =>
            {
                return AsyncValue<int>.StartNew(() =>
                {
                    Thread.Sleep(2000);
                    return 2;
                });
            });
            promise1.Wait();
            stopwatch.Stop();
            Assert.AreEqual(2, promise1.GetValue());
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);

            stopwatch.Restart();
            AsyncCompletion orphan = null;
            AsyncValue<int> promise2 = AsyncValue<int>.StartNew(() =>
            {
                orphan = AsyncValue<int>.StartNew(() =>
                {
                    Thread.Sleep(2000);
                    return 2;
                });
                return 3;
            });
            promise2.Wait();
            stopwatch.Stop();
            orphan.Wait();
            Assert.AreEqual(3, promise2.GetValue());
            Assert.IsTrue(stopwatch.ElapsedMilliseconds < 200, "Waited longer than 200ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncCompletion_WaitForDone()
        {
            AsyncCompletion promise = AsyncCompletion.Done;

            promise.Wait(OneSecond);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncCompletion_DoubleWait()
        {
            AsyncCompletion promise = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(OneSecond);
            });

            promise.Wait(TwoSecond);

            promise.Wait(OneSecond);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncValueWait()
        {
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
                {
                    Stopwatch stopwatch = new Stopwatch();
                    stopwatch.Start();
                    Thread.Sleep(1000);
                    stopwatch.Stop();
                    return stopwatch.ElapsedMilliseconds;
                });

            promise.Wait();
            Assert.IsTrue(promise.GetValue() >= 800); // check that we waited at least 0.9 second
            Assert.IsTrue(promise.GetValue() <= 1200);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ErrorBroken()
        {
            AsyncCompletion promise = null;
            try
            {
                promise = AsyncCompletion.StartNew(() =>
                {
                    throw new Exception("Just a test!");
                });
            }
            catch (Exception)
            {
                Assert.Fail("Should not have thrown 1");
            }
            try
            {
                promise.Wait();
                Assert.Fail("Should have thrown 2");
            }
            catch (Exception)
            {
                Console.WriteLine("Thrown correctly 3");
            }
        }

        private AsyncCompletion justThrows()
        {
            throw new Exception("Just a test!");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ErrorException()
        {
            AsyncCompletion promise = null;
            try
            {
                promise = justThrows();
                Assert.Fail("Should have thrown 4");
            }
            catch (Exception)
            {
                Console.WriteLine("Thrown correctly 5");
            }
            try
            {
                promise.Wait();
                Assert.Fail("Should have thrown 6");
            }
            catch (Exception)
            {
                Console.WriteLine("Thrown correctly 7");
            }
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWith()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });

            AsyncCompletion promise2 = promise1.ContinueWith(() =>
            {
                Thread.Sleep(1000);
            });

            promise2.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
                throw new Exception("Test exception");
            });

            AsyncCompletion promise2 = promise1.ContinueWith(
                () =>
                    {
                        // nothing
                    },
                ex =>
                    {
                        Thread.Sleep(1000);                        
                    });

            try
            {
                promise2.Wait();                
            }
            catch (Exception e)
            {
                Assert.Fail("Should not have thrown exception: " + e);
            }
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithValue()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });

            AsyncValue<int> promise2 = promise1.ContinueWith(() =>
            {
                Thread.Sleep(1000);
                return 1000;
            });

            var result = promise2.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(result == 1000, "Result is not 1000");
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms");
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithAsyncValue()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });

            AsyncValue<int> promise2 = promise1.ContinueWith(() =>
            {
                Thread.Sleep(1000);
                return AsyncValue<int>.StartNew(() => 1000).GetValue();
            },
            ex => {
                return 2000;
            });

            var result = promise2.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(result == 1000, "Result is not 1000");
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ContinueWithValueException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                throw new Exception("Test exception");
            });

            AsyncValue<int> promise2 = promise1.ContinueWith(() =>
            {
                Thread.Sleep(1000);
                return 1000;
            },
            ex => {
                Thread.Sleep(2000);
                return 2000;
            });

            var result = promise2.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(result == 2000, "Result is not 2000. It is " + result);
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms. Waited " + stopwatch.ElapsedMilliseconds); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms. Waited " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncValueContinueWith()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncValue<int> promise1 = AsyncValue<int>.StartNew(() =>
            {
                return  AsyncValue<int>.StartNew(() =>
                {
                    Thread.Sleep(1000);
                    return 1000;
                });
            });

            AsyncValue<long> promise2 = promise1.ContinueWith(promise =>
            {
                Thread.Sleep(promise);
                stopwatch.Stop();
                return stopwatch.ElapsedMilliseconds;
            });

            promise2.Wait();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited only " + stopwatch.ElapsedMilliseconds
                + " milliseconds, should have been at least 1800"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited " + stopwatch.ElapsedMilliseconds
                + " milliseconds, should have been no more than 2200");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncValueContinueWithException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncValue<int> promise1 = AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000);
                if (stopwatch == null /*never*/)
                    return 1000;
                throw new Exception("Test exception");
            });

            AsyncValue<long> promise2 = promise1.ContinueWith(
                promise =>
                    {
                        Thread.Sleep(3000);
                        return 3000L;
                    },
                ex =>
                    {
                        Thread.Sleep(1000);
                        return 2000L;
                    });

            long result = promise2.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Elapsed " + stopwatch.ElapsedMilliseconds); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2400, "Elapsed " + stopwatch.ElapsedMilliseconds);
            Assert.IsTrue(result == 2000L);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_Resolver()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletionResolver resolver = new AsyncCompletionResolver();

            AsyncCompletion.StartNew(() =>
            {
                return AsyncCompletion.StartNew(() =>
                {
                    Thread.Sleep(1000);
                    resolver.Resolve();
                });
            }).Ignore();

            resolver.AsyncCompletion.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Elapsed=" + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Elapsed=" + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncValueResolver()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncValueResolver<int> resolver = new AsyncValueResolver<int>();

            AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
                resolver.Resolve(10);
            }).Ignore();

            resolver.AsyncValue.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200);
            Assert.AreEqual(10, resolver.AsyncValue.GetValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsTask_Wait()
        {
            AsyncValue<long> promise = AsyncValue<long>.StartNew(() =>
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();
                Thread.Sleep(1000);
                stopwatch.Stop();
                return stopwatch.ElapsedMilliseconds;
            });

            Task<long> t = promise.AsTask();
            t.Wait();
            long result = t.Result;
            Assert.IsTrue(result >= 800); // check that we waited at least 0.9 second
            Assert.IsTrue(result <= 1200);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsTask_ErrorBroken()
        {
            AsyncCompletion promise = null;
            try
            {
                promise = AsyncCompletion.StartNew(() =>
                {
                    throw new Exception("Just a test!");
                });
            }
            catch (Exception)
            {
                Assert.Fail("Should not have thrown 1");
            }
            bool didThrow = false;
            try
            {
                Task t = promise.AsTask();
                t.Wait();
            }
            catch (Exception exc)
            {
                didThrow = true;
                Console.WriteLine("Thrown correctly 3 - " + exc);
            }
            if (!didThrow)
            {
                Assert.Fail("Should have thrown 2");
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsTask_ContinueWith()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise1 = AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000);
            });

            Task t = promise1.AsTask();
            Task t2 = t.ContinueWith(task =>
            {
                Thread.Sleep(1000);
            });

            t2.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Waited less than 1800ms"); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2200, "Waited longer than 2200ms");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public async Task AC_AsTask_ContinueWithException()
        {
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncValue<int> promise1 = AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000);
                if (stopwatch == null /*never*/)
                    return 1000;
                throw new Exception("Test exception");
            });

            Task t = promise1.AsTask();
            Task<long> t2 = t.ContinueWith(task =>
            {
                if (task.IsFaulted)
                {
                    // Must observe a Task's exception(s) either by Waiting on the Task or accessing its Exception property.
                    Console.WriteLine("Observed exception from Task {0} = {1}", task.Id, task.Exception);

                    Thread.Sleep(1000);
                    return 2000L;
                }
                else
                {
                    Thread.Sleep(2000);
                    return 3000L;
                }
            });

            await t2;
            long result = t2.Result;

            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 1800, "Elapsed " + stopwatch.ElapsedMilliseconds); // check that we waited at least 1.9 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 2400, "Elapsed " + stopwatch.ElapsedMilliseconds);
            Assert.AreEqual(2000L, result, "Result returned");

            Assert.IsTrue(t2.IsCompleted, "Task 2 completed");
            Assert.IsFalse(t2.IsFaulted, "Task 2 should not be faulted");
            Assert.IsTrue(t.IsCompleted, "Task 1 completed");
            Assert.IsTrue(t.IsFaulted, "Task 1 should be faulted");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_AsTask_ErrorBroken()
        {
            AsyncValue<string> promise = null;
            bool shouldThrow = true;
            bool didThrow = false;
            try
            {
                promise = AsyncValue<string>.StartNew(() =>
                {
                    if (shouldThrow) throw new Exception("Just a test!");
                    return "Hello";
                });
            }
            catch (Exception)
            {
                Assert.Fail("Should not have thrown 1");
            }
            try
            {
                Task<string> t = promise.AsTask();
                t.Wait();
            }
            catch (Exception exc)
            {
                didThrow = true;
                Console.WriteLine("Thrown correctly 3 - " + exc);
            }
            if (!didThrow)
            {
                Assert.Fail("Should have thrown 2");
            }
        }

        ////[TestMethod]
        //public void AC_Finalization()
        //{
        //    bool doThrow = true;
        //    AsyncValue<int> a = AsyncValue<int>.StartNew(() =>
        //    {
        //        AsyncValue<int> b = AsyncValue<int>.StartNew(() =>
        //        {
        //            if (!doThrow)
        //                return 0;
        //            //Thread.Sleep(1000);
        //            throw new Exception("Just a test!");
        //        });
        //        AsyncValue<int> c = b.ContinueWith((int x) => { Console.WriteLine("ContinueWith executed"); return 55; });
        //        Thread.Sleep(1000);
        //        return 6;// c.GetValue();
        //    });
        //    a.Wait();
        //    Thread.Sleep(1000);
        //}


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncExecutorWithRetriesTest_1()
        {
            int counter = 0;
            Func<int, Task<int>> myFunc = ((int funcCounter) =>
            {
// ReSharper disable AccessToModifiedClosure
                Assert.AreEqual(counter, funcCounter);
                Console.WriteLine("Running for {0} time.", counter);
                counter++;
                if (counter == 5)
                    return Task.FromResult(28);
                else
                    throw new ArgumentException("Wrong arg!");
// ReSharper restore AccessToModifiedClosure
            });
            Func<Exception, int, bool> errorFilter = ((Exception exc, int i) =>
            {
                return true;
            });

            Task<int> promise = AsyncExecutorWithRetries.ExecuteWithRetries(myFunc, 10, 10, null, errorFilter);
            int value = promise.Result;
            Console.WriteLine("Value is {0}.", value);
            counter = 0;
            try
            {
                promise = AsyncExecutorWithRetries.ExecuteWithRetries(myFunc, 3, 3, null, errorFilter);
                value = promise.Result;
                Console.WriteLine("Value is {0}.", value);
            }
            catch (Exception)
            {
                return;
            }
            Assert.Fail("Should have thrown");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncExecutorWithRetriesTest_2()
        {
            int counter = 0;
            const int countLimit = 5;
            Func<int, Task<int>> myFunc = ((int funcCounter) =>
            {
// ReSharper disable AccessToModifiedClosure
                Assert.AreEqual(counter, funcCounter);
                Console.WriteLine("Running for {0} time.", counter);
                return Task.FromResult(++counter);
// ReSharper restore AccessToModifiedClosure
            });
            Func<int, int, bool> successFilter = ((int count, int i) => count != countLimit);

            int maxRetries = 10;
            int expectedRetries = countLimit;
            Task<int> promise = AsyncExecutorWithRetries.ExecuteWithRetries(myFunc, maxRetries, maxRetries, successFilter, null, Constants.INFINITE_TIMESPAN);
            int value = promise.Result;
            Console.WriteLine("Value={0} Counter={1} ExpectedRetries={2}", value, counter, expectedRetries);
            Assert.AreEqual(expectedRetries, value, "Returned value");
            Assert.AreEqual(counter, value, "Counter == Returned value");

            counter = 0;
            maxRetries = 3;
            expectedRetries = maxRetries;
            promise = AsyncExecutorWithRetries.ExecuteWithRetries(myFunc, maxRetries, maxRetries, successFilter, null);
            value = promise.Result;
            Console.WriteLine("Value={0} Counter={1} ExpectedRetries={2}", value, counter, expectedRetries);
            Assert.AreEqual(expectedRetries, value, "Returned value");
            Assert.AreEqual(counter, value, "Counter == Returned value");
        }

        [TestMethod]
        public void AC_AsyncExecutorWithRetriesTest_3()
        {
            TimeSpan timeout = TimeSpan.FromMilliseconds(1000);
            int counter = 0;
            Func<int, Task<int>> myFunc = ((int funcCounter) =>
            {
                Assert.AreEqual(counter, funcCounter);
                Console.WriteLine("Running for {0} time.", counter);
                return Task.FromResult(counter++);
            });
            Func<int, int, bool> successFilter = ((int count, int i) =>
            {
                return true;
            });

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            bool timeoutHappened = false;
            Task<int> promise = AsyncExecutorWithRetries.ExecuteWithRetries(myFunc, -1, -1, successFilter, null, timeout);
                //new ExponentialBackoff(TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(1000), TimeSpan.FromMilliseconds(100)));
            try
            {
                int value = promise.Result;
                Assert.Fail("Should have thrown " + value);
            }
            catch (Exception exc)
            {
                stopwatch.Stop();
                Exception baseExc = exc.GetBaseException();

                if (baseExc is TimeoutException)
                    timeoutHappened = true;
                else
                    Console.WriteLine(exc);
            }
            Assert.IsTrue(timeoutHappened);
            Assert.IsTrue(stopwatch.Elapsed >= timeout.Multiply(0.9) && stopwatch.Elapsed <= timeout.Multiply(2), "Elapsed = " + stopwatch.Elapsed + " timeout = " + timeout);
        }

        public void AC_AsyncExecutor_BackoffTest_1()
        {
            TimeSpan EXP_ERROR_BACKOFF_MIN = TimeSpan.FromSeconds(5);
            TimeSpan EXP_ERROR_BACKOFF_MAX = EXP_ERROR_BACKOFF_MIN.Multiply(2);
            TimeSpan EXP_CONTENTION_BACKOFF_MIN = TimeSpan.FromMilliseconds(100);
            TimeSpan EXP_CONTENTION_BACKOFF_MAX = TimeSpan.FromMilliseconds(5000);
            TimeSpan EXP_BACKOFF_STEP = TimeSpan.FromMilliseconds(500);

            AsyncExecutorWithRetries.ExecuteWithRetries(
                (int counter) =>
                {
                    Console.WriteLine("#{0}, time {1}.", counter, Logger.PrintDate(DateTime.UtcNow));
                    throw new Exception();
                    //return false;
                },
                10, 
                10, // 5 retries
                (bool result, int i) => { return result == false; },   // if failed to Update on contention - retry   
                (Exception exc, int i) => { return true; },            // Retry on errors. 
                Constants.INFINITE_TIMESPAN,
                new ExponentialBackoff(EXP_CONTENTION_BACKOFF_MIN, EXP_CONTENTION_BACKOFF_MAX, EXP_BACKOFF_STEP), // how long to wait between sucessfull retries
                new ExponentialBackoff(EXP_ERROR_BACKOFF_MIN, EXP_ERROR_BACKOFF_MAX, EXP_BACKOFF_STEP)  // how long to wait between error retries
            ).Wait();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncExecutorWithRetriesTest_4()
        {
            int counter = 0;
            int lastIteration = 0;
            Func<int, Task<int>> myFunc = ((int funcCounter) =>
            {
                lastIteration = funcCounter;
                Assert.AreEqual(counter, funcCounter);
                Console.WriteLine("Running for {0} time.", counter);
                return Task.FromResult(++counter);
            });
            Func<Exception, int, bool> errorFilter = ((Exception exc, int i) =>
            {
                Assert.AreEqual(lastIteration, i);
                Assert.Fail("Should not be called");
                return true;
            });

            int maxRetries = 5;
            Task<int> promise = AsyncExecutorWithRetries.ExecuteWithRetries(
                myFunc, 
                maxRetries, 
                errorFilter,
                default(TimeSpan),
                new FixedBackoff(TimeSpan.FromSeconds(1)));

            int value = promise.Result;
            Console.WriteLine("Value={0} Counter={1} ExpectedRetries={2}", value, counter, 0);
            Assert.AreEqual(counter, value, "Counter == Returned value");
            Assert.AreEqual(counter, 1, "Counter == Returned value");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_AsyncExecutorWithRetriesTest_5()
        {
            int counter = 0;
            int lastIteration = 0;
            Func<int, Task<int>> myFunc = ((int funcCounter) =>
            {
                lastIteration = funcCounter;
                Assert.AreEqual(counter, funcCounter);
                Console.WriteLine("Running FUNC for {0} time.", counter);
                ++counter;
                throw new ArgumentException(counter.ToString(CultureInfo.InvariantCulture));
            });
            Func<Exception, int, bool> errorFilter = ((Exception exc, int i) =>
            {
                Console.WriteLine("Running ERROR FILTER for {0} time.", i);
                Assert.AreEqual(lastIteration, i);
                if (i==0 || i==1)
                    return true;
                else if (i == 2)
                    throw exc;
                else
                    return false;
            });

            int maxRetries = 5;
            Task<int> promise = AsyncExecutorWithRetries.ExecuteWithRetries(
                myFunc,
                maxRetries,
                errorFilter,
                default(TimeSpan),
                new FixedBackoff(TimeSpan.FromSeconds(1)));
            try
            {
                int value = promise.Result;
                Assert.Fail("Should have thrown");
            }
            catch (Exception exc)
            {
                Exception baseExc = exc.GetBaseException();
                Assert.AreEqual(baseExc.GetType(), typeof(ArgumentException));
                Console.WriteLine("baseExc.GetType()={0} Counter={1}", baseExc.GetType(), counter);
                Assert.AreEqual(counter, 3, "Counter == Returned value");
            }
        }
    }
}

// ReSharper restore ConvertToLambdaExpression
