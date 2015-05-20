using System;
using System.Diagnostics;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

namespace UnitTests.AsyncPrimitivesTests
{
    [TestClass]
    public class AC_BackgroundWorkerTests
    {
        const bool shouldTrackObservations = true;
        private bool prevTrackObservations;

        [TestInitialize]
        public void MyTestInitialize()
        {
            List<string> unobservedPromises = AsyncCompletion.GetUnobservedPromises();

            prevTrackObservations = AsyncCompletion.TrackObservations;
            AsyncCompletion.TrackObservations = shouldTrackObservations;

            Console.WriteLine("Set AsyncCompletion.TrackObservations to {0}. There were {1} unobserved promise(s)", shouldTrackObservations, unobservedPromises.Count);
            OrleansTask.Reset();

            AC_AsyncValueTests.CheckUnobservedPromises(unobservedPromises);
        }

        [TestCleanup]
        public void MyTestCleanup()
        {
            List<string> unobservedPromises = AsyncCompletion.GetUnobservedPromises();

            AsyncCompletion.TrackObservations = prevTrackObservations;
            Console.WriteLine("Reset AsyncCompletion.TrackObservations to {0}. There were {1} unobserved promise(s)", prevTrackObservations, unobservedPromises.Count);
            OrleansTask.Reset();

            AC_AsyncValueTests.CheckUnobservedPromises(unobservedPromises);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ThreadPool_QueueUserWorkItem()
        {
            AsyncCompletionResolver ar = new AsyncCompletionResolver();

            WaitCallback wcb = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");
                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));
                // Resolve promise
                ar.Resolve();
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            object data = null;
            ThreadPool.QueueUserWorkItem(wcb, data);

            AsyncCompletion promise = ar.AsyncCompletion;
            promise.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ThreadPool_QueueUserWorkItem_Multi()
        {
            const int NumWorkItems = 10;

            AsyncCompletionResolver[] resolvers = new AsyncCompletionResolver[NumWorkItems];

            for (int i = 0; i < NumWorkItems; i++)
            {
                resolvers[i] = new AsyncCompletionResolver();
            }

            WaitCallback wcb = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");

                int i = (int)state;

                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // Resolve promise
                resolvers[i].Resolve();
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < NumWorkItems; i++)
            {
                ThreadPool.QueueUserWorkItem(wcb, i);
            }

            AsyncCompletion promise = AsyncCompletion.JoinAll(resolvers.Select(ar => ar.AsyncCompletion));
            promise.Wait();

            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 5000, "Waited longer than 5000ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_ThreadPool_QueueUserWorkItem()
        {
            AsyncValueResolver<int> ar = new AsyncValueResolver<int>();

            WaitCallback wcb = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");

                int i = (int)state;

                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // Resolve promise
                ar.Resolve(i);
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            int data = new Random().Next();
            ThreadPool.QueueUserWorkItem(wcb, data);

            AsyncValue<int> promise = ar.AsyncValue;
            int retVal = promise.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);
            Assert.AreEqual(data, retVal, "Expected value = " + data);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_ThreadPool_QueueUserWorkItem_Multi()
        {
            const int NumWorkItems = 10;

            AsyncValueResolver<int>[] resolvers = new AsyncValueResolver<int>[NumWorkItems];

            for (int i = 0; i < NumWorkItems; i++)
            {
                resolvers[i] = new AsyncValueResolver<int>();
            }

            WaitCallback wcb = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");

                int i = (int)state;

                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));

                // Resolve promise
                resolvers[i].Resolve(i);
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < NumWorkItems; i++)
            {
                ThreadPool.QueueUserWorkItem(wcb, i);
            }

            AsyncCompletion.JoinAll(resolvers.Select(ar => ar.AsyncValue)).Wait();

            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 5000, "Waited longer than 5000ms. " + stopwatch.ElapsedMilliseconds);
            for (int i = 0; i < NumWorkItems; i++)
            {
                int expected = i;
                Assert.AreEqual(expected, resolvers[i].AsyncValue.GetValue(), i + " - Expected value = " + expected);
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ThreadPool_ExecuteOnThreadPool()
        {
            Action action = () =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");
                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            AsyncCompletion promise = AsyncCompletion.ExecuteOnThreadPool(action);
            promise.Wait();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AC_ThreadPool_ExecuteOnThreadPool_Multi()
        {
            const int NumWorkItems = 10;

            AsyncCompletion[] promises = new AsyncCompletion[NumWorkItems];

            Action action = () =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");
                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < NumWorkItems; i++)
            {
                promises[i] = AsyncCompletion.ExecuteOnThreadPool(action);
            }

            AsyncCompletion promise = AsyncCompletion.JoinAll(promises);
            promise.Wait();
            
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 5000, "Waited longer than 5000ms. " + stopwatch.ElapsedMilliseconds);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_ThreadPool_ExecuteOnThreadPool()
        {
            Func<object,int> func = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");

                int i = (int)state;

                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));

                return i;
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            int data = new Random().Next();
            AsyncValue<int> promise = AsyncValue<int>.ExecuteOnThreadPool(func, data);
            int retVal = promise.GetValue();
            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 1200, "Waited longer than 1200ms. " + stopwatch.ElapsedMilliseconds);
            Assert.AreEqual(data, retVal, "Expected value = " + data);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("AsynchronyPrimitives")]
        public void AV_ThreadPool_ExecuteOnThreadPool_Multi()
        {
            const int NumWorkItems = 10;

            AsyncValue<int>[] promises = new AsyncValue<int>[NumWorkItems];

            Func<object, int> func = (state) =>
            {
                Assert.AreEqual(TaskScheduler.Default, TaskScheduler.Current, "Running under .NET ThreadPool / TaskScheduler.Default");

                int i = (int)state;

                // Do something
                Thread.Sleep(TimeSpan.FromSeconds(1));

                return i;
            };

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            for (int i = 0; i < NumWorkItems; i++)
            {
                promises[i] = AsyncValue<int>.ExecuteOnThreadPool(func, i);
            }

            AsyncCompletion.JoinAll(promises).Wait();

            stopwatch.Stop();
            Assert.IsTrue(stopwatch.ElapsedMilliseconds >= 800, "Waited less than 800ms. " + stopwatch.ElapsedMilliseconds); // check that we waited at least 0.8 second
            Assert.IsTrue(stopwatch.ElapsedMilliseconds <= 5000, "Waited longer than 5000ms. " + stopwatch.ElapsedMilliseconds);
            for (int i = 0; i < NumWorkItems; i++)
            {
                int expected = i;
                Assert.AreEqual(expected, promises[i].GetValue(), i + " - Expected value = " + expected);
            }
        }
    }
}
