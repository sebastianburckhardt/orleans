using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrains;
using SimpleGrain;

namespace UnitTests
{
    /// <summary>
    /// Summary description for SimpleGrain
    /// </summary>
    [TestClass]
    public class SimpleGrainTests : UnitTestBase
    {
        private readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        //ResultHandle result;

        public SimpleGrainTests()
            : base(new Options {StartPrimary = true, StartSecondary  = false }) // TEMP TEST HACK
        {
        }

        /// <summary>
        /// Gets or sets the test context which provides information about and functionality for the current test run.
        /// </summary>
        public TestContext TestContext { get; set; }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        [ClassInitialize]
        public static void MyClassInitialize(TestContext testContext)
        {
            //ResetDefaultRuntimes();
        }

        //
        // Use ClassCleanup to run code after all tests in a class have run
        [ClassCleanup]
        public static void MyClassCleanup()
        {
            //ResetDefaultRuntimes();
        }

        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task SimpleGrain_GetGrain()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            int ignored = await grain.GetAxB();
        }


        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrainControlFlow()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            
            Task setPromise = grain.SetA(2);
            setPromise.Wait();

            setPromise = grain.SetB(3);
            setPromise.Wait();

            Task<int> intPromise = grain.GetAxB();
            Assert.AreEqual(6, intPromise.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task SimpleGrainDataFlow()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");

            Task setAPromise = grain.SetA(3);
            Task setBPromise = grain.SetB(4);
            await Task.WhenAll(setAPromise, setBPromise);
            var x = await grain.GetAxB();
            result.Result = x;
            result.Done = true;
            
            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsNotNull(result.Result);
            Assert.AreEqual(12, result.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrainControlFlow_AsTask()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");

            Task setPromise = grain.SetA(2);
            setPromise.Wait();

            setPromise = grain.SetB(3);
            setPromise.Wait();

            Task<int> intPromise = grain.GetAxB();
            Assert.AreEqual(6, intPromise.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrainDataFlow_AsTask()
        {
            ResultHandle result = new ResultHandle();

            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");

            Task setAPromise = grain.SetA(3);
            Task setBPromise = grain.SetB(4);
            Task<int> intPromise = Task.Factory.ContinueWhenAll(new Task[] { setAPromise, setBPromise }, tasks =>
            {
                for (int i = 0; i < tasks.Length; i++ )
                {
                    Assert.IsFalse(tasks[i].IsFaulted, "Task " + i + " should not be Faulted");
                }

                return grain.GetAxB();
            }).Unwrap();

            intPromise.ContinueWith(x =>
            {
                result.Result = x.Result;
                result.Done = true;
            }).Wait();

            Assert.IsTrue(result.WaitForFinished(timeout));
            Assert.IsNotNull(result.Result);
            Assert.AreEqual(12, result.Result);
        }

        // TODO: Extension methods
        //[TestMethod]
        //public void SimpleGrainPropertyTest()
        //{
        //    ResultHandle result = new ResultHandle();

        //    ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
        //    var setPromise = grain.Set_A(17);
        //    setPromise.ContinueWith(() =>
        //        {
        //            AsyncValue<int> intPromise = grain.A;
        //            intPromise.ContinueWith(x =>
        //                                        {
        //                                            result.Result = x;
        //                                            result.Done = true;
        //                                        });
        //        });

        //    Assert.IsTrue(result.WaitForFinished(timeout));
        //    Assert.IsNotNull(result.Result);
        //    Assert.AreEqual(17, result.Result);
        //}

        // TODO: Extension methods
        //[TestMethod]
        //public void SimpleGrainReferenceTest()
        //{
        //    ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");

        //    AsyncValue<int> intPromise = grain.GetAxB( 5, 6 );
        //    Assert.AreEqual(30, intPromise.Result);

        //    AsyncCompletion setPromise = grain.SetA(3);
        //    setPromise.Wait();
        //    setPromise = grain.SetB(4);
        //    setPromise.Wait();
        //    AsyncValue<int> intPromise2 = grain.GetAxB();
        //    AsyncValue<int> intPromise3 = grain.GetAxB( intPromise2, 10 );
        //    Assert.AreEqual(120, intPromise3.Result);
        //    Console.WriteLine("ISimpleGrainTest DONE!");
        //}

        //[TestMethod]
        //public void SimpleGrainEvent()
        //{
        //    result = new ResultHandle();
        //    //ResetRuntimes();

        //    ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
        //    Assert.IsNotNull(grain, "Failed to get a grain reference");
        //    grain.StateUpdateEvent += grain_StateUpdateEvent1;

        //    AsyncCompletion setPromise = grain.SetA(3);
        //    AsyncValue<bool> continuePromise = result.AsyncWaitForContinue(timeout);
        //    Assert.IsTrue(continuePromise.Result);
        //    result.Continue = false;
        //    grain.StateUpdateEvent -= grain_StateUpdateEvent1;
        //    grain.StateUpdateEvent += grain_StateUpdateEvent2;
        //    setPromise = grain.SetB(5);
        //    continuePromise = result.AsyncWaitForContinue(timeout);
        //    Assert.IsTrue(continuePromise.Result);
        //    result.Done = true;
        //    Assert.IsTrue(result.WaitForFinished(timeout), "Timeout waiting for test to finish.");
        //}

        //[TestMethod, TestCategory("Nightly"), TestCategory("General")]
        //public void SimpleMainGrainContainerTest()
        //{
        //    var main = SimpleMainGrainFactory.GetGrain(GetRandomGrainId());
        //    main.Run()
        //        .Wait();
        //}

        //void grain_StateUpdateEvent1(object sender, SimpleGrainClient.StateUpdateEventArgs e)
        //{
        //    Console.WriteLine("grain_StateUpdateEvent1");
        //    Assert.AreEqual(3, e.A);
        //    result.Continue = true;
        //}

        //void grain_StateUpdateEvent2(object sender, SimpleGrainClient.StateUpdateEventArgs e)
        //{
        //    Console.WriteLine("grain_StateUpdateEvent2");
        //    Assert.AreEqual(3, e.A);
        //    Assert.AreEqual(5, e.B);
        //    result.Continue = true;
        //}

        //[TestMethod]
        //public void SimpleGrainEventHadnlerSetRemove()
        //{
        //    result = new ResultHandle();
        //    //ResetRuntimes();

        //    ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");        //    Assert.IsNotNull(grain, "Failed to get a grain reference");
        //    grain.StateUpdateEvent += grain_StateUpdateEvent3;

        //    AsyncCompletion setPromise = grain.SetA(4);
        //    AsyncValue<bool> continuePromise = result.AsyncWaitForContinue(timeout);
        //    Assert.IsTrue(continuePromise.Result);
        //    grain.StateUpdateEvent -= grain_StateUpdateEvent3;
        //    result.Continue = false;
        //    setPromise = grain.SetB(6);
        //    setPromise.Wait();
        //    result.Continue = false;
        //    grain.StateUpdateEvent += grain_StateUpdateEvent3;
        //    grain.StateUpdateEvent -= grain_StateUpdateEvent3;
        //    grain.StateUpdateEvent += grain_StateUpdateEvent3;
        //    setPromise = grain.SetB(7);
        //    continuePromise = result.AsyncWaitForContinue(timeout / 2);
        //    Assert.IsTrue(continuePromise.Result);            
        //}

        //void grain_StateUpdateEvent3(object sender, SimpleGrainClient.StateUpdateEventArgs e)
        //{
        //    Console.WriteLine("grain_StateUpdateEvent3");
        //    Assert.AreEqual(4, e.A);
        //    Assert.IsTrue(e.B == 0 || e.B == 7);
        //    result.Continue = true;
        //}

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrain_AsyncMethods()
        {
            ISimpleGrainWithAsyncMethods grain = SimpleGrainWithAsyncMethodsFactory.GetGrain(GetRandomGrainId());
            Task setPromise = grain.SetA_Async(10);
            setPromise.Wait();

            setPromise = grain.SetB_Async(30);
            setPromise.Wait();

            Task<int> intPromise = grain.GetAxB_Async();
            Assert.AreEqual(300, intPromise.Result);
        }

        // todo: [TestMethod]
        public void SimpleGrain_AsyncMethodWithResolver()
        {
            ISimpleGrainWithAsyncMethods grain = SimpleGrainWithAsyncMethodsFactory.GetGrain(GetRandomGrainId());

            Task<int> promise = grain.GetX();
            Thread.Sleep(1000); // Wait a second and see that the promise is still unresolved
            Assert.IsTrue(promise.Status == TaskStatus.Running);

            grain.SetX(100).Wait(); // Set x and resolve the promise by doing that
            Assert.AreEqual(100, promise.Result); // Now the promise should be resolved
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrain_PromiseForward()
        {
            ISimpleGrain forwardGrain = PromiseForwardGrainFactory.GetGrain(GetRandomGrainId());
            Task<int> promise = forwardGrain.GetAxB(5, 6);
            int result = promise.Result;
            Assert.AreEqual(30, result);
        }

        //GK disabled - tests ordering:
        //[TestMethod]
        public void SimpleGrainAsyncReference()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");

            grain.SetA(2);
            grain.SetB(3);
            Task<int> intPromise = grain.GetAxB();
            Assert.AreEqual(6, intPromise.Result);
        }

        //[TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        [TestMethod, TestCategory("Failures"), TestCategory("ReadOnly")]
        public void SimpleGrain_ReadOnly()
        {
            ISimpleGrain simple = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            var a = simple.ReadOnlyInterlock(1000);
            var b = simple.ReadOnlyInterlock(1000);
            try
            {
                Task.WhenAll(new[] {a, b}).Wait();
            }
            catch (TimeoutException)
            {
                Assert.Fail("ReadOnly requests should have executed concurrently");
            }
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public void SimpleGrain_ExclusiveNonReadOnly()
        {
            var simple = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            //var done = Enumerable.Range(0, 10).Select(_ => simple.ExclusiveWait(100)).ToArray();
            var done = new Task[10];
            for (int i = 0; i < 10; i++)
            {
                done[i] = simple.ExclusiveWait(100);
            }
            try
            {
                Task.WhenAll(done).Wait();
            }
            catch (InvalidOperationException)
            {
                Assert.Fail("Non read-only requests should not execute concurrently");
            }
            catch (Exception ex)
            {
                Assert.Fail("Unexpected exception " + ex.GetBaseException());
            }
        }

        //[TestMethod, TestCategory("Nightly"), TestCategory("General"), TestCategory("CLI")]
        //public void SimpleCLIGrainDataFlow()
        //{
        //    ResultHandle result = new ResultHandle();

        //    ISimpleCLIGrain grain = SimpleCLIGrainFactory.GetGrain(GetRandomGrainId()); ;

        //    AsyncCompletion setAPromise = grain.SetA(3);
        //    AsyncCompletion setBPromise = grain.SetB(4);
        //    AsyncValue<int> intPromise = AsyncCompletion.Join(setAPromise, setBPromise).ContinueWith(() =>
        //    {
        //        return grain.GetAxB();
        //    });

        //    intPromise.ContinueWith(x =>
        //    {
        //        result.Result = x;
        //        result.Done = true;
        //    }).Ignore();

        //    Assert.IsTrue(result.WaitForFinished(timeout));
        //    Assert.IsNotNull(result.Result);
        //    Assert.AreEqual(12, result.Result);
        //}

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void SimpleGrain_Timing_1()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            
            DateTime t1 = default(DateTime), t2 = default(DateTime), t3 = default(DateTime), t4 = default(DateTime), t5 = default(DateTime), t6 = default(DateTime);
            AsyncCompletion p1 = null, p2 = null;
            for (int i = 0; i < 2; i++) // execute 2 times to eliminate time differences at JIT compile at first execution.
            {
                t1 = DateTime.UtcNow;
                p1 = AsyncCompletion.FromTask(grain.SetA(2));
                t2 = DateTime.UtcNow;
                p2 = p1.ContinueWith(() =>
                {
                    t3 = DateTime.UtcNow;
                    Thread.Sleep(1000);
                    t4 = DateTime.UtcNow;
                });
                t5 = DateTime.UtcNow;
                p2.Wait();
                t6 = DateTime.UtcNow;
            }
        }

        public void SimpleGrain_Timing_2()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            
            for (int i = 0; i < 10; i++)
            {
                DateTime t1 = DateTime.UtcNow;
                AsyncCompletion p1 = AsyncCompletion.FromTask(grain.SetA(2));
                DateTime t2 = DateTime.UtcNow;
                p1.Wait();
            }
        }

        private bool AlmostEqual(DateTime t1, DateTime t2, TimeSpan allowedDelta)
        {
            TimeSpan delta = TimeSpan.FromTicks(Math.Abs(t1.Ticks - t2.Ticks));
            return delta <= allowedDelta;
        }
    }
}
