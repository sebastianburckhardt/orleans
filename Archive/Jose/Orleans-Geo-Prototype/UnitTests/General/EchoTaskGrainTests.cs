using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Echo;


namespace UnitTests.General
{
    [TestClass]
    public class EchoTaskGrainTests : UnitTestBase
    {
        private readonly TimeSpan timeout = Debugger.IsAttached ? TimeSpan.FromMinutes(10) : TimeSpan.FromSeconds(10);

        const string expectedEcho = "Hello from EchoGrain";
        const string expectedEchoError = "Error from EchoGrain";
        private IEchoTaskGrain grain;

        public static readonly TimeSpan Epsilon = TimeSpan.FromSeconds(1);

        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_GetGrain()
        {
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_Echo()
        {
            Stopwatch clock = new Stopwatch();

            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("CreateGrain took " + clock.Elapsed);

            clock.Restart();
            Task<string> promise = grain.EchoAsync(expectedEcho);
            string received = promise.Result;
            logger.Info("EchoGrain.Echo took " + clock.Elapsed);

            Assert.AreEqual(expectedEcho, received);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_EchoError()
        {
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
        
            Task<string> promise = grain.EchoErrorAsync(expectedEchoError);
            bool ok = promise.ContinueWith(t =>
            {
                if (!t.IsFaulted) Assert.Fail("EchoError should not have completed successfully");

                Exception exc = t.Exception;
                while (exc is AggregateException) exc = exc.InnerException;
                string received = exc.Message;
                Assert.AreEqual(expectedEchoError, received);
            }).Wait(timeout);
            Assert.IsTrue(ok, "Finished OK");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_Timeout_Wait()
        {
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
        
            TimeSpan delay30 = TimeSpan.FromSeconds(30); // grain call timeout (set in config)
            TimeSpan delay45 = TimeSpan.FromSeconds(45);
            TimeSpan delay60 = TimeSpan.FromSeconds(60);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Task<int> promise = grain.BlockingCallTimeoutAsync(delay60);
            bool ok = promise.ContinueWith(t =>
            {
                if (!t.IsFaulted) Assert.Fail("BlockingCallTimeout should not have completed successfully");

                Exception exc = t.Exception;
                while (exc is AggregateException) exc = exc.InnerException;
                Assert.AreEqual(typeof(TimeoutException), exc.GetType(), "Received exception type: {0}", exc);
            }).Wait(delay45);
            sw.Stop();
            Assert.IsTrue(ok, "Wait should not have timed-out. The grain call should have time out.");
            Assert.IsTrue(TimeIsLonger(sw.Elapsed, delay30), "Elapsted time out of range: {0}", sw.Elapsed);
            Assert.IsTrue(TimeIsShorter(sw.Elapsed, delay60), "Elapsted time out of range: {0}", sw.Elapsed);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public async Task EchoGrain_Timeout_Await()
        {
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            
            TimeSpan delay30 = TimeSpan.FromSeconds(30);
            TimeSpan delay60 = TimeSpan.FromSeconds(60);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                Task<int> promise = grain.BlockingCallTimeoutAsync(delay60);
                var res = await promise;
                Assert.Fail("BlockingCallTimeout should not have completed successfully");
            }
            catch (Exception exc)
            {
                while (exc is AggregateException) exc = exc.InnerException;
                Assert.AreEqual(typeof(TimeoutException), exc.GetType(), "Received exception type: {0}", exc);
            }
            sw.Stop();
            Assert.IsTrue(TimeIsLonger(sw.Elapsed, delay30), "Elapsted time out of range: {0}", sw.Elapsed);
            Assert.IsTrue(TimeIsShorter(sw.Elapsed, delay60), "Elapsted time out of range: {0}", sw.Elapsed);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_Timeout_Result()
        {
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            
            TimeSpan delay30 = TimeSpan.FromSeconds(30);
            TimeSpan delay60 = TimeSpan.FromSeconds(60);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            try
            {
                Task<int> promise = grain.BlockingCallTimeoutAsync(delay60);
                var res = promise.Result;
                Assert.Fail("BlockingCallTimeout should not have completed successfully, but returned " + res);
            }
            catch (Exception exc)
            {
                while (exc is AggregateException) exc = exc.InnerException;
                Assert.AreEqual(typeof(TimeoutException), exc.GetType(), "Received exception type: {0}", exc);
            }
            sw.Stop();
            Assert.IsTrue(TimeIsLonger(sw.Elapsed, delay30), "Elapsted time out of range: {0}", sw.Elapsed);
            Assert.IsTrue(TimeIsShorter(sw.Elapsed, delay60), "Elapsted time out of range: {0}", sw.Elapsed);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_LastEcho()
        {
            Stopwatch clock = new Stopwatch();

            EchoGrain_Echo();

            clock.Start();
            Task<string> promise = grain.LastEchoAsync;
            string received = promise.Result;
            logger.Info("EchoGrain.LastEcho took " + clock.Elapsed);

            Assert.AreEqual(expectedEcho, received, "LastEcho-Echo");

            EchoGrain_EchoError();

            clock.Restart();
            promise = grain.LastEchoAsync;
            received = promise.Result;
            logger.Info("EchoGrain.LastEcho-Error took " + clock.Elapsed);

            Assert.AreEqual(expectedEchoError, received, "LastEcho-Error");
        }
        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_Ping()
        {
            Stopwatch clock = new Stopwatch();

            string what = "CreateGrain";
            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("{0} took {1}", what, clock.Elapsed);

            what = "EchoGrain.Ping";
            clock.Restart();
            Task promise = grain.PingAsync();
            bool ok = promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsTrue(ok, "Finished OK");
            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_PingSilo_Local()
        {
            Stopwatch clock = new Stopwatch();

            string what = "CreateGrain";
            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("{0} took {1}", what, clock.Elapsed);

            what = "EchoGrain.PingLocalSilo";
            clock.Restart();
            Task promise = grain.PingLocalSiloAsync();
            bool ok = promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsTrue(ok, "Finished OK");
            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_PingSilo_Remote()
        {
            Stopwatch clock = new Stopwatch();

            string what = "CreateGrain";
            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("{0} took {1}", what, clock.Elapsed);

            SiloAddress silo1 = Primary.Silo.SiloAddress;
            SiloAddress silo2 = Secondary.Silo.SiloAddress;

            what = "EchoGrain.PingRemoteSilo[1]";
            clock.Restart();
            Task promise = grain.PingRemoteSiloAsync(silo1);
            bool ok = promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsTrue(ok, "Finished OK");
            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);

            what = "EchoGrain.PingRemoteSilo[2]";
            clock.Restart();
            promise = grain.PingRemoteSiloAsync(silo2);
            ok = promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsTrue(ok, "Finished OK");
            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_PingSilo_OtherSilo()
        {
            Stopwatch clock = new Stopwatch();

            string what = "CreateGrain";
            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("{0} took {1}", what, clock.Elapsed);

            what = "EchoGrain.PingOtherSilo";
            clock.Restart();
            Task promise = grain.PingOtherSiloAsync();
            bool ok = promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsTrue(ok, "Finished OK");
            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("General")]
        public void EchoGrain_PingSilo_OtherSilo_Membership()
        {
            Stopwatch clock = new Stopwatch();

            string what = "CreateGrain";
            clock.Start();
            grain = EchoTaskGrainFactory.GetGrain(GetRandomGrainId());
            logger.Info("{0} took {1}", what, clock.Elapsed);

            what = "EchoGrain.PingOtherSiloMembership";
            clock.Restart();
            Task promise = grain.PingClusterMemberAsync();
            promise.Wait(timeout);
            logger.Info("{0} took {1}", what, clock.Elapsed);

            Assert.IsFalse(promise.IsFaulted, what + " Faulted " + promise.Exception);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task EchoTaskGrain_Await()
        {
            IBlockingEchoTaskGrain g = BlockingEchoTaskGrainFactory.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo(expectedEcho);
            string received = await promise;
            Assert.AreEqual(expectedEcho, received, "Echo");

            promise = g.CallMethodAV_Await(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodAV_Await");

            promise = g.CallMethodTask_Await(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodTask_Await");
        }

        [TestMethod, TestCategory("Failures"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task EchoTaskGrain_Blocking()
        {
            IBlockingEchoTaskGrain g = BlockingEchoTaskGrainFactory.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo(expectedEcho);
            string received = await promise;
            Assert.AreEqual(expectedEcho, received, "Echo");

            promise = g.CallMethodAV_Block(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodAV_Block");

            promise = g.CallMethodTask_Block(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodTask_Block");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task EchoTaskGrain_Await_Reentrant()
        {
            IReentrantBlockingEchoTaskGrain g = ReentrantBlockingEchoTaskGrainFactory.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo(expectedEcho);
            string received = await promise;
            Assert.AreEqual(expectedEcho, received, "Echo");

            promise = g.CallMethodAV_Await(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodAV_Await");

            promise = g.CallMethodTask_Await(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodTask_Await");
        }

        [TestMethod, TestCategory("Failures"), TestCategory("General"), TestCategory("SimpleGrain")]
        public async Task EchoTaskGrain_Blocking_Reentrant()
        {
            IReentrantBlockingEchoTaskGrain g = ReentrantBlockingEchoTaskGrainFactory.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo(expectedEcho);
            string received = await promise;
            Assert.AreEqual(expectedEcho, received, "Echo");

            promise = g.CallMethodAV_Block(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodAV_Block");

            promise = g.CallMethodTask_Block(expectedEcho);
            received = await promise;
            Assert.AreEqual(expectedEcho, received, "CallMethodTask_Block");
        }

        // ---------- Utility methods ----------

        private bool TimeIsLonger(TimeSpan time, TimeSpan limit)
        {
            logger.Info("Compare TimeIsLonger: Actual={0} Limit={1} Epsilon={2}", time, limit, Epsilon);
            return time >= (limit - Epsilon);
        }

        private bool TimeIsShorter(TimeSpan time, TimeSpan limit)
        {
            logger.Info("Compare TimeIsShorter: Actual={0} Limit={1} Epsilon={2}", time, limit, Epsilon);
            return time <= (limit + Epsilon);
        }
    }
}
