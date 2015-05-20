using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;

namespace UnitTests
{
    [TestClass]
    public class ClientAddressableTests : UnitTestBase
    {
        private object anchor;

        private class MyPseudoGrain : IClientAddressableTestClientObject
        {
            private int counter = 0;
            private List<int> numbers = new List<int>();

            public Task<string> OnHappyPath(string message)
            {
                if (string.IsNullOrEmpty(message))
                    throw new ArgumentException("target");
                else
                    return Task.FromResult(message);
            }

            public Task OnSadPath(string message)
            {
                if (string.IsNullOrEmpty(message))
                    throw new ArgumentException("target");
                else
                    throw new ApplicationException(message);
            }

            public Task<int> OnSerialStress(int n)
            {
                Assert.AreEqual(this.counter, n);
                ++this.counter;
                return Task.FromResult(10000 + n);
            }

            public Task<int> OnParallelStress(int n)
            {
                this.numbers.Add(n);
                return Task.FromResult(10000 + n);
            }

            public void VerifyNumbers(int iterationCount)
            {
                Assert.AreEqual(iterationCount, this.numbers.Count);
                this.numbers.Sort();
                for (var i = 0; i < this.numbers.Count; ++i)
                    Assert.AreEqual(i, this.numbers[i]);
            }
        }

        private class MyProducer : IClientAddressableTestProducer
        {
            int counter = 0;

            public Task<int> Poll()
            {
                ++this.counter;
                return Task.FromResult(this.counter);
            }
        }

        [TestCleanup]
        public void CleanupTest()
        {
            this.anchor = null;
        }

        [TestMethod, TestCategory("ClientAddressable"), TestCategory("Nightly"), TestCategory("BVT")]
        public async Task TestClientAddressableHappyPath()
        {
            var myOb = new MyPseudoGrain();
            this.anchor = myOb;
            var myRef = await ClientAddressableTestClientObjectFactory.CreateObjectReference(myOb);
            var proxy = ClientAddressableTestGrainFactory.GetGrain(GetRandomGrainId());
            const string expected = "o hai!";
            await proxy.SetTarget(myRef);
            var actual = await proxy.HappyPath(expected);
            Assert.AreEqual(expected, actual);

            ClientAddressableTestClientObjectFactory.DeleteObjectReference(myRef);
        }

        [TestMethod, TestCategory("ClientAddressable"), TestCategory("Nightly"), TestCategory("BVT")]
        [ExpectedException(typeof(ApplicationException))]
        public async Task TestClientAddressableSadPath()
        {
            const string message = "o hai!";
            try
            {
                var myOb = new MyPseudoGrain();
                this.anchor = myOb;
                var myRef = await ClientAddressableTestClientObjectFactory.CreateObjectReference(myOb);
                var proxy = ClientAddressableTestGrainFactory.GetGrain(GetRandomGrainId());
                await proxy.SetTarget(myRef);
                await proxy.SadPath(message);

                ClientAddressableTestClientObjectFactory.DeleteObjectReference(myRef);
            }
            catch (AggregateException e)
            {
                var ef = e.Flatten();
                if (ef.InnerExceptions.Count == 1 &&
                    ef.InnerExceptions[0] is ApplicationException 
                    && ef.InnerExceptions[0].Message == message)
                {
                    throw (ApplicationException)ef.InnerExceptions[0];                    
                }
                else
                {
                    throw;                    
                }
            }
        }

        [TestMethod, TestCategory("ClientAddressable"), TestCategory("Nightly")]
        public async Task GrainShouldSuccessfullyPullFromClientObject()
        {
            var myOb = new MyProducer();
            this.anchor = myOb;
            var myRef = await ClientAddressableTestProducerFactory.CreateObjectReference(myOb);
            var rendez = ClientAddressableTestRendezvousGrainFactory.GetGrain(0);
            var consumer = ClientAddressableTestConsumerFactory.GetGrain(0);

            await rendez.SetProducer(myRef);
            await consumer.Setup();
            var n = await consumer.PollProducer();
            Assert.AreEqual(1, n);

            ClientAddressableTestProducerFactory.DeleteObjectReference(myRef);
        }

        [TestMethod, TestCategory("ClientAddressable"), TestCategory("Nightly")]
        public async Task MicroClientAddressableSerialStressTest()
        {
            const int iterationCount = 1000;

            var myOb = new MyPseudoGrain();
            this.anchor = myOb;
            var myRef = await ClientAddressableTestClientObjectFactory.CreateObjectReference(myOb);
            var proxy = ClientAddressableTestGrainFactory.GetGrain(GetRandomGrainId());
            await proxy.SetTarget(myRef);
            await proxy.MicroSerialStressTest(iterationCount);

            ClientAddressableTestClientObjectFactory.DeleteObjectReference(myRef);
        }

        [TestMethod, TestCategory("ClientAddressable"), TestCategory("Nightly")]
        public async Task MicroClientAddressableParallelStressTest()
        {
            const int iterationCount = 1000;

            var myOb = new MyPseudoGrain();
            this.anchor = myOb;
            var myRef = await ClientAddressableTestClientObjectFactory.CreateObjectReference(myOb);
            var proxy = ClientAddressableTestGrainFactory.GetGrain(GetRandomGrainId());
            await proxy.SetTarget(myRef);
            await proxy.MicroParallelStressTest(iterationCount);

            ClientAddressableTestClientObjectFactory.DeleteObjectReference(myRef);

            myOb.VerifyNumbers(iterationCount);
        }
    }
}
