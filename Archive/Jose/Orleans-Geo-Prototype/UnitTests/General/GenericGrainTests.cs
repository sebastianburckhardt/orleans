using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrainInterfaces.Generic;

namespace UnitTests
{
    [TestClass]
    public class GenericGrainTests : UnitTestBase
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);
        private static int grainId = 0;

        public GenericGrainTests() : base(true)
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
            //CheckForUnobservedPromises();
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
            //ResetDefaultRuntimes();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public async Task Generic_SimpleGrain_GetGrain()
        {
            var grain = SimpleGenericGrainFactory<int>.GetGrain(grainId++);
            await grain.GetA();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_SimpleGrainControlFlow()
        {
            var a = new Random().Next(100);
            var b = a + 1;
            var expected = a + "x" + b;

            var grain = SimpleGenericGrainFactory<int>.GetGrain(grainId++);

            grain.SetA(a).Wait();

            grain.SetB(b).Wait();

            AsyncValue<string> stringPromise = AsyncValue.FromTask(grain.GetAxB());
            Assert.AreEqual(expected, stringPromise.GetValue());
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_SimpleGrainDataFlow()
        {
            var a = new Random().Next(100);
            var b = a+1;
            var expected = a + "x" + b;
            
            ResultHandle result = new ResultHandle();

            var grain = SimpleGenericGrainFactory<int>.GetGrain(grainId++);

            AsyncCompletion setAPromise = AsyncCompletion.FromTask(grain.SetA(a));
            AsyncCompletion setBPromise = AsyncCompletion.FromTask(grain.SetB(b));
            AsyncValue<string> stringPromise = AsyncCompletion.Join(setAPromise, setBPromise).ContinueWith(() =>
            {
                return AsyncValue.FromTask(grain.GetAxB());
            });

            stringPromise.ContinueWith(x =>
            {
                result.Result = x;
                result.Done = true;
            },
            exc =>
            {
                Assert.Fail("Received exception: " + exc);
            }).Ignore();

            Assert.IsTrue(result.WaitForFinished(timeout), "WaitforFinished Timeout=" + timeout);
            Assert.IsNotNull(result.Result, "Should not be null result");
            Assert.AreEqual(expected, result.Result, "Got expected result");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Generics")]
        public async Task Generic_SimpleGrain2_GetGrain()
        {
            var g1 = SimpleGenericGrainFactory<int>.GetGrain(grainId++);
            var g2 = SimpleGenericGrainUFactory<int>.GetGrain(grainId++);
            var g3 = SimpleGenericGrain2Factory<int, int>.GetGrain(grainId++);
            await g1.GetA();
            await g2.GetA();
            await g3.GetA();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_SimpleGrainControlFlow2_SetAB()
        {
            var a = new Random().Next(100);
            var b = a + 1;
            var expected = a + "x" + b;

            var g1 = SimpleGenericGrainFactory<int>.GetGrain(grainId++);
            var g2 = SimpleGenericGrainUFactory<int>.GetGrain(grainId++);
            var g3 = SimpleGenericGrain2Factory<int, int>.GetGrain(grainId++);

            g1.SetA(a).Wait();
            g2.SetA(a).Wait();
            g3.SetA(a).Wait();

            g1.SetB(b).Wait();
            g2.SetB(b).Wait();
            g3.SetB(b).Wait();

            Task<string> r1 = g1.GetAxB();
            Task<string> r2 = g2.GetAxB();
            Task<string> r3 = g3.GetAxB();
            Assert.AreEqual(expected, r1.Result);
            Assert.AreEqual(expected, r2.Result);
            Assert.AreEqual(expected, r3.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_SimpleGrainControlFlow2_GetAB()
        {
            var a = new Random().Next(100);
            var b = a + 1;
            var expected = a + "x" + b;

            var g1 = SimpleGenericGrainFactory<int>.GetGrain(grainId++);
            var g2 = SimpleGenericGrainUFactory<int>.GetGrain(grainId++);
            var g3 = SimpleGenericGrain2Factory<int, int>.GetGrain(grainId++);

            Task<string> r1 = g1.GetAxB(a,b);
            Task<string> r2 = g2.GetAxB(a,b);
            Task<string> r3 = g3.GetAxB(a,b);
            var s1 = r1.Result;
            var s2 = r2.Result;
            var s3 = r3.Result;
            Assert.AreEqual(expected, s1);
            Assert.AreEqual(expected, s2);
            Assert.AreEqual(expected, s3);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen"), TestCategory("Generics")]
        public void Generic_SimpleGrainControlFlow3()
        {
            ISimpleGenericGrain2<int, float> g = SimpleGenericGrain2Factory<int, float>.GetGrain(grainId++);
            g.SetA(3).Wait();
            g.SetB(1.25f).Wait();
            Assert.AreEqual("3x1.25", g.GetAxB().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("CodeGen"), TestCategory("Generics")]
        public void Generic_SelfManagedGrainControlFlow()
        {
            IGenericSelfManagedGrain<int, float> g = GenericSelfManagedGrainFactory<int, float>.GetGrain(0);
            g.SetA(3).Wait();
            g.SetB(1.25f).Wait();
            Assert.AreEqual("3x1.25", g.GetAxB().Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void GrainWithListFields()
        {
            string a = new Random().Next(100).ToString(CultureInfo.InvariantCulture);
            string b = new Random().Next(100).ToString(CultureInfo.InvariantCulture);

            var g1 = GrainWithListFieldsFactory.GetGrain(grainId++);

            var p1 = g1.AddItem(a);
            var p2  = g1.AddItem(b);
            Task.WhenAll(p1,p2).Wait();

            var r1 = g1.GetItems().Result;
            Assert.AreEqual(a, r1[0]);
            Assert.AreEqual(b, r1[1]);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_GrainWithListFields()
        {
            int a = new Random().Next(100);
            int b = new Random().Next(100);

            var g1 = GenericGrainWithListFieldsFactory<int>.GetGrain(grainId++);

            var p1 = g1.AddItem(a);
            var p2 = g1.AddItem(b);
            Task.WhenAll(p1, p2).Wait();

            var r1 = g1.GetItems().Result;
            Assert.AreEqual(a, r1[0]);
            Assert.AreEqual(b, r1[1]);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_GrainWithNoProperties_ControlFlow()
        {
            int a = new Random().Next(100);
            int b = new Random().Next(100);
            string expected = a + "x" + b;

            var g1 = GenericGrainWithNoPropertiesFactory<int>.GetGrain(grainId++);

            Task<string> r1 = g1.GetAxB(a, b);
            Assert.AreEqual(expected, r1.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void GrainWithNoProperties_ControlFlow()
        {
            int a = new Random().Next(100);
            int b = new Random().Next(100);
            string expected = a + "x" + b;

            var g1 = GrainWithNoPropertiesFactory.GetGrain(grainId++);

            Task<string> r1 = g1.GetAxB(a, b);
            Assert.AreEqual(expected, r1.Result);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_ReaderWriterGrain1()
        {
            int a = new Random().Next(100);
            var g = GenericReaderWriterGrain1Factory<int>.GetGrain(grainId++);
            g.SetValue(a).Wait();
            var res = g.Value.Result;
            Assert.AreEqual(a, res);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_ReaderWriterGrain2()
        {
            int a = new Random().Next(100);
            string b = "bbbbb";
            var g = GenericReaderWriterGrain2Factory<int,string>.GetGrain(grainId++);
            g.SetValue1(a).Wait();
            g.SetValue2(b).Wait();
            var r1 = g.Value1.Result;
            Assert.AreEqual(a, r1);
            var r2 = g.Value2.Result;
            Assert.AreEqual(b, r2);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_ReaderWriterGrain3()
        {
            int a = new Random().Next(100);
            string b = "bbbbb";
            double c = 3.145;
            var g = GenericReaderWriterGrain3Factory<int, string, double>.GetGrain(grainId++);
            g.SetValue1(a).Wait();
            g.SetValue2(b).Wait();
            g.SetValue3(c).Wait();
            var r1 = g.Value1.Result;
            Assert.AreEqual(a, r1);
            var r2 = g.Value2.Result;
            Assert.AreEqual(b, r2);
            var r3 = g.Value3.Result;
            Assert.AreEqual(c, r3);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public void Generic_Non_Primitive_Type_Argument()
        {
            IEchoHubGrain<Guid, string> g1 = EchoHubGrainFactory<Guid, string>.GetGrain(1);
            IEchoHubGrain<Guid, int> g2 = EchoHubGrainFactory<Guid, int>.GetGrain(1);
            IEchoHubGrain<Guid, byte[]> g3 = EchoHubGrainFactory<Guid, byte[]>.GetGrain(1);
            
            Assert.AreNotEqual(((GrainReference)g1).GrainId, ((GrainReference)g2).GrainId);
            Assert.AreNotEqual(((GrainReference)g1).GrainId, ((GrainReference)g3).GrainId);
            Assert.AreNotEqual(((GrainReference)g2).GrainId, ((GrainReference)g3).GrainId);

            g1.Foo(Guid.Empty, "", 1).Wait();
            g2.Foo(Guid.Empty, 0, 2).Wait();
            g3.Foo(Guid.Empty, new byte[]{}, 3).Wait();

            Assert.AreEqual(g1.X.Result, 1);
            Assert.AreEqual(g2.X.Result, 2);
            Assert.AreEqual(g3.X.Result, 3);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Generics")]
        public async Task Generic_Echo_Chain_1()
        {
            const string msg = "Hello from EchoGenericChainGrain-1";

            IEchoGenericChainGrain<string> g = EchoGenericChainGrainFactory<string>.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo(msg);
            string received = await promise;
            Assert.AreEqual(msg, received, "Echo");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Generics")]
        public async Task Generic_Echo_Chain_2()
        {
            const string msg = "Hello from EchoGenericChainGrain-2";

            IEchoGenericChainGrain<string> g = EchoGenericChainGrainFactory<string>.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo2(msg);
            string received = await promise;
            Assert.AreEqual(msg, received, "Echo");
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Generics")]
        public async Task Generic_Echo_Chain_3()
        {
            const string msg = "Hello from EchoGenericChainGrain-3";

            IEchoGenericChainGrain<string> g = EchoGenericChainGrainFactory<string>.GetGrain(GetRandomGrainId());

            Task<string> promise = g.Echo3(msg);
            string received = await promise;
            Assert.AreEqual(msg, received, "Echo");
        }
    }
}
