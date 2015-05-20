using System;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using BenchmarkGrains;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using SimpleGrain;

namespace UnitTests
{
    /// <summary>
    /// Summary description for GrainReferenceTest
    /// </summary>
    [TestClass]
    public class GrainReferenceTest : UnitTestBase
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(5);
        private static readonly Random random = new Random();

        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestCleanup]
        public void TestCleanup()
        {
            UnitTestBase.CheckForUnobservedPromises();
        }

        private TestContext testContextInstance;

        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        #region Additional test attributes
        //
        // You can use the following additional attributes as you write your tests:
        //
        // Use ClassInitialize to run code before running the first test in the class
        // [ClassInitialize()]
        // public static void MyClassInitialize(TestContext testContext) { }
        //
        // Use ClassCleanup to run code after all tests in a class have run
        // [ClassCleanup()]
        // public static void MyClassCleanup() { }
        //
        // Use TestInitialize to run code before running each test 
        // [TestInitialize()]
        // public void MyTestInitialize() { }
        //
        // Use TestCleanup to run code after each test has run
        // [TestCleanup()]
        // public void MyTestCleanup() { }
        //
        #endregion

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void GrainReferenceComparison_DifferentReference()
        {
            ISimpleGrain ref1 = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            ISimpleGrain ref2 = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            Assert.IsTrue(ref1 != ref2);
            Assert.IsTrue(ref2 != ref1);
            Assert.IsFalse(ref1 == ref2);
            Assert.IsFalse(ref2 == ref1);
            Assert.IsFalse(ref1.Equals(ref2));
            Assert.IsFalse(ref2.Equals(ref1));
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void TaskCompletionSource_Resolve()
        {
            string str = "Hello TaskCompletionSource";
            TaskCompletionSource<string> tcs = new TaskCompletionSource<string>();
            Task task = tcs.Task;
            Assert.IsFalse(task.IsCompleted, "TCS.Task not yet completed");
            tcs.SetResult(str);
            Assert.IsTrue(task.IsCompleted, "TCS.Task is now completed");
            Assert.IsFalse(task.IsFaulted, "TCS.Task should not be in faulted state: " + task.Exception);
            Assert.AreEqual(str, tcs.Task.Result, "Result");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void AsyncValueResolver_Resolve()
        {
            string str = "Hello Asyncvalue";
            AsyncValueResolver<string> avr = new AsyncValueResolver<string>();
            AsyncValue<string> av = avr.AsyncValue;
            Assert.IsFalse(av.IsCompleted, "AsyncValue not yet completed");
            avr.Resolve(str);
            Assert.IsTrue(av.IsCompleted, "AsyncValue is now completed");
            Assert.IsFalse(av.IsFaulted, "AsyncValue should not be in faulted state: " + av.Exception);
            Assert.AreEqual(str, av.GetValue(), "Result");
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("General")]
        public void GrainReference_Pass_this()
        {
            IChainedGrain g1 = ChainedGrainFactory.GetGrain(1);
            IChainedGrain g2 = ChainedGrainFactory.GetGrain(2);
            
            g1.PassThis(g2).Wait();
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Serialization")]
        public void GrainReference_DotNet_Serialization()
        {
            int id = random.Next();
            TestGrainReferenceSerialization(id, false, false);
        }

        [TestMethod, TestCategory("Nightly"), TestCategory("Serialization")]
        public void GrainReference_DotNet_Serialization_Unresolved()
        {
            int id = random.Next();
            TestGrainReferenceSerialization(id, false, false);
        }

        // Test case currently fails:
        // Json serializer requires message types to be simple DTOs with default constuctors and read-write properties
        // http://stackoverflow.com/questions/19517422/eastnetq-json-serialization-exception
        // Newtonsoft.Json.JsonSerializationException: Unable to find a constructor to use for type SimpleGrain.SimpleGrainFactory+SimpleGrainReference. A class should either have a default constructor, one constructor with arguments or a constructor marked with the JsonConstructor attribute. Path 'A', line 1, position 5.
        [TestMethod, TestCategory("Failures"), TestCategory("Serialization"), TestCategory("Json")]
        public void GrainReference_Json_Serialization()
        {
            int id = random.Next();
            TestGrainReferenceSerialization(id, true, true);
        }

        // Test case currently fails:
        // Json serializer requires message types to be simple DTOs with default constuctors and read-write properties
        // http://stackoverflow.com/questions/19517422/eastnetq-json-serialization-exception
        // Newtonsoft.Json.JsonSerializationException: Unable to find a constructor to use for type SimpleGrain.SimpleGrainFactory+SimpleGrainReference. A class should either have a default constructor, one constructor with arguments or a constructor marked with the JsonConstructor attribute. Path 'A', line 1, position 5.
        [TestMethod, TestCategory("Failures"), TestCategory("Serialization"), TestCategory("Json")]
        public void GrainReference_Json_Serialization_Unresolved()
        {
            int id = random.Next();
            TestGrainReferenceSerialization(id, false, true);
        }

        private static void TestGrainReferenceSerialization(int id, bool resolveBeforeSerialize, bool useJson)
        {
            // Make sure grain references serialize well through .NET serializer.
            var grain = SimpleGrainFactory.GetGrain(id, "SimpleGrain");

            if (resolveBeforeSerialize)
            {
                grain.SetA(id).Wait(); //  Resolve GR
            }

            object other;
            if (useJson)
            {
                // Serialize + Deserialise through Json serializer
                other = NewtonsoftJsonSerialiseRoundtrip(grain);
                //other = JavaScriptJsonSerialiseRoundtrip(grain);
            }
            else
            {
                // Serialize + Deserialise through .NET serializer
                other = DotNetSerialiseRoundtrip(grain);
            }

            if (!resolveBeforeSerialize)
            {
                grain.SetA(id).Wait(); //  Resolve GR
            }

            Assert.IsInstanceOfType(other, grain.GetType(), "Deserialized grain reference type = {0}", grain.GetType());
            ISimpleGrain otherGrain = other as ISimpleGrain;
            Assert.IsNotNull(otherGrain, "Other grain");
            Assert.AreEqual(grain, otherGrain, "Deserialized grain reference equality is preserved");
            int res = otherGrain.GetA().Result;
            Assert.AreEqual(id, res, "Returned values from call to deserialized grain reference");
        }

        private static object DotNetSerialiseRoundtrip(object obj)
        {
            object other;
            using (var memoryStream = new MemoryStream())
            {
                var formatter = new BinaryFormatter();
                formatter.Serialize(memoryStream, obj);
                memoryStream.Flush();
                memoryStream.Position = 0; // Reset to start
                other = formatter.Deserialize(memoryStream);
            }
            return other;
        }

        private static object NewtonsoftJsonSerialiseRoundtrip(object obj)
        {
            // http://james.newtonking.com/json/help/index.html?topic=html/T_Newtonsoft_Json_JsonConvert.htm
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
            object other = Newtonsoft.Json.JsonConvert.DeserializeObject(json, obj.GetType());
            return other;
        }

        private static object JavaScriptJsonSerialiseRoundtrip(object obj)
        {
            JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
            string json = jsonSerializer.Serialize(obj);
            object other = jsonSerializer.Deserialize(json, obj.GetType());
            return other;
        }

        //[TestMethod]
        //public void GrainReference_Interning()
        //{
        //    Guid guid = new Guid();
        //    GrainId gid1 = GrainId.GetGrainId(guid.ToString());
        //    GrainId gid2 = GrainId.GetGrainId(guid.ToByteArray());
        //    GrainReference g1 = GrainReference.FromGrainId(gid1);
        //    GrainReference g2 = GrainReference.FromGrainId(gid2);
        //    Assert.AreEqual(g1, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g1, g2, "Should be same / intern'ed GrainReference object");

        //    // Round-trip through Serializer
        //    GrainReference g3 = (GrainReference) SerializationManager.DeserializeWrapper(SerializationManager.SerializeWrapper(g1));
        //    Assert.AreEqual(g3, g1, "Should be equal GrainReference's");
        //    Assert.AreEqual(g3, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g3, g1, "Should be same / intern'ed GrainReference object");
        //    Assert.AreSame(g3, g2, "Should be same / intern'ed GrainReference object");
        //}

        //[TestMethod]
        //public void GrainReference_Interning_Sys_DirectoryGrain()
        //{
        //    GrainReference g1 = GrainReference.FromGrainId(Constants.DirectoryServiceId);
        //    GrainReference g2 = GrainReference.FromGrainId(Constants.DirectoryServiceId);
        //    Assert.AreEqual(g1, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g1, g2, "Should be same / intern'ed GrainReference object");

        //    // Round-trip through Serializer
        //    GrainReference g3 = (GrainReference)SerializationManager.DeserializeWrapper(SerializationManager.SerializeWrapper(g1));
        //    Assert.AreEqual(g3, g1, "Should be equal GrainReference's");
        //    Assert.AreEqual(g3, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g3, g1, "Should be same / intern'ed GrainReference object");
        //    Assert.AreSame(g3, g2, "Should be same / intern'ed GrainReference object");
        //}

        //[TestMethod]
        //public void GrainReference_Interning_Sys_StoreGrain()
        //{
        //    GrainReference g1 = GrainReference.FromGrainId(Constants.SystemPersistenceId);
        //    GrainReference g2 = GrainReference.FromGrainId(Constants.SystemPersistenceId);
        //    Assert.AreEqual(g1, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g1, g2, "Should be same / intern'ed GrainReference object");

        //    // Round-trip through Serializer
        //    GrainReference g3 = (GrainReference)SerializationManager.DeserializeWrapper(SerializationManager.SerializeWrapper(g1));
        //    Assert.AreEqual(g3, g1, "Should be equal GrainReference's");
        //    Assert.AreEqual(g3, g2, "Should be equal GrainReference's");
        //    Assert.AreSame(g3, g1, "Should be same / intern'ed GrainReference object");
        //    Assert.AreSame(g3, g2, "Should be same / intern'ed GrainReference object");
        //}

        //[TestMethod]
        //public void GrainReference_Interning_Sys_StoreGrain_Facets()
        //{
        //    GrainReference g1 = (GrainReference)StoreGrainFactory.Cast(GrainReference.FromGrainId(Constants.SystemPersistenceId));
        //    GrainReference g2 = (GrainReference)StoreAccessFactory.Cast(GrainReference.FromGrainId(Constants.SystemPersistenceId));
        //    Assert.AreEqual(g1.GrainId, g2.GrainId, "Faceted GrainReference's with same GrainId");
        //    Assert.AreNotSame(g1, g2, "GrainReferences for different interfaces on same grain should be different");
        //}

#if TODO
        //GK disabled:
        //[TestMethod]
        public void ReferenceRecovery()
        {
            ISimpleGrain grain = SimpleGrainFactory.GetGrain(GetRandomGrainId(), "SimpleGrain");
            Guid channelId = ((GrainReferenceAsync)grain).Reference.Channel.Id;

            grain.SetA(2).Wait(1000);

            Orleans.Stop();
            DirectoryServiceRuntime.Stop();
            DirectoryServiceRuntime.Start(44444);
            Orleans.Start();
            AsyncCompletion promise = grain.SetB(3);
            promise.Wait();
            int result = grain.GetAxB().Result;
            Assert.AreEqual(0, result);
            Assert.AreNotEqual(channelId, ((GrainReferenceAsync)grain).Reference.Channel.Id, "Channel stayed the same after reference recovery while it was expected to change.");

            grain.SetA(5).Wait(5000);
            result = grain.GetAxB().Result;
            Assert.AreEqual(15, result);
        }

        //GK disabled:
        //[TestMethod]
        public void ReferenceRecoveryPersistentGrain()
        {
            try
            {
                ISimplePersistentGrain grain = SimplePersistentGrainFactory.GetGrain(GetRandomGrainId());
                Guid channelId = ((GrainReferenceAsync) grain).Reference.Channel.Id;
                
                grain.SetA(2).Wait(1000);

                Orleans.Stop();
                DirectoryServiceRuntime.Stop();
                DirectoryServiceRuntime.Start(44444);
                Orleans.Start();

                AsyncCompletion promise = grain.SetB(3);
                promise.Wait();
                int result = grain.GetAxB().Result;
                Assert.AreEqual(6, result);
                Assert.AreNotEqual(channelId, ((GrainReferenceAsync)grain).Reference.Channel.Id, "Channel stayed the same after reference recovery while it was expected to change.");

                grain.SetA(5).Wait(5000);
                result = grain.GetAxB().Result;
                Assert.AreEqual(15, result);
            }
            catch (Exception exc)
            {
                Console.Write("Test Exception: {0}", exc);
            }
        }


        //[TestMethod]
        //public void ReferenceInvalidation()
        //{
        //    result = new ResultHandle();
        //    SimpleGrainReference grain = SimpleGrainReference.GetReference("ReferenceInvalidation");
        //    grain.InvalidationEvent += grain_InvalidationEvent;
        //    ResetDefaultRuntimes();

        //    Assert.IsTrue(result.WaitForFinished(5000));
        //}

        //void grain_InvalidationEvent(object source, GrainReferenceInvalidationEventArgs e)
        //{
        //    result.Done = true;
        //}
#endif
    }
}
