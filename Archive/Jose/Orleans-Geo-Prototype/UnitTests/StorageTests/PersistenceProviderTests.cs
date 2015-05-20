using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Providers;
using Orleans.Runtime.Storage;
using Orleans.Storage;
using Samples.StorageProviders;

namespace UnitTests.StorageTests
{
    [TestClass]
    public class PersistenceProviderTests
    {
        public const string AzureConnectionString =
            //"UseDevelopmentStorage=true";
            "DefaultEndpointsProtocol=https;AccountName=orleanstestdata;AccountKey=qFJFT+YAikJPCE8V5yPlWZWBRGns4oti9tqG6/oYAYFGI4kFAnT91HeiWMa6pddUzDcG5OAmri/gk7owTOQZ+A==";

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            BufferPool.InitGlobalBufferPool(new MessagingConfiguration(false));
        }

        [ClassCleanup]
        public static void ClassCleanup()
        {
        }

        [TestInitialize]
        public void TestInitialize()
        {
        }

        [TestCleanup]
        public void TestCleanup()
        {
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence")]
        public async Task PersistenceProvider_Memory_WriteRead()
        {
            const string name = "PersistenceProvider_Memory_WriteRead";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store = new MockStorageProvider();
            await store.Init(name, storageProviderManager, null);
            await Test_PersistenceProvider_WriteRead(name, store, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence")]
        public async Task PersistenceProvider_FileStore_WriteRead()
        {
            const string name = "PersistenceProvider_FileStore_WriteRead";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            var cfgProps = new Dictionary<string, string>();
            cfgProps.Add("RootDirectory", "Data");
            IProviderConfiguration cfg = new ProviderConfiguration(cfgProps, "FileStorage", "FileStore");
            IStorageProvider store = new OrleansFileStorage();
            await store.Init(name, storageProviderManager, cfg);
            await Test_PersistenceProvider_WriteRead(name, store, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence")]
        public async Task PersistenceProvider_Sharded_WriteRead()
        {
            const string name = "PersistenceProvider_Sharded_WriteRead";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store1 = new MockStorageProvider();
            IStorageProvider store2 = new MockStorageProvider();
            await storageProviderManager.AddAndInitProvider("Store1", store1);
            await storageProviderManager.AddAndInitProvider("Store2", store2);
            var composite = await ConfigureShardedStorageProvider(name, storageProviderManager);

            await Test_PersistenceProvider_WriteRead(name, composite, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence")]
        public async Task PersistenceProvider_Sharded_9_WriteRead()
        {
            const string name = "PersistenceProvider_Sharded_9_WriteRead";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store1 = new MockStorageProvider();
            IStorageProvider store2 = new MockStorageProvider();
            IStorageProvider store3 = new MockStorageProvider();
            IStorageProvider store4 = new MockStorageProvider();
            IStorageProvider store5 = new MockStorageProvider();
            IStorageProvider store6 = new MockStorageProvider();
            IStorageProvider store7 = new MockStorageProvider();
            IStorageProvider store8 = new MockStorageProvider();
            IStorageProvider store9 = new MockStorageProvider();
            await storageProviderManager.AddAndInitProvider("Store1", store1);
            await storageProviderManager.AddAndInitProvider("Store2", store2);
            await storageProviderManager.AddAndInitProvider("Store3", store3);
            await storageProviderManager.AddAndInitProvider("Store4", store4);
            await storageProviderManager.AddAndInitProvider("Store5", store5);
            await storageProviderManager.AddAndInitProvider("Store6", store6);
            await storageProviderManager.AddAndInitProvider("Store7", store7);
            await storageProviderManager.AddAndInitProvider("Store8", store8);
            await storageProviderManager.AddAndInitProvider("Store9", store9);

            ShardedStorageProvider composite = await ConfigureShardedStorageProvider(name, storageProviderManager);

            await Test_PersistenceProvider_WriteRead(name, composite, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure")]
        public async Task PersistenceProvider_Azure_Read()
        {
            const string name = "PersistenceProvider_Azure_Read";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store = new AzureTableStorage();
            var properties = new Dictionary<string, string>();
            properties.Add("DataConnectionString", AzureConnectionString);
            var config = new ProviderConfiguration(properties, null);
            await store.Init(name, storageProviderManager, config);
            await Test_PersistenceProvider_Read(name, store, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure")]
        public async Task PersistenceProvider_Azure_WriteRead()
        {
            const string name = "PersistenceProvider_Azure_WriteRead";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store = new AzureTableStorage();
            var properties = new Dictionary<string, string>();
            properties.Add("DataConnectionString", AzureConnectionString);
            var config = new ProviderConfiguration(properties, null);
            await store.Init(name, storageProviderManager, config);
            await Test_PersistenceProvider_WriteRead(name, store, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure")]
        public async Task PersistenceProvider_Azure_WriteRead_Json()
        {
            const string name = "PersistenceProvider_Azure_WriteRead_Json";
            StorageProviderManager storageProviderManager = new StorageProviderManager();
            await storageProviderManager.LoadEmptyStorageProviders();

            IStorageProvider store = new AzureTableStorage();
            var properties = new Dictionary<string, string>();
            properties.Add("DataConnectionString", AzureConnectionString);
            properties.Add("UseJsonFormat", "true");
            var config = new ProviderConfiguration(properties, null);
            await store.Init(name, storageProviderManager, config);
            await Test_PersistenceProvider_WriteRead(name, store, storageProviderManager);
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Nightly"), TestCategory("Persistence"), TestCategory("Azure")]
        public void AzureTableStorage_ConvertToFromStorageFormat()
        {
            TestStoreGrainState initialState = new TestStoreGrainState { A = "1", B = 2, C = 3 };
            GrainStateEntity entity = new GrainStateEntity();
            var storage = new AzureTableStorage();
            var logger = Logger.GetLogger("PersistenceProviderTests");
            storage.InitLogger(logger);
            storage.ConvertToStorageFormat(initialState, entity);
            Assert.IsNotNull(entity.Data, "Entity.Data");
            TestStoreGrainState convertedState = new TestStoreGrainState();
            storage.ConvertFromStorageFormat(convertedState, entity);
            Assert.IsNotNull(convertedState, "Converted state");
            Assert.AreEqual(initialState.A, convertedState.A, "A");
            Assert.AreEqual(initialState.B, convertedState.B, "B");
            Assert.AreEqual(initialState.C, convertedState.C, "C");
        }

        [TestMethod, TestCategory("Persistence"), TestCategory("Performance"), TestCategory("Json")]
        public void Json_Perf_Newtonsoft_vs_Net()
        {
            int numIterations = 10000;

            Dictionary<string, object> dataValues = new Dictionary<string, object>();
            var dotnetJsonSerializer = new System.Web.Script.Serialization.JavaScriptSerializer();
            string jsonData;
            int[] idx = { 0 };
            TimeSpan baseline = UnitTestBase.TimeRun(numIterations, TimeSpan.Zero, ".Net JavaScriptSerializer",
            () =>
            {
                dataValues.Clear();
                dataValues.Add("A", idx[0]++);
                dataValues.Add("B", idx[0]++);
                dataValues.Add("C", idx[0]++);
                jsonData = dotnetJsonSerializer.Serialize(dataValues);
            });
            idx[0] = 0;
            TimeSpan elapsed = UnitTestBase.TimeRun(numIterations, baseline, "Newtonsoft Json JavaScriptSerializer",
            () =>
            {
                dataValues.Clear();
                dataValues.Add("A", idx[0]++);
                dataValues.Add("B", idx[0]++);
                dataValues.Add("C", idx[0]++);
                jsonData = Newtonsoft.Json.JsonConvert.SerializeObject(dataValues);
            });
        }

        // Utility functions

        private static async Task Test_PersistenceProvider_Read(string grainTypeName, IStorageProvider store, IStorageProviderManager storageProviderManager)
        {
            GrainReference reference = GrainReference.FromGrainId(GrainId.NewId());
            TestStoreGrainState state = new TestStoreGrainState();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            TestStoreGrainState storedState = new TestStoreGrainState();
            await store.ReadStateAsync(grainTypeName, reference, storedState);
            TimeSpan readTime = sw.Elapsed;
            Console.WriteLine("{0} - Read time = {1}", store.GetType().FullName, readTime);
            Assert.AreEqual(state.A, storedState.A, "A");
            Assert.AreEqual(state.B, storedState.B, "B");
            Assert.AreEqual(state.C, storedState.C, "C");
        }

        private static async Task Test_PersistenceProvider_WriteRead(string grainTypeName, IStorageProvider store, IStorageProviderManager storageProviderManager)
        {
            GrainReference reference = GrainReference.FromGrainId(GrainId.NewId());
            TestStoreGrainState state = TestStoreGrainState.NewRandomState();
            Stopwatch sw = new Stopwatch();
            sw.Start();
            await store.WriteStateAsync(grainTypeName, reference, state);
            TimeSpan writeTime = sw.Elapsed;
            sw.Restart();
            TestStoreGrainState storedState = new TestStoreGrainState();
            await store.ReadStateAsync(grainTypeName, reference, storedState);
            TimeSpan readTime = sw.Elapsed;
            Console.WriteLine("{0} - Write time = {1} Read time = {2}", store.GetType().FullName, writeTime, readTime);
            Assert.AreEqual(state.A, storedState.A, "A");
            Assert.AreEqual(state.B, storedState.B, "B");
            Assert.AreEqual(state.C, storedState.C, "C");
        }

        private async Task<ShardedStorageProvider> ConfigureShardedStorageProvider(string name, StorageProviderManager storageProviderManager)
        {
            var composite = new ShardedStorageProvider();
            var provider1 = (IStorageProvider)storageProviderManager.GetProvider("Store1");
            var provider2 = (IStorageProvider)storageProviderManager.GetProvider("Store2");
            List<IOrleansProvider> providers = new List<IOrleansProvider>();
            providers.Add(provider1);
            providers.Add(provider2);
            await composite.Init(name, storageProviderManager, new ProviderConfiguration(null, providers));
            return composite;
        }
    }

    public class TestStoreGrainState : GrainState
    {
        private static readonly Random random = new Random();

        public string A { get; set; }
        public int B { get; set; }
        public long C { get; set; }

        public TestStoreGrainState()
            : base("System.Object")
        {
        }

        public static TestStoreGrainState NewRandomState()
        {
            return new TestStoreGrainState
            {
                A = random.Next().ToString(CultureInfo.InvariantCulture),
                B = random.Next(),
                C = random.Next()
            };
        }

        public override Dictionary<string, object> AsDictionary()
        {
            Dictionary<string, object> values = new Dictionary<string, object>();
            values["A"] = this.A;
            values["B"] = this.B;
            values["C"] = this.C;
            return values;
        }

        public override void SetAll(Dictionary<string, object> values)
        {
            object value;
            if (values.TryGetValue("A", out value)) A = (string)value;
            //if (values.TryGetValue("B", out value)) B = (int)value;
            if (values.TryGetValue("B", out value)) B = value is Int64 ? (int)(long)value : (int)value;
            if (values.TryGetValue("C", out value)) { C = value is Int32 ? (int)value : (long)value; }
        }
    }
}
