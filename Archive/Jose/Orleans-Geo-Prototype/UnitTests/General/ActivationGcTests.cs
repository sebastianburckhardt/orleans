using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Orleans;
using Orleans.Counters;
using Orleans.Management;

using UnitTestGrainInterfaces;

namespace UnitTests.General
{
    // todo: rename to ActivationCollectorTests (kept ActivationGcTests to make code review easier).
    [TestClass]
    public class ActivationGcTests : UnitTestBase
    {
        static private readonly TimeSpan DEFAULT_COLLECTION_QUANTUM = TimeSpan.FromSeconds(10);
        static private readonly TimeSpan DEFAULT_IDLE_TIMEOUT = DEFAULT_COLLECTION_QUANTUM;
        static private readonly TimeSpan WAIT_TIME = DEFAULT_IDLE_TIMEOUT.Multiply(3.0);

        [ClassCleanup]
        public static void ClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        private async Task<int> GetActivationCount(string fullTypeName)
        {
            int result = 0;

            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
            SimpleGrainStatistic[] stats = await mgmtGrain.GetSimpleGrainStatistics();
            foreach (var stat in stats)
            {
                if (stat.GrainType == fullTypeName)
                    result += stat.ActivationCount;
            }
            return result;
        }

        private void Initialize(TimeSpan collectionAgeLimit, TimeSpan quantum)
        {
            ResetDefaultRuntimes();
            UnitTestBase.Initialize(new Options { StartFreshOrleans = true, StartPrimary = true, StartSecondary = true, DefaultCollectionAgeLimit = collectionAgeLimit, CollectionQuantum = quantum});
        }

        private void Initialize(TimeSpan collectionAgeLimit)
        {
            Initialize(collectionAgeLimit, collectionAgeLimit);
        }

        private void Initialize()
        {
            Initialize(TimeSpan.Zero, DEFAULT_COLLECTION_QUANTUM);
        }

        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ActivationCollectorShouldCollectIdleActivations()
        {
            Initialize(DEFAULT_IDLE_TIMEOUT);

            const int grainCount = 1000;
            const string fullGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain1";

            List<Task> tasks = new List<Task>();
            logger.Info("IdleActivationCollectorShouldCollectIdleActivations: activating {0} grains.", grainCount);
            for (var i = 0; i < grainCount; ++i)
            {
                IIdleActivationGcTestGrain1 g = IdleActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                tasks.Add(g.Nop());
            }
            await Task.WhenAll(tasks);

            int activationsCreated = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(grainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", grainCount, activationsCreated));

            logger.Info("IdleActivationCollectorShouldCollectIdleActivations: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            int activationsNotCollected = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(0, activationsNotCollected, string.Format("{0} activations should have been collected", activationsNotCollected));
        }   

        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ActivationCollectorShouldNotCollectBusyActivations()
        {
            Initialize(DEFAULT_IDLE_TIMEOUT);

            const int idleGrainCount = 500;
            const int busyGrainCount = 500;
            const string idleGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain1";
            const string busyGrainTypeName = "UnitTestGrains.BusyActivationGcTestGrain1";

            List<Task> tasks0 = new List<Task>();
            List<IBusyActivationGcTestGrain1> busyGrains = new List<IBusyActivationGcTestGrain1>();
            logger.Info("ActivationCollectorShouldNotCollectBusyActivations: activating {0} busy grains.", busyGrainCount);
            for (var i = 0; i < busyGrainCount; ++i)
            {
                IBusyActivationGcTestGrain1 g = BusyActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                busyGrains.Add(g);
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);
            bool[] quit = new bool[]{ false };
            Func<Task> busyWorker =
                async () =>
                {
                    logger.Info("ActivationCollectorShouldNotCollectBusyActivations: busyWorker started");
                    List<Task> tasks1 = new List<Task>();
                    while (!quit[0])
                    {
                        foreach (var g in busyGrains)
                            tasks1.Add(g.Nop());
                        await Task.WhenAll(tasks1);
                    }
                };
            Task.Run(busyWorker).Ignore();

            logger.Info("ActivationCollectorShouldNotCollectBusyActivations: activating {0} idle grains.", idleGrainCount);
            tasks0.Clear();
            for (var i = 0; i < idleGrainCount; ++i)
            {
                IIdleActivationGcTestGrain1 g = IdleActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);

            int activationsCreated = await GetActivationCount(idleGrainTypeName) + await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(idleGrainCount + busyGrainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", idleGrainCount + busyGrainCount, activationsCreated));

            logger.Info("ActivationCollectorShouldNotCollectBusyActivations: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            // we should have only collected grains from the idle category (IdleActivationGcTestGrain1).
            int idleActivationsNotCollected = await GetActivationCount(idleGrainTypeName);
            int busyActivationsNotCollected = await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(0, idleActivationsNotCollected, string.Format("{0} idle activations should have been collected", idleActivationsNotCollected));
            Assert.AreEqual(busyGrainCount, busyActivationsNotCollected, string.Format("{0} busy activations should not have been collected", busyActivationsNotCollected));

            quit[0] = true;
        }          
        
        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ManualCollectionShouldNotCollectBusyActivations()
        {
            Initialize(DEFAULT_IDLE_TIMEOUT);

            TimeSpan shortIdleTimeout = TimeSpan.FromSeconds(1);
            const int idleGrainCount = 500;
            const int busyGrainCount = 500;
            const string idleGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain1";
            const string busyGrainTypeName = "UnitTestGrains.BusyActivationGcTestGrain1";

            List<Task> tasks0 = new List<Task>();
            List<IBusyActivationGcTestGrain1> busyGrains = new List<IBusyActivationGcTestGrain1>();
            logger.Info("ManualCollectionShouldNotCollectBusyActivations: activating {0} busy grains.", busyGrainCount);
            for (var i = 0; i < busyGrainCount; ++i)
            {
                IBusyActivationGcTestGrain1 g = BusyActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                busyGrains.Add(g);
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);
            bool[] quit = new bool[]{ false };
            Func<Task> busyWorker =
                async () =>
                {
                    logger.Info("ManualCollectionShouldNotCollectBusyActivations: busyWorker started");
                    List<Task> tasks1 = new List<Task>();
                    while (!quit[0])
                    {
                        foreach (var g in busyGrains)
                            tasks1.Add(g.Nop());
                        await Task.WhenAll(tasks1);
                    }
                };
            Task.Run(busyWorker).Ignore();

            logger.Info("ManualCollectionShouldNotCollectBusyActivations: activating {0} idle grains.", idleGrainCount);
            tasks0.Clear();
            for (var i = 0; i < idleGrainCount; ++i)
            {
                IIdleActivationGcTestGrain1 g = IdleActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);

            int activationsCreated = await GetActivationCount(idleGrainTypeName) + await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(idleGrainCount + busyGrainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", idleGrainCount + busyGrainCount, activationsCreated));

            logger.Info("ManualCollectionShouldNotCollectBusyActivations: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", shortIdleTimeout.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(shortIdleTimeout);

            TimeSpan everything = TimeSpan.FromMinutes(10);
            logger.Info("ManualCollectionShouldNotCollectBusyActivations: triggering manual collection (timespan is {0} sec).",  everything.TotalSeconds);
            IOrleansManagementGrain mgmtGrain = OrleansManagementGrainFactory.GetGrain(RuntimeInterfaceConstants.SystemManagementId);
            await mgmtGrain.ForceActivationCollection(everything);
            

            logger.Info("ManualCollectionShouldNotCollectBusyActivations: waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            // we should have only collected grains from the idle category (IdleActivationGcTestGrain).
            int idleActivationsNotCollected = await GetActivationCount(idleGrainTypeName);
            int busyActivationsNotCollected = await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(0, idleActivationsNotCollected, string.Format("{0} idle activations should have been collected", idleActivationsNotCollected));
            Assert.AreEqual(busyGrainCount, busyActivationsNotCollected, string.Format("{0} busy activations should not have been collected", busyActivationsNotCollected));

            quit[0] = true;
        }    
        
        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ActivationCollectorShouldNotCollectIdleActivationsIfDisabled()
        {
            Initialize();

            const int grainCount = 1000;
            const string fullGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain1";

            List<Task> tasks = new List<Task>();
            logger.Info("ActivationCollectorShouldNotCollectIdleActivationsIfDisabled: activating {0} grains.", grainCount);
            for (var i = 0; i < grainCount; ++i)
            {
                IIdleActivationGcTestGrain1 g = IdleActivationGcTestGrain1Factory.GetGrain(Guid.NewGuid());
                tasks.Add(g.Nop());
            }
            await Task.WhenAll(tasks);

            int activationsCreated = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(grainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", grainCount, activationsCreated));

            logger.Info("ActivationCollectorShouldNotCollectIdleActivationsIfDisabled: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            int activationsNotCollected = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(1000, activationsNotCollected, "0 activations should have been collected");
        }   
        
        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ActivationCollectorShouldCollectIdleActivationsSpecifiedInPerTypeConfiguration()
        {
            Initialize();

            const int grainCount = 1000;
            const string fullGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain2";

            List<Task> tasks = new List<Task>();
            logger.Info("ActivationCollectorShouldCollectIdleActivationsSpecifiedInPerTypeConfiguration: activating {0} grains.", grainCount);
            for (var i = 0; i < grainCount; ++i)
            {
                IIdleActivationGcTestGrain2 g = IdleActivationGcTestGrain2Factory.GetGrain(Guid.NewGuid());
                tasks.Add(g.Nop());
            }
            await Task.WhenAll(tasks);

            int activationsCreated = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(grainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", grainCount, activationsCreated));

            logger.Info("ActivationCollectorShouldCollectIdleActivationsSpecifiedInPerTypeConfiguration: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            int activationsNotCollected = await GetActivationCount(fullGrainTypeName);
            Assert.AreEqual(0, activationsNotCollected, string.Format("{0} activations should have been collected", activationsNotCollected));
        }   

        [TestMethod, TestCategory("ActivationCollector"), TestCategory("Nightly")]
        public async Task ActivationCollectorShouldNotCollectBusyActivationsSpecifiedInPerTypeConfiguration()
        {
            Initialize();

            const int idleGrainCount = 500;
            const int busyGrainCount = 500;
            const string idleGrainTypeName = "UnitTestGrains.IdleActivationGcTestGrain2";
            const string busyGrainTypeName = "UnitTestGrains.BusyActivationGcTestGrain2";

            List<Task> tasks0 = new List<Task>();
            List<IBusyActivationGcTestGrain2> busyGrains = new List<IBusyActivationGcTestGrain2>();
            logger.Info("ActivationCollectorShouldNotCollectBusyActivationsSpecifiedInPerTypeConfiguration: activating {0} busy grains.", busyGrainCount);
            for (var i = 0; i < busyGrainCount; ++i)
            {
                IBusyActivationGcTestGrain2 g = BusyActivationGcTestGrain2Factory.GetGrain(Guid.NewGuid());
                busyGrains.Add(g);
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);
            bool[] quit = new bool[]{ false };
            Func<Task> busyWorker =
                async () =>
                {
                    logger.Info("ActivationCollectorShouldNotCollectBusyActivationsSpecifiedInPerTypeConfiguration: busyWorker started");
                    List<Task> tasks1 = new List<Task>();
                    while (!quit[0])
                    {
                        foreach (var g in busyGrains)
                            tasks1.Add(g.Nop());
                        await Task.WhenAll(tasks1);
                    }
                };
            Task.Run(busyWorker).Ignore();

            logger.Info("ActivationCollectorShouldNotCollectBusyActivationsSpecifiedInPerTypeConfiguration: activating {0} idle grains.", idleGrainCount);
            tasks0.Clear();
            for (var i = 0; i < idleGrainCount; ++i)
            {
                IIdleActivationGcTestGrain2 g = IdleActivationGcTestGrain2Factory.GetGrain(Guid.NewGuid());
                tasks0.Add(g.Nop());
            }
            await Task.WhenAll(tasks0);

            int activationsCreated = await GetActivationCount(idleGrainTypeName) + await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(idleGrainCount + busyGrainCount, activationsCreated, string.Format("{0} activations should have been created; got {1} instead", idleGrainCount + busyGrainCount, activationsCreated));

            logger.Info("IdleActivationCollectorShouldNotCollectBusyActivations: grains activated; waiting {0} sec (activation GC idle timeout is {1} sec).", WAIT_TIME.TotalSeconds, DEFAULT_IDLE_TIMEOUT.TotalSeconds);
            await Task.Delay(WAIT_TIME);

            // we should have only collected grains from the idle category (IdleActivationGcTestGrain2).
            int idleActivationsNotCollected = await GetActivationCount(idleGrainTypeName);
            int busyActivationsNotCollected = await GetActivationCount(busyGrainTypeName);
            Assert.AreEqual(0, idleActivationsNotCollected, string.Format("{0} idle activations should have been collected", idleActivationsNotCollected));
            Assert.AreEqual(busyGrainCount, busyActivationsNotCollected, string.Format("{0} busy activations should not have been collected", busyActivationsNotCollected));

            quit[0] = true;
        }   
    }
}
