using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrains;

namespace UnitTests
{
    [TestClass]
    public class MasterWorkersTests : UnitTestBase
    {
        private static int timeout = Debugger.IsAttached ? 300 * 1000 : 300 * 1000;
        private IMasterGrain masterGrain;
        private List<IWorkerGrain> workerGrains;
        private IAggregatorGrain aggregatorGrain;

        private int NumSilos = 1;
        private int NumWorkers = 2;
        private int NumItemsPerWorker = 100;
        private int ItemLenght = 5000;

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        public static void RunTest(string[] args)
        {
            int numSilos = int.Parse(args[0]);
            int numWorkers = int.Parse(args[1]);
            int numItemsPerWorker = int.Parse(args[2]);
            int itemLenght = int.Parse(args[3]);

            MasterWorkersTests tests = new MasterWorkersTests(numSilos, numWorkers, numItemsPerWorker, itemLenght);
            tests.MasterWorkersTests_1();
        }

        public MasterWorkersTests(int numSilos, int numWorkers, int numItemsPerWorker, int itemLenght)
            : base(new Options { StartPrimary = numSilos > 0, StartSecondary = numSilos > 1, StartClient = numSilos > 0 })
        {
            NumSilos = numSilos;
            NumWorkers = numWorkers;
            NumItemsPerWorker = numItemsPerWorker;
            ItemLenght = itemLenght;
            if (numSilos==0)
            {
                OrleansClient.Initialize();
            }
        }

        private void StartGrains()
        {
            // create all grains and wait to make sure all created.
            masterGrain = MasterGrainFactory.GetGrain(GetRandomGrainId());
            aggregatorGrain = AggregatorGrainFactory.GetGrain(GetRandomGrainId());
            workerGrains = new List<IWorkerGrain>();
            for (int i = 0; i < NumWorkers; i++)
            {
                IWorkerGrain worker = WorkerGrainFactory.GetGrain(GetRandomGrainId());
                workerGrains.Add(worker);
            }
            List<IAddressable> grains = new List<IAddressable>();
            grains.AddRange(workerGrains);
            grains.Add(masterGrain);
            grains.Add(aggregatorGrain);
            AsyncCompletion.JoinAll(grains).Wait();

            // now initialize grains.
            List<Task> promises = new List<Task>();
            promises.Add(masterGrain.Initialize(workerGrains.ToArray()));
            for (int i = 0; i < workerGrains.Count; i++)
            {
                promises.Add(workerGrains[i].Initialize(aggregatorGrain));
            }
            promises.Add(aggregatorGrain.Initialize(NumWorkers));
            Task.WhenAll(promises).Wait();
        }


        public void MasterWorkersTests_1()
        {
            Console.WriteLine(String.Format("MasterWorkersTests_1, numWorkers={0}, numItems={1}", NumWorkers, NumItemsPerWorker));
            
            StartGrains();

            Console.WriteLine(String.Format("MasterWorkersTests_1 Done creating grains"));

            Stopwatch s = new Stopwatch();
            s.Start();
            AsyncCompletion workPromise = AsyncCompletion.FromTask(masterGrain.DoWork(NumItemsPerWorker, ItemLenght));
            workPromise.ContinueWith(() =>
                {
                    Console.WriteLine(String.Format("workPromise was resolved."));
                });

            AsyncValue<List<double>> resultPromise = AsyncValue.FromTask(aggregatorGrain.GetResults());
            resultPromise.ContinueWith(() =>
                {
                    s.Stop();
                    Console.WriteLine(String.Format("resultPromise was resolved."));
                }).Wait();

            double timeActual = s.Elapsed.TotalSeconds;
            Console.WriteLine(String.Format("MasterWorkersTests_1 took {0:F2} seconds to do all work.", timeActual));
        }
    }
}