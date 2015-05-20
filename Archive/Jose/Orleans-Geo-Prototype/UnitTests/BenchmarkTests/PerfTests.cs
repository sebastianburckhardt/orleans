using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;

using UnitTestGrains;
using UnitTestGrainInterfaces;

namespace UnitTests
{
    /// <summary>
    /// Summary description for BenchmarkTests
    /// </summary>
    [TestClass]
    public class PerfTests : UnitTestBase
    {
        private static int timeout = Debugger.IsAttached ? 300 * 1000 : 300 * 1000;
        //private static IBenchmarkGrain benchmarkGrainOne;
        //private static IBenchmarkGrain benchmarkGrainTwo;
        private int NumSilos = 1;
        private static int NumIterations = -1;

        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        public PerfTests()
            : base(new Options
            {
                StartFreshOrleans = true
            })
        {
        }

        public PerfTests(bool remoteSilos, int numSilos, int numIterations)
            : base (new Options
            {
                StartFreshOrleans = !remoteSilos,
                StartPrimary = !remoteSilos,
                StartSecondary = !remoteSilos && numSilos > 1,
                StartClient = !remoteSilos
            })
        {
            NumSilos = numSilos;
            NumIterations = numIterations;
            if (remoteSilos)
            {
                OrleansClient.Initialize();
            }
        }

        private string GetParams() 
        {
            return String.Format("{0} silos, {1} iterations", NumSilos, NumIterations);
        }

        private static void InitializeBenchmarkGrains(int numSilos)
        {
            //benchmarkGrainOne = BenchmarkGrainFactory.CreateGrain(
            //    Name: Guid.NewGuid().ToString(),
            //    Strategies: new[] { GrainStrategy.PartitionPlacement(0, 2) });

            //benchmarkGrainTwo = BenchmarkGrainFactory.CreateGrain(
            //    Name: Guid.NewGuid().ToString(),
            //    Strategies: new[] { GrainStrategy.PartitionPlacement((numSilos==1) ? 0 : 1, 2) });

            //benchmarkGrainOne.Wait();
            //benchmarkGrainTwo.Wait();
        }

        public void Perf_ExchangeMessage(int dataLength)
        {
            //Console.WriteLine("\n\nPerf_ExchangeMessage with {0}, dataLength={1}\n\n", GetParams(), dataLength);

            //InitializeBenchmarkGrains(NumSilos);
            //List<TimeSpan> timeSpans = benchmarkGrainOne.ExchangeMessage(benchmarkGrainTwo, dataLength, NumIterations).Result;

            //List<double> spans = timeSpans.Select(span => span.TotalMilliseconds).ToList();
            //double timeAverage = spans.Sum(span => span) / NumIterations;
            //spans.Sort();
            //double min05 = spans[(int)(NumIterations * 0.05)];
            //double max95 = spans[(int)(NumIterations * 0.95)];
            //Console.WriteLine("\n\nPerf_ExchangeMessage took {0:F3} ms on average. min05={1}, max95={2}\n\n", timeAverage, min05, max95);
        }

        public void Perf_CreateGrains(int numGrains, bool selfManaged)
        {
            Console.WriteLine("\n\nPerf_CreateGrains with {0}, {1} grains, selfManaged={2}.\n\n", GetParams(), numGrains, selfManaged);

            Stopwatch s = new Stopwatch();
            for (int i = 0; i < NumIterations; i++)
            {
                List<IAddressable> grains = new List<IAddressable>();
                List<AsyncCompletion> promises = new List<AsyncCompletion>();
                s.Start();
                for (int j = 0; j < numGrains; j++)
                {
                    if (selfManaged)
                    {
                        ISimpleSelfManagedGrain grain = SimpleSelfManagedGrainFactory.GetGrain(j+1);
                        AsyncCompletion promise = AsyncCompletion.FromTask(grain.GetLabel());
                        promises.Add(promise);
                    }
                    else
                    {
                        IBenchmarkGrain benchmarkGrain = BenchmarkGrainFactory.GetGrain(Guid.NewGuid());
                        grains.Add(benchmarkGrain);
                    }
                }
                AsyncCompletion joinedPromise = null;
                if (selfManaged)
                {
                    joinedPromise = AsyncCompletion.JoinAll(promises.ToArray());
                }
                joinedPromise.ContinueWith(() =>
                    {
                        s.Stop();
                    }).Wait();
            }

            double timeActual = s.Elapsed.TotalMilliseconds / NumIterations;
            Console.WriteLine("\n\nPerf_CreateGrains took {0:F3} ms on average.\n\n", timeActual);
        }

        public void Perf_CreateGrainsBatch(int numGrains)
        {
            //Console.WriteLine("\n\nPerf_CreateGrainsBatch with {0} {1} grains.\n\n", GetParams(), numGrains);

            //Stopwatch s = new Stopwatch();
            //for (int i = 0; i < NumIterations; i++)
            //{
            //    List<BenchmarkGrainProperties> grainStates = new List<BenchmarkGrainProperties>();
            //    for (int j = 0; j < numGrains; j++)
            //    {
            //        BenchmarkGrainProperties state = new BenchmarkGrainProperties();
            //        state.Name = Guid.NewGuid().ToString();
            //        grainStates.Add(state);
            //    }

            //    s.Start();
            //    AsyncValue<IBenchmarkGrain[]> grains = BenchmarkGrainFactory.CreateMany(new BenchmarkGrainProperties(), grainStates);
            //    grains.ContinueWith(() =>
            //    {
            //        s.Stop();
            //    }).Wait();
            //}

            //double timeActual = s.Elapsed.TotalMilliseconds / NumIterations;
            //Console.WriteLine("\n\nPerf_CreateGrainsBatch took {0:F3} ms on average.\n\n", timeActual);
        }

        public void Perf_PromiseOverhead(int dataLength)
        {
            //Console.WriteLine("\n\nPerf_PromiseOverhead with {0}, dataLength={1}\n\n", GetParams(), dataLength);

            //InitializeBenchmarkGrains(NumSilos);
            //TimeSpan syncTimeSpan = benchmarkGrainOne.PromiseOverhead(NumIterations, dataLength, false).Result;
            //TimeSpan asyncTimeSpan = benchmarkGrainOne.PromiseOverhead(NumIterations, dataLength, true).Result;

            //double syncTimeActual = syncTimeSpan.TotalMilliseconds / NumIterations;
            //double asyncTimeActual = asyncTimeSpan.TotalMilliseconds / NumIterations;
            //Console.WriteLine("\n\nPerf_PromiseOverhead SYNC took {0:F3} ms on average.\n\n", syncTimeActual);
            //Console.WriteLine("\n\nPerf_PromiseOverhead A-SYNC took {0:F3} ms on average.\n\n", asyncTimeActual);

            //double overhead = (asyncTimeActual - syncTimeActual);
            //Console.WriteLine("\n\nPerf_PromiseOverhead OVERHEAD is {0:F3} ms on average.\n\n\n\n", overhead);

        }

        private static void RunConcurrentBenchmark(bool write, int numActivations, int numSilos, int numClients, int numMessagesPerClient, int delay)
        {
            //int numDropMessages = Math.Min(NumIterations/2, 5);

            //IBenchmarkGrain target = BenchmarkGrainFactory.CreateGrain(
            //    Name: Guid.NewGuid().ToString(),
            //    DummyDelay: delay,
            //    Strategies: new[] { GrainStrategy.PartitionPlacement(2, 3, numActivations) });
            //IBenchmarkGrain[] clients = Enumerable.Range(0, numClients).Select(_ =>
            //    BenchmarkGrainFactory.CreateGrain(
            //        Name: Guid.NewGuid().ToString(),
            //        Other: target,
            //        Strategies: new[] { GrainStrategy.PartitionPlacement(1, 3, numActivations) })).ToArray();
            //AsyncCompletion.JoinAll(clients).Wait();

            //Stopwatch s = new Stopwatch();

            //for (int i = 0; i < numDropMessages + NumIterations; i++)
            //{
            //    if (i >= numDropMessages)
            //        s.Start();
            //    AsyncCompletion.JoinAll(clients.Select(client =>
            //            write ? client.WriteOther(numMessagesPerClient) : client.ReadOther(numMessagesPerClient)))
            //        .Wait();
            //    if (i >= numDropMessages)
            //        s.Stop();
            //    if (write) Thread.Sleep(5000);
            //}

            //double timeActual = s.Elapsed.TotalMilliseconds / (NumIterations * numClients * numMessagesPerClient);
            //Console.WriteLine(
            //    "\n\nPerf_Concurrent clients, messages, mode, delay, throughput: {0}\t{1}\t{2}\t{3}\t{4:F1}\n\n",
            //    numClients, numMessagesPerClient, write ? "write" : "read", delay, 1000 / timeActual);
        }

        [TestMethod, TestCategory("Revisit"), TestCategory("Performance")]
        public void Perf_Concurrent()
        {
            Perf_Concurrent("*", 5, 1, 10, 100, 1);
        }

        public static void Perf_Concurrent(string which, int numActivations, int numSilos, int numClients, int numMessagesPerClient, int delay)
        {
            if (which == "*") which = "rw";
            foreach (var test in which.ToLower())
            {
                RunConcurrentBenchmark(test == 'w', numActivations, numSilos, numClients, numMessagesPerClient, delay);
            }
        }
    }
}
