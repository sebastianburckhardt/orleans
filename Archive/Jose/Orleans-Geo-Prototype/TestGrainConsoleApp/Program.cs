using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using TestGrainInterface;

namespace TestGrainConsoleApp
{
    class DumbClient
    {
        public async Task Run(string cluster, int startId, int endId)
        {
            try
            {
                OrleansClient.Initialize(cluster);
                List<Task> toWait = new List<Task>();
                for (int i = startId; i < endId; ++i)
                {
                    ITestGrainInterface grainRef = TestGrainInterfaceFactory.GetGrain(i);
                    toWait.Add(grainRef.SayHelloAsync());
                }
                await Task.WhenAll(toWait);
                Console.WriteLine("Done " + startId + " to " + endId);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error calling HelloOrleans grain: " + exc);
            }
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var prog = new DumbClient();
            for (int j = 0; j < 1000; ++j)
            {
                Thread.Sleep(1000);
                prog.Run(args[0], j, j + 1).Wait();
            }
        }
    }
}
