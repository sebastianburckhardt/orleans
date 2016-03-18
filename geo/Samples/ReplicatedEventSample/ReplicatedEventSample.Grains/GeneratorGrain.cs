using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ReplicatedEventSample.Interfaces;
using Orleans;
using Orleans.Core;

namespace ReplicatedEventSample.Grains
{
    class GeneratorGrain : Grain, IGeneratorGrain
    {
        public Task Start()
        {
            // keep generating for at least 10 minutes
            DelayDeactivation(TimeSpan.FromMinutes(10));

            if (!started)
            {
                started = true;
             
                // find event grain for this generator
                eventgrain = GrainFactory.GetGrain<IEventGrain>("event" + this.GetPrimaryKeyLong());

                
                RegisterTimer(Generate, null, 
                    TimeSpan.FromSeconds(random.Next(20)),  // start within 20 secs
                    TimeSpan.FromSeconds(2)); // one outcome about every two seconds
            }

            return TaskDone.Done;
        }

        bool started;
        Random random = new Random();
        IEventGrain eventgrain;

        private async Task Generate(Object ignoredparameter)
        {
            // wait 0-1 seconds
            await Task.Delay((int) (1000 * random.NextDouble()));

            // pick random name and score for outcome
            var outcome = new Outcome()
            {
               Name = ((char) ('A' + random.Next(26))).ToString(),
               Score = random.Next(100),
               When = DateTime.UtcNow
            };

            // notify event grain of new outcome
            await eventgrain.NewOutcome(outcome);
        }
    }
}
