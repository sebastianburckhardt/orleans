using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace ReplicatedEventSample.Interfaces
{
    public interface IEventGrain : IGrainWithIntegerKey
    {
        Task NewOutcome(string name, int rank);

        Task<List<string>> GetTopThree();

        Task<int> GetRank(string name);
    }

  
}
