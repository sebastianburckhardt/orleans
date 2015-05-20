using System.Threading.Tasks;
using Orleans;
using System.Collections.Generic;
using System;

namespace UnitTestGrains
{
    [StatelessWorker]
    public interface IStatelessWorkerGrain : IGrain
    {
        Task LongCall();
        Task<Tuple<Guid, List<Tuple<DateTime,DateTime>>>> GetCallStats();
    }
}