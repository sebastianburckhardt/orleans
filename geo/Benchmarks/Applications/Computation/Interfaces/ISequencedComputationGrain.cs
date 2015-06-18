using Orleans;
using System;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks.Computation.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ISequencedComputationGrain : Orleans.IGrain
    {
        Task WriteNow(int pTime);
        Task WriteLater(int pTime);

        Task<byte[]> ReadApprox(string reqId);
        Task<byte[]> ReadCurrent(string reqId);


    }

}
