using Orleans;
using System;
using System.Threading.Tasks;

namespace Computation.Interfaces
{
    // The grain supports two operations, to read an array of bytes, or to write an array of bytes
    public interface IComputationGrain : Orleans.IGrain
    {
       Task<byte[]> Read(string reqId);
       Task Write(int pTime);

    }

}
