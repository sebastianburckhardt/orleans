using Orleans;
using System;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks.Size.Interfaces
{
    // The grain supports two operations, to read an array of bytes, or to write an array of bytes
    public interface ISizeGrain : Orleans.IGrain
    {
        Task<Byte[]> Read(string reqId);
        Task Write(Byte[] payload);

    }

}
