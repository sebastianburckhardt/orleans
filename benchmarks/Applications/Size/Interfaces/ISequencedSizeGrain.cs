using Orleans;
using System;
using System.Threading.Tasks;

namespace Size.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ISequencedSizeGrain : Orleans.IGrain
    {
        Task WriteNow(Byte[] payload);
        Task WriteLater(Byte[] payload);

        Task<Byte[]> ReadApprox(string reqId);
        Task<Byte[]> ReadCurrent(string reqId);


    }

}
