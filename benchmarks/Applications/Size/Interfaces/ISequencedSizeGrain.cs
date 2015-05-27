using Orleans;
using System;
using System.Threading.Tasks;

namespace Size.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ISequencedSizeGrain : Orleans.IGrain
    {
       Task WriteNow(byte[] payload);
       Task WriteLater(byte[] payload);

       Task<byte[]> ReadApprox(string reqId);
       Task<byte[]> ReadCurrent(string reqId);


    }

}
