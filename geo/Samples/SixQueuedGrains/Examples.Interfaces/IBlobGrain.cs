using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Examples.Interfaces
{
    public interface IBlobGrain : IGrain
    {
        Task<byte[]> Get();
        Task Set(byte[] value);
    }

}
