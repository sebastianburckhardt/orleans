using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans;

namespace Orleans.UnitTest.Unsigned.GrainInterfaces
{
    public interface IUnsignedEchoGrain : Orleans.IAddressable
    {
        Task<string> Echo(string name);
    }

    public interface IUnsignedRefTypeEchoGrain : Orleans.IAddressable
    {
        Task<TableServiceEntity> Echo(TableServiceEntity entry);
    }
}
