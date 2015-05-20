using System;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.UnitTest.Unsigned.GrainInterfaces;

namespace Orleans.UnitTest.Unsigned.Grains
{
    /// <summary>
    /// Orleans grain implementation class for IUnsignedEchoGrain.
    /// </summary>
    [Serializable]
    public class UnsignedEchoGrain : Orleans.GrainBase, IUnsignedEchoGrain
    {
        public Task<string> Echo(string str)
        {
            return Task.FromResult(str);
        }
    }

    /// <summary>
    /// Orleans grain implementation class for IUnsignedRefEchoTypeGrain.
    /// </summary>
    [Serializable]
    public class UnsignedRefTypeEchoGrain : Orleans.GrainBase, IUnsignedRefTypeEchoGrain
    {
        public Task<TableServiceEntity> Echo(TableServiceEntity entry)
        {
            return Task.FromResult(entry);
        }
    }
}
