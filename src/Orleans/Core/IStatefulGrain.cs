
using System.Threading.Tasks;
using Orleans.Storage;

namespace Orleans
{
    internal interface IStatefulGrain
    {
        void SetStorageProvider(IStorageProvider storageProvider);
        Task InitializeState();
    }
}
