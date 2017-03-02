
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public interface ITransactionManagerProxy : ITransactionManagerService, IGrainWithIntegerKey
    {
    }

    public interface ITMProxyDirectoryGrain : IGrainWithIntegerKey
    {
        Task<ITransactionManagerProxy> GetReference();
        Task SetReference(ITransactionManagerProxy reference);
    }
}
