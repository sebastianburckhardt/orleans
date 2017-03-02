
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public class TMProxyDirectoryGrain : Grain, ITMProxyDirectoryGrain
    {
        private ITransactionManagerProxy reference;

        public Task SetReference(ITransactionManagerProxy proxyReference)
        {
            this.reference = proxyReference;
            return TaskDone.Done;
        }

        public Task<ITransactionManagerProxy> GetReference()
        {
            return Task.FromResult(reference);
        }
    }
}
