
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    internal class TransactionServiceGrainFactory : ITransactionServiceFactory
    {
        private readonly IGrainFactory grainFactory;

        public TransactionServiceGrainFactory(IGrainFactory grainFactory)
        {
            this.grainFactory = grainFactory;
        }

        public Task<ITransactionStartService> GetTransactionStartService()
        {
            return Task.FromResult<ITransactionStartService>(this.grainFactory.GetGrain<ITransactionManagerGrain>(0));
        }

        public Task<ITransactionCommitService> GetTransactionCommitService()
        {
            return Task.FromResult<ITransactionCommitService>(this.grainFactory.GetGrain<ITransactionManagerGrain>(0));
        }
    }
}
