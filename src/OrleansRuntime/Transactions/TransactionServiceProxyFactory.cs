
using System;
using System.Threading.Tasks;
using Orleans.Runtime;
using Orleans.Runtime.Configuration;

namespace Orleans.Transactions
{
    internal class TransactionServiceProxyFactory : ITransactionServiceFactory
    {
        private static readonly IBackoffProvider BackoffPolicy = new ExponentialBackoff(TimeSpan.FromSeconds(1),
            TimeSpan.FromSeconds(20), TimeSpan.FromSeconds(1));

        private readonly Logger logger;
        private readonly IGrainFactory grainFactory;
        private readonly int startDirectoryGrainId;
        private readonly int commitDirectoryGrainId;

        public TransactionServiceProxyFactory(TransactionsConfiguration config, NodeConfiguration nodeConfig,
            IGrainFactory grainFactory)
        {
            logger = LogManager.GetLogger("TransactionServiceProxyFactory");
            this.grainFactory = grainFactory;
            startDirectoryGrainId = nodeConfig.SiloName.GetHashCode()% config.TransactionManagerProxyCount;
            commitDirectoryGrainId = (startDirectoryGrainId + 1)% config.TransactionManagerProxyCount;
        }

        public async Task<ITransactionStartService> GetTransactionStartService()
        {
            return await GetTMProxyWithRetry(startDirectoryGrainId);
        }

        public async Task<ITransactionCommitService> GetTransactionCommitService()
        {
            return await GetTMProxyWithRetry(commitDirectoryGrainId);
        }

        private async Task<ITransactionManagerService> GetTMProxy(int directoryGrainId)
        {
            ITMProxyDirectoryGrain directory = this.grainFactory.GetGrain<ITMProxyDirectoryGrain>(directoryGrainId);

            logger.Info(ErrorCode.Transactions_GetTMProxy, "Retrieving TM Proxy reference from directory");
            ITransactionManagerService tmProxy = await directory.GetReference();

            if (tmProxy == null)
            {
                throw new OrleansTransactionServiceNotAvailableException();
            }
            return tmProxy;
        }

        private Task<ITransactionManagerService> GetTMProxyWithRetry(int directoryGrainId)
        {
            return AsyncExecutorWithRetries.ExecuteWithRetries(
                i => GetTMProxy(directoryGrainId),
                AsyncExecutorWithRetries.INFINITE_RETRIES,
                (e, i) => true,
                Constants.INFINITE_TIMESPAN,
                BackoffPolicy);
        }
    }
}
