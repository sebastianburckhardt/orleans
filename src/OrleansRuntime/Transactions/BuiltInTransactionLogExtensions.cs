
using Orleans.Runtime.Configuration;

namespace Orleans.Transactions
{
    /// <summary>
    /// Extension methods for configuring built in Transaction logs 
    /// </summary>
    public static class BuiltInTransactionLogExtensions
    {
        /// <summary>
        /// Configures the transaction manager to use <see cref="MemoryTransactionLogStorage"/>.
        /// </summary>
        /// <param name="config">The cluster configuration.</param>
        public static void UseMemoryTransactionLog(
            this ClusterConfiguration config)
        {
            config.Globals.Transactions.LogStorageTypeName = typeof(MemoryTransactionLogStorage).AssemblyQualifiedName;
        }

        /// <summary>
        /// Configures the transaction manager to use <see cref="MemoryTransactionLogStorage"/>.
        /// </summary>
        /// <param name="config">The client configuration.</param>
        public static void UseMemoryTransactionLog(
            this ClientConfiguration config)
        {
            config.Transactions.LogStorageTypeName = typeof(MemoryTransactionLogStorage).AssemblyQualifiedName;
        }
    }
}
