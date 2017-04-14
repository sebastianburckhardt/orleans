using Orleans.Runtime.Configuration;
using System;
using System.Xml;

namespace Orleans.Transactions
{
    [Serializable]
    public class TransactionsConfiguration
    {
        /// <summary>
        /// The LogStorageType value controls the persistent storage used for the transaction log. This value is resolved from the LogStorageTypeName attribute if XML configuration is used.
        /// </summary>
        public Type LogStorageType { get; set; }

        /// <summary>
        /// The TransactionManager value controls the type of the TransactionManager will be used. This value is resolved from the TransactionManagerTypeName attribute if XML configuration is used.
        /// This value must be in pair with TransactionServiceFactoryType attribute.
        /// </summary>
        public Type TransactionManagerType { get; set; }

        /// <summary>
        /// The TransactionServiceFactoryType value controls the type of the TransactionServiceFactoryTypeName will be used. This value is resolved from the TransactionServiceFactoryTypeName attribute if XML configuration is used.
        /// This value must be in pair with TransactionManagerType attribute.
        /// </summary>
        public Type TransactionServiceFactoryType { get; set; }

        /// <summary>
        /// The number of new Transaction Ids allocated on every write to the log.
        /// To avoid writing to log on every transaction start, transaction Ids are allocated in batches.
        /// </summary>
        public int TransactionIdAllocationBatchSize { get; set; }

        /// <summary>
        /// A new batch of transaction Ids will be automatically allocated if the available ids drop below
        /// this threshold.
        /// </summary>
        public int AvailableTransactionIdThreshold { get; set; }

        /// <summary>
        /// The number of TM Proxies. TM Proxy is the client-addressable object used by the Transaction
        /// Agents to communicate with the Transaction Manager.
        /// </summary>
        public int TransactionManagerProxyCount { get; set; }

        /// <summary>
        /// How long to preserve a transaction record in the TM memory after the transaction has completed.
        /// This is used to answer queries about the outcome of the transaction.
        /// </summary>
        public TimeSpan TransactionRecordPreservationDuration { get; set; }

        /// <summary>
        /// Provides connection string for an external table based transaction log storage.
        /// </summary>
        public string LogConnectionString { get; set; }

        /// <summary>
        /// Provides name of the table for an external table based transaction log storage.
        /// </summary>
        public string LogTableName { get; set; }

        /// <summary>
        /// TransactionsConfiguration constructor.
        /// </summary>
        public TransactionsConfiguration()
        {
            TransactionManagerProxyCount = 1;
            TransactionIdAllocationBatchSize = 50000;
            AvailableTransactionIdThreshold = 20000;
            TransactionRecordPreservationDuration = TimeSpan.FromMinutes(1);
        }

        /// <summary>
        /// Load this configuration from xml element.
        /// </summary>
        /// <param name="child"></param>
        public void Load(XmlElement child)
        {
            if (child.HasAttribute("LogStorageTypeName"))
            {
                var logStorageTypeName = child.GetAttribute("LogStorageTypeName");
                this.LogStorageType = ResolveType(logStorageTypeName, nameof(logStorageTypeName));
            }

            if (child.HasAttribute("TransactionManagerTypeName"))
            {
                var transactionManagerTypeName = child.GetAttribute("TransactionManagerTypeName");
                this.TransactionManagerType = ResolveType(transactionManagerTypeName, nameof(transactionManagerTypeName));
            }

            if (child.HasAttribute("TransactionServiceFactoryTypeName"))
            {
                var transactionServiceFactoryTypeName = child.GetAttribute("TransactionServiceFactoryTypeName");
                this.TransactionServiceFactoryType = ResolveType(transactionServiceFactoryTypeName, nameof(transactionServiceFactoryTypeName));
            }

            if (child.HasAttribute("TransactionIdAllocationBatchSize"))
            {
                this.TransactionIdAllocationBatchSize = ConfigUtilities.ParseInt(child.GetAttribute("TransactionIdAllocationBatchSize"),
                    "Invalid integer value for the TransactionIdAllocationBatchSize element");
            }

            if (child.HasAttribute("AvailableTransactionIdThreshold"))
            {
                this.AvailableTransactionIdThreshold = ConfigUtilities.ParseInt(child.GetAttribute("AvailableTransactionIdThreshold"),
                    "Invalid integer value for the AvailableTransactionIdThreshold element");
            }

            if (child.HasAttribute("TransactionManagerProxyCount"))
            {
                this.TransactionManagerProxyCount = ConfigUtilities.ParseInt(child.GetAttribute("TransactionManagerProxyCount"),
                    "Invalid integer value for the TransactionManagerProxyCount element");
            }

            if (child.HasAttribute("TransactionRecordPreservationDuration"))
            {
                this.TransactionRecordPreservationDuration = ConfigUtilities.ParseTimeSpan(child.GetAttribute("TransactionRecordPreservationDuration"),
                    "Invalid TimeSpan value for the TransactionRecordPreservationDuration element");
            }

            if (child.HasAttribute("LogConnectionString"))
            {
                this.LogConnectionString = child.GetAttribute("LogConnectionString");
            }

            if (child.HasAttribute("LogTableName"))
            {
                this.LogTableName = child.GetAttribute("LogTableName");
            }

        }

        private static Type ResolveType(string typeName, string configurationValueName)
        {
            Type resolvedType = null;

            if (!string.IsNullOrWhiteSpace(typeName))
            {
                resolvedType = Type.GetType(typeName);

                if (resolvedType == null)
                {
                    throw new InvalidOperationException($"Cannot locate the type specified in the configuration file for {configurationValueName}: '{typeName}'.");
                }

                if (!resolvedType.IsClass || resolvedType.IsAbstract)
                {
                    throw new InvalidOperationException($"{resolvedType} is either not a class or an abstract class.");
                }
            }

            return resolvedType;
        }
    }
}
