using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace Orleans.Transactions
{
    [Serializable]
    public class TransactionsConfiguration
    {
        /// <summary>
        /// Log configuration that controls the persistent storage used for the transaction log.
        /// </summary>
        public enum TransactionLogType
        {
            /// <summary>Store transaction log in memory. 
            /// This option is should only be used for testing because the log has to be durable.</summary>
            Memory,
            /// <summary>AzureTable is used to store transaction log. 
            /// This option should be used in production.</summary>
            AzureTable
        }

        /// <summary>
        /// Orleans framework supported transaction manager types
        /// </summary>
        public enum OrleansTransactionManagerType
        {
            /// <summary>
            /// Transaction manager hosted in a grain.  Viable for test, development, and services with low transactional performance requirements
            /// If not specificed Transaction Manager type will default to GrainBased
            /// </summary>
            GrainBased,
            /// <summary>
            /// Transaction manager hosted in an Orleans client service.
            /// </summary>
            ClientService,
        }

        public static readonly string DefaultOrleansTransactionManagerType = OrleansTransactionManagerType.GrainBased.ToString();

        /// <summary>
        /// The LogType attribute controls the persistent storage used for the transaction log.
        /// </summary>
        public TransactionLogType LogType { get; set; }

        /// <summary>
        /// Whether to clear the log on Transaction Service startup. Used primarily for testing purposes
        /// </summary>
        public bool ClearLogOnStartup { get; set; }

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
        /// Azure DataConnectionString for storage connection.
        /// </summary>
        public string DataConnectionString { get; set; }

        public string TransactionManagerType { get; set; } = DefaultOrleansTransactionManagerType;

        /// <summary>
        /// TransactionsConfiguration constructor.
        /// </summary>
        public TransactionsConfiguration()
        {
            LogType = TransactionLogType.Memory;
            ClearLogOnStartup = false;
            TransactionManagerProxyCount = 1;
            TransactionIdAllocationBatchSize = 50000;
            AvailableTransactionIdThreshold = 20000;
        }

        /// <summary>
        /// Load this configuration from xml element.
        /// </summary>
        /// <param name="child"></param>
        public void Load(XmlElement child)
        {
            if (child.HasAttribute("LogType"))
            {
                this.LogType = GetLogType(child.GetAttribute("LogType"));
            }

            if (child.HasAttribute("ClearLogOnStartup"))
            {
                this.ClearLogOnStartup = ConfigUtilities.ParseBool(child.GetAttribute("ClearLogOnStartup"),
                    "Invalid boolean value for the ClearLogOnStartup element");
            }

            if (child.HasAttribute("TransactionIdAllocationBatchSize"))
            {
                this.TransactionIdAllocationBatchSize = ConfigUtilities.ParseInt(child.GetAttribute("TransactionIdAllocationBatchSize"),
                    "Invalid boolean value for the TransactionIdAllocationBatchSize element");
            }
            
            if (child.HasAttribute("AvailableTransactionIdThreshold"))
            {
                this.AvailableTransactionIdThreshold = ConfigUtilities.ParseInt(child.GetAttribute("AvailableTransactionIdThreshold"),
                    "Invalid boolean value for the AvailableTransactionIdThreshold element");
            }

            if (child.HasAttribute("TransactionManagerProxyCount"))
            {
                this.TransactionManagerProxyCount = ConfigUtilities.ParseInt(child.GetAttribute("TransactionManagerProxyCount"),
                    "Invalid boolean value for the TransactionManagerProxyCount element");
            }

            if (child.HasAttribute("DataConnectionString"))
            {
                this.DataConnectionString = child.GetAttribute("DataConnectionString");
            }

            if (child.HasAttribute("TransactionManagerType"))
            {
                this.TransactionManagerType = child.GetAttribute("TransactionManagerType");
            }
        }

        private TransactionLogType GetLogType(string type)
        {
            if (type.Equals("Memory", StringComparison.InvariantCultureIgnoreCase))
            {
                return TransactionLogType.Memory;
            }

            if (type.Equals("AzureTable", StringComparison.InvariantCultureIgnoreCase))
            {
                return TransactionLogType.AzureTable;
            }

            throw new FormatException(string.Format("Invalid value {0} for TransactionLogType", type));
        }

    }
}
