using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.WindowsAzure.Storage.Pileus.Utils
{
    public static class CostModel
    {
        private static readonly double ONE_GB=Math.Pow(2,30);

        /// <summary>
        /// Cost duration in seconds.
        /// </summary>
        public static readonly double COST_MODEL_DURATION = 30 * 24 * 60 * 60;

        #region Azure_Costs
        /// <summary>
        /// Cost (in cents) of storing 1 B of data in a site for a month
        /// Taken from Azure
        /// </summary>
        public static readonly double STORAGE_COST_PER_MONTH = 7 / ONE_GB;

        /// <summary>
        /// Cost (in cents) of transfering 1B between two sites for a month
        /// Taken from Azure
        /// </summary>
        public static readonly double TRANSFER_COST = 6 / ONE_GB;

        /// <summary>
        /// Cost (in cents) of renting a worker or VM for a month
        /// Taken from azure
        /// </summary>
        public static readonly double COMPUTING_COST = 1488;

        /// <summary>
        /// Cost (in cents) of performing a transaction
        /// Taken from Azure
        /// </summary>
        public static readonly double TRANSACTION_COST = 1e-5;

        #endregion


        /// <summary>
        /// Returns size of the container in Bytes
        /// </summary>
        /// <param name="container"></param>
        /// <returns>size in Bytes</returns>
        public static long GetContainerSize(CloudBlobContainer container)
        {
            long size=0;
            foreach (ICloudBlob blob in container.ListBlobs())
                size += blob.Properties.Length;

            return size;
        }

        //we assume we can do Sync in a batch. Hence, we need 2 transactions
        public static double GetSyncCost(int numberOfWrites, int syncPeriod)
        {
            int numberOfSyncs = Convert.ToInt32((COST_MODEL_DURATION * 1000) / syncPeriod);
            return 2 * numberOfSyncs * TRANSACTION_COST;
        }

        public static double GetPrimaryTransactionalCost(int numberOfReads, int numberOfWrites)
        {
            return TRANSACTION_COST * (numberOfReads + numberOfWrites);
        }

        public static double GetSecondaryTransactionalCost(int numberOfReads)
        {
            return TRANSACTION_COST * numberOfReads;
        }

        public static double GetStorageCost(CloudBlobContainer container)
        {
            return STORAGE_COST_PER_MONTH * GetContainerSize(container);
        }

        
    }
}
