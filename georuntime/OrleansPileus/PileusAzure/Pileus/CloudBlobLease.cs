using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    
    /*
     * DEPRECATED: Leases have been replaced by optimistic concurrency control using ETags.
     * This class was left in case we find a need for leases in the future.
     */

    /* 
     * Intended usage:
     * using (CloudBlobLease lease = new CloudBlobLease(blob, LeaseTakingPolicy.TryUntilSuccessful)) 
     * { 
     *      if (lease.HasLease) 
     *      {
     *           do something while holding lease
     *      } 
     * }
     */

    internal enum LeaseTakingPolicy
    {
        TryOnce,
        TryUntilSuccessful
    }

    /// <summary>
    /// Try to take the lease on the given blob.
    /// Creating a new instance of this class (e.g. in a using statement) attempts to acquire the lease.
    /// Disposing of the instance releases the lease.
    /// </summary>
    internal class CloudBlobLease : IDisposable
    {
        private ICloudBlob blob;

        private bool disposed = false;

        /// <summary>
        /// Trys to acquire the lease on the configuration blob.
        /// Note that the caller must check HasLease to see whether the acquisition succeeded.
        /// </summary>
        /// <param name="containerName">Name of the container/configuration</param>
        /// <param name="policy">Whether to try once or many times</param>
        /// <returns>An instance of the CloudBlobLease object.</returns>
        public CloudBlobLease(string containerName, LeaseTakingPolicy policy)
        {
            this.blob = ClientRegistry.GetConfigurationContainer(containerName).GetBlockBlobReference(ConstPool.CURRENT_CONFIGURATION_BLOB_NAME);
            AcquireBlobLease(policy);
        }

        /// <summary>
        /// Trys to acquire the lease on a blob.
        /// Note that the caller must check HasLease to see whether the acquisition succeeded.
        /// </summary>
        /// <param name="blob">Particular blob we would like to take the lease on</param>
        /// <param name="policy">Whether to try once or many times</param>
        /// <returns>An instance of the CloudBlobLease object.</returns>
        public CloudBlobLease(ICloudBlob blob, LeaseTakingPolicy policy)
        {
            this.blob = blob;
            AcquireBlobLease(policy);
        }

        public Boolean HasLease{get; internal set;}

        public string LeaseId
        {
            get;
            internal set;
        }

        /// <summary>
        /// Add leasID to the provided accesscondition.
        /// </summary>
        /// <param name="accessCondition">accessCondition which the leaseID should be added to</param>
        /// <returns></returns>
        public AccessCondition getAccessConditionWithLeaseId(AccessCondition accessCondition = null)
        {
            if (accessCondition == null)
                accessCondition = new AccessCondition();

            accessCondition.LeaseId = this.LeaseId;

            return accessCondition;
        }

        private void AcquireBlobLease(LeaseTakingPolicy policy)
        {
            string ProposedLeaseId = Guid.NewGuid().ToString();
            bool isDone = false;

            // if policy == LeaseTakingPolicy.TryUntilSuccessful then loop indefinitely
            while (!isDone)
            {
                try
                {
                    LeaseId = blob.AcquireLease(null, ProposedLeaseId);
                    HasLease = true;
                    isDone = true;
                }
                catch (StorageException ex)
                {
                    HasLease = false;
                    if (StorageExceptionCode.Conflict(ex)) //Conflict
                    {
                        if (policy == LeaseTakingPolicy.TryOnce)
                            isDone = true;
                    }
                    else
                    {
                        ReleaseBlobLease();
                        throw;
                    }
                }
            }
        }

        private void ReleaseBlobLease()
        {
            try
            {
                if (LeaseId == null || LeaseId.Equals(""))
                    return;

                blob.ReleaseLease(AccessCondition.GenerateLeaseCondition(LeaseId));
            }
            catch (StorageException ex)
            {
                if (StorageExceptionCode.NotFound(ex))
                    // Container is removed, hence its lease.
                    return;
                else
                    throw ex;
            }
            catch (Exception exx)
            {
                throw exx;
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }


        protected virtual void Dispose(bool disposing)
        {
            if (!disposed)
            {
                //Release unmanaged resources
                ReleaseBlobLease();

                if (disposing)
                {
                }

                disposed = true;
            }
        }


        ~CloudBlobLease()
        {
            Dispose(false);
        }
    }
}
