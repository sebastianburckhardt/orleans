using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Diagnostics;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using System.IO;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /*
     * DEPRECATED: Leases have been replaced by optimistic concurrency control using ETags.
     * This class was left in case we find a need for leases in the future.
     */

    /// <summary>
    /// Take the lease on all primary blobs.
    /// 
    /// TODO: this class is very similar to <see cref="CLoudBlobLease"/>. This class should indeed extend it.
    /// </summary>
    public class PrimaryCloudBlobLease: IDisposable
    {

        private bool disposed = false;

        public Boolean HasLease{get; internal set;}

        private Dictionary<ICloudBlob, string> leasedBlobs;

        public string ProposedLeaseId
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

            accessCondition.LeaseId = this.ProposedLeaseId;

            return accessCondition;
        }

        /// <summary>
        /// Lease configuration container responsible for this blob along with blobs in the containers
        /// </summary>
        /// <param name="blobName">Particular blobs we would like to lease</param>
        /// <param name="containerElementSet">list of containers</param>
        /// <param name="putOptimization">perform put optimization. I.e., for put operations, it is not required to take lease if there is only one primary.</param>
        public PrimaryCloudBlobLease(string blobName, ReplicaConfiguration configuration, bool putOptimization = false)
        {

            if ((putOptimization) && configuration.PrimaryServers.Count == 1)
            {
                //There is only one primary. No need to 
                HasLease = true;
                return;
            }

            leasedBlobs = new Dictionary<ICloudBlob,string>();
            //Reconfiguration is not going to happen in near future. 
            //We can safely take leases of primary blobs.
            this.ProposedLeaseId = Guid.NewGuid().ToString();

            foreach (string serverName in configuration.PrimaryServers)
            {
                try
                {
                    ICloudBlob blob = ClientRegistry.GetCloudBlob(serverName, configuration.Name, blobName);
                    if (!blob.Exists())
                    {
                        //we cannot take lease on a non-existing blob. 
                        //Hence, we create it first. 
                        byte[] dummy=new byte[1];
                        dummy[0] = (byte)0;
                        var ms = new MemoryStream(dummy);
                        blob.UploadFromStream(ms, null, null, null);
                    }
                    string leaseID = blob.AcquireLease(null, ProposedLeaseId);
                    leasedBlobs.Add(blob, leaseID);
                }
                catch (StorageException ex)
                {
                    releaseContainers();
                    leasedBlobs.Clear();
                    HasLease = false;
                    if (ex.GetBaseException().Message.Contains("409")) //Conflict
                    {
                        return;
                    }
                    else
                    {
                        throw ex;
                    }
                }
            }
            HasLease = true;
        }

        private void releaseContainers()
        {
            //leasedBlobs is null iff putOptimization is set to true, and no lease is taken consequently.
            if (leasedBlobs == null || leasedBlobs.Keys.Count == 0)
                return;

            foreach (ICloudBlob blob in leasedBlobs.Keys)
            {
                AccessCondition condition = new AccessCondition();
                condition.LeaseId = leasedBlobs[blob];
                try
                {
                    blob.ReleaseLease(condition);
                }
                catch (StorageException ex)
                {
                    // Container is removed, hence its lease.
                    if (ex.GetBaseException().Message.Contains("404")) 
                    {
                        return;
                    }
                    else
                        throw;
                }
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
                releaseContainers();

                if (disposing && leasedBlobs!=null)
                {
                    leasedBlobs.Clear();
                }

                disposed = true;
            }
        }


        ~PrimaryCloudBlobLease()
        {
            Dispose(false);
        }


    }
}
