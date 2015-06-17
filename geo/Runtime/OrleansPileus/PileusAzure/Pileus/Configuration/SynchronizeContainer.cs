using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    /// <summary>
    /// Performs some micro actions for <see cref="ConfigurationAction"/> 
    /// </summary>
    public class SynchronizeContainer
    {
        private Object mylock;
        private int concurrentSyncs;

        private CloudBlobContainer sourceContainer;
        private CloudBlobContainer targetContainer;

        private DateTimeOffset? lastModified;

        private Task task;


        public SynchronizeContainer(CloudBlobContainer sourceContainer, CloudBlobContainer targetContainer, DateTimeOffset? lastModified = null)
        {
            mylock = new Object();
            concurrentSyncs = 0;

            this.sourceContainer = sourceContainer;
            this.targetContainer = targetContainer;

            this.lastModified = lastModified;
        }

        private void Sync()
        {
            try
            {
                //we first initialize a new replica in the new server
                if (!targetContainer.Exists())
                    targetContainer.Create();

            }
            catch (StorageException ex)
            {
                //409 is the conflict message.
                if (StorageExceptionCode.Conflict(ex))
                {
                    throw ex;
                }
            }

            if (lastModified == null)
            {
                targetContainer.FetchAttributes();
                if (targetContainer.Metadata.ContainsKey("lastsync"))
                {
                    //if no lastmodified time is provided in the constructor, we still try to be fast.
                    //So, we check to see if by any chance the container previously has synchronized.
                    lastModified = DateTimeOffset.Parse(targetContainer.Metadata["lastsync"]);
                }
            }

            DateTimeOffset startTime = DateTimeOffset.Now;
            try
            {
                IEnumerable<IListBlobItem> sourceBlobList=null;
                if (lastModified != null)
                {
                    sourceBlobList= sourceContainer.ListBlobs().OfType<ICloudBlob>().Where(b => b.Properties.LastModified > lastModified);
                }
                else
                {
                    //this is the slowest sync path. It needs to go through all blobs.
                    //it tries to avoid this path as much as possible. 
                    sourceBlobList = sourceContainer.ListBlobs();
                }

                foreach (ICloudBlob sourceBlob in sourceBlobList)
                {
                    ICloudBlob targetBlob = targetContainer.GetBlockBlobReference(sourceBlob.Name);

                    //Since its an inter-account transfer, we need access rights to do the transfer. 
                    var sas = sourceContainer.GetSharedAccessSignature(new SharedAccessBlobPolicy()
                    {
                        SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5),
                        SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(30),
                        Permissions = SharedAccessBlobPermissions.Read,
                    });

                    AccessCondition ac = new AccessCondition();
                    Interlocked.Increment(ref concurrentSyncs);
                    var srcBlockBlobSasUri = string.Format("{0}{1}", sourceBlob.Uri, sas);
                    targetBlob.BeginStartCopyFromBlob(new Uri(srcBlockBlobSasUri), BlobCopyFinished, null);
                }                

                while (concurrentSyncs > 0)
                {
                    lock (mylock)
                        Monitor.Wait(mylock, 200);
                }

                targetContainer.Metadata["lastsync"] = startTime.ToString();
                targetContainer.SetMetadata();
            }
            catch (StorageException se)
            {
                //if the blob/container does not exist, it means that it is removed by configurator. 
                //We safely return. 
                if (StorageExceptionCode.NotFound(se))
                {
                    return;
                }
                throw se;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void BeginSyncContainers()
        {
            task=Task.Factory.StartNew(Sync);
        }

        public void EndSyncContainers()
        {
            task.Wait();
        }

        public void SyncContainers()
        {
            Sync();
        }

        private void BlobCopyFinished(IAsyncResult result)
        {
            Interlocked.Decrement(ref concurrentSyncs);
            lock (mylock)
            {
                if (concurrentSyncs == 0)
                    Monitor.Pulse(mylock);
            }
        }

    }
}
