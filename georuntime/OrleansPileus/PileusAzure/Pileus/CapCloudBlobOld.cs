using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    
    /// DEPRECATED: DO NOT USE.
    
    /// <summary>
    /// Provides a simple wrapper around most methods in the ICloudBlob interface,
    /// but allows some read operations to take an SLA and implements those methods accordingly.
    /// Moreover, write operations are performed synchronously on all primary replicas (CloudBlobContainer).
    /// </summary>
    public class CapCloudBlobOld

    {

        // The Consistency-based SLA engine that makes decisions on which blob to use
        private ConsistencySLAEngine slaEngine;

        // Stopwatch for timing roundtrip latencies
        private Stopwatch watch;

        // The container configuration that specifies what are the primary and secondary containers.
        private ReplicaConfiguration configuration;

        // The class that implements the read/write protocol
        private ReadWriteFramework protocol;

        // Default constructor
        public CapCloudBlobOld() {}

        /// <summary>
        /// Creates a new instance of a CapCloudBlob.
        /// </summary>
        /// <param name="strong">A reference to the strongly consistent copy of the blob.</param>
        /// <param name="eventual">A reference to the eventually consistent copy of the blob.</param>
        /// <param name="engine">The SLA enforcement engine.</param>
        public CapCloudBlobOld(string name, ReplicaConfiguration configuration, ConsistencySLAEngine engine)
        {
            this.Name = name;
            this.slaEngine = engine;
            this.configuration = configuration;
            this.watch = new Stopwatch();
            this.protocol = new ReadWriteFramework(name, configuration, engine);
        }

        private DateTimeOffset Timestamp(ICloudBlob blob) 
        {
            // TODO: ensure that this works in the case where the primary server changes
            // get timestamp from metadata if present since primary and secondary will have different LastModified times for same version of blob
            DateTimeOffset result;
            if (blob.Metadata.ContainsKey("LastModifiedOnPrimary"))
            {
                result = DateTimeOffset.Parse(blob.Metadata["LastModifiedOnPrimary"]);
            }
            else
            {
                result = blob.Properties.LastModified ?? DateTimeOffset.MinValue;
            }
            return result;
        }

        #region operations that use the SLA to choose between the strong and eventual blobs

        /// <summary>
        /// Download to stream from a blob acording to the SLA.
        /// 
        /// Fast mode => 2Delta
        /// slow mode for strong consistency => 4Delta (i.e., 2Delta for reading optimistically, 2Delta for checking that primary is not changed)
        /// TODO: it is only required to perform the second check if it is necessary to read from the primary replica, not because it is a nearby replica.
        /// </summary>
        /// <param name="target"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        public void DownloadToStream(System.IO.Stream target, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            if (options == null)
            {
                options = new BlobRequestOptions();
            }
            options.DisableContentMD5Validation = true;
            
            //ReadOp op = blob => blob.DownloadToStream(target, accessCondition, options, operationContext);
            //protocol.Read(op);
            protocol.Read(blob => blob.DownloadToStream(target, accessCondition, options, operationContext));
            /*
            ServerState ss=null;
            try
            {
                if (options == null)
                {
                    options = new BlobRequestOptions();
                }
                options.DisableContentMD5Validation = true;

                bool isDone = false;
                do
                {
                    ss = slaEngine.FindServerToRead(Name);
                    ICloudBlob blob = ClientRegistry.GetCloudBlob(ss.Name, configuration.Name, Name);
                    
                    if (configuration.IsInFastMode() || !ss.IsPrimary)
                    {
                        //it suffices to read from this replica which is secondary
                        watch.Start();

                        blob.DownloadToStream(target, accessCondition, options, operationContext);
                        watch.Stop();
                        Console.WriteLine("Downloaded blob from " + ss.Name + " in " + watch.ElapsedMilliseconds);
                        ss.AddRtt(watch.ElapsedMilliseconds);
                        slaEngine.Session.RecordObjectRead(blob.Name, Timestamp(blob), ss, slaEngine.Sla.Id);
                        isDone = true;
                    }
                    else
                    {
                        //we must read from this replica that is primary. 
                        //We first read, then refresh the container to see if the configuration is changed.

                        watch.Start();
                        blob.DownloadToStream(target, accessCondition, options, operationContext);
                        
                        watch.Stop();
                        Console.WriteLine("Downloaded blob from " + ss.Name + " in " + watch.ElapsedMilliseconds + " in slow mode");
                        configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());
                        
                        if (configuration.PrimaryServers.Contains(ss.Name))
                        {
                            //We have contacted the primary replica, hence we are good to go.

                            ss.AddRtt(watch.ElapsedMilliseconds);
                          //  slaEngine.SessionState.RecordObjectRead(blob.Name, Timestamp(blob), ss);
                            slaEngine.Session.RecordObjectRead(blob.Name, Timestamp(blob), ss, slaEngine.Sla.Id);
                            isDone = true;
                        }
                    }
                } while (!isDone);

            }
            catch (StorageException se)
            {
                if (StorageExceptionCode.NotFound(se))
                {
                    //blob is not found.
                    //this is fine because the replica might have removed.
                    //it simply returns, so client need to re-execute if this happens. 
                    //We can also re-execute the read here, but for debugging purposes, it's simpler for the client to re-execute. 
                    //Of course in real system, we need to re-execute at this level.
                    return;
                }
                Console.WriteLine(se.StackTrace.ToString());
                throw se;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("Object reference not set to an instance of an object"))
                    return;
                Console.WriteLine(ex.StackTrace.ToString());
                throw ex;
            }
            */

        }

        // TODO: implement this
        //public void DownloadRangeToStream(System.IO.Stream target, long? offset, long? length, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    ServerState ss = slaEngine.FindServerToRead(name);
        //    ICloudBlob blob = configuration.GetCloudBlobContainerDetail(ss.Name).GetCloudBlob(name);

        //    watch.Start();
        //    blob.DownloadRangeToStream(target, offset, length, accessCondition, options, operationContext);
        //    ss.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead(blob.Name, Timestamp(blob), ss);
        //}

        public bool Exists(Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {
            // Note sure that it's a good idea to ask a secondary site if a blob exists...
            bool result = false;
            protocol.Read(blob => result = blob.Exists(options, operationContext));
            /*
            ServerState ss = slaEngine.FindServerToRead(Name);
            ICloudBlob blob = ClientRegistry.GetCloudBlob(ss.Name, configuration.Name, Name);

            watch.Start();
            bool result = blob.Exists(options, operationContext);
            ss.AddRtt(watch.ElapsedMilliseconds);
            // slaEngine.SessionState.RecordObjectRead(blob.Name, Timestamp(blob), ss);
            slaEngine.Session.RecordObjectRead(blob.Name, Timestamp(blob), ss, "");
            */
            return result;
        }

        public void FetchAttributes(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {
            // TODO: use protocol
            ServerState ss = slaEngine.FindServerToRead(Name);
            ICloudBlob blob = ClientRegistry.GetCloudBlob(ss.Name, configuration.Name, Name);

            watch.Start();
            blob.FetchAttributes(accessCondition, options, operationContext);
            ss.AddRtt(watch.ElapsedMilliseconds);
            // slaEngine.SessionState.RecordObjectRead(blob.Name, Timestamp(blob), ss);
            slaEngine.Session.RecordObjectRead(blob.Name, Timestamp(blob), ss,"");
        }

        #endregion

        // TODO: Add public CloudBlob Main return configuration.PrimaryServers.First().ServiceClient.GetCloudBlob
        // This allows clients to call routines that are not implemented here or to contact the primary directly
        // e.g. blob.Main.FetchAttributes()
        
        public string Name
        {
            get;
            private set;
        }

        public Microsoft.WindowsAzure.Storage.Blob.CloudBlobClient ServiceClient
        {
            get
            {
                CloudBlobContainer primaryContainer = ClientRegistry.GetCloudBlobContainer(configuration.PrimaryServers.First(), Name);
                return primaryContainer.ServiceClient;
            }
        }

        //public int StreamWriteSizeInBytes
        //{
        //    get
        //    {
        //        return strongBlob.StreamWriteSizeInBytes;
        //    }
        //    set
        //    {
        //        strongBlob.StreamWriteSizeInBytes = value;
        //        eventualBlob.StreamWriteSizeInBytes = value;
        //    }
        //}

        //public int StreamMinimumReadSizeInBytes
        //{
        //    get
        //    {
        //        return strongBlob.StreamMinimumReadSizeInBytes;
        //    }
        //    set
        //    {
        //        strongBlob.StreamMinimumReadSizeInBytes = value;
        //        eventualBlob.StreamMinimumReadSizeInBytes = value;
        //    }
        //}

        //public Microsoft.WindowsAzure.Storage.Blob.BlobProperties Properties
        //{
        //    get { return strongBlob.Properties; }
        //}

        //public IDictionary<string, string> Metadata
        //{
        //    get { return strongBlob.Metadata; }
        //}

        //public DateTimeOffset? SnapshotTime
        //{
        //    get { return strongBlob.SnapshotTime; }
        //}

        //public Microsoft.WindowsAzure.Storage.Blob.CopyState CopyState
        //{
        //    get { return strongBlob.CopyState; }
        //}

        //public Microsoft.WindowsAzure.Storage.Blob.BlobType BlobType
        //{
        //    get { return strongBlob.BlobType; }
        //}

        /// <summary>
        /// Upload to primary blobs from the provided stream.
        /// 
        /// Our put is using an optimization where it does not take lease on blobs if there is only one primary container. 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        public void UploadFromStream(System.IO.Stream source, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {
            source.Position = 0;
            protocol.Write(blob => blob.UploadFromStream(source, /*lease.getAccessConditionWithLeaseId(accessCondition)*/ accessCondition, options, operationContext), accessCondition);
            /*
            bool isDone = false;

            do
            {
                try
                {

                    if (configuration.IsInFastMode())
                    {
                        DoUploadFromStream(source, accessCondition, options, operationContext);
                        isDone = true;
                    }
                    else
                    {
                        //We are not sure if reconfiguration is happening or not. We execute put in slow mode.
                        using (CloudBlobLease lease = new CloudBlobLease(configuration.Name, LeaseTakingPolicy.TryOnce))
                        {
                            if (lease.HasLease)
                            {
                                configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());
                                DoUploadFromStream(source, accessCondition, options, operationContext);
                                isDone = true;
                            }
                            else
                            {
                                continue;
                            }
                        }
                    }
                }
                catch (StorageException ex)
                {
                    throw ex;
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.StackTrace.ToString());
                    throw ex;
                }
            }
            while (!isDone);
            */
        }

        private void DoUploadFromStream(System.IO.Stream source, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {
            try
            {
                bool done = false;
                while (!done)
                {
                    using (PrimaryCloudBlobLease lease = new PrimaryCloudBlobLease(Name, configuration, true))
                    {
                        if (lease.HasLease)
                        {
                            foreach (string server in configuration.PrimaryServers)
                            {
                                watch.Start();

                                ICloudBlob blob = ClientRegistry.GetCloudBlob(server, configuration.Name, Name, false);

                                source.Position = 0;
                                blob.UploadFromStream(source, lease.getAccessConditionWithLeaseId(accessCondition), options, operationContext);

                                watch.Stop();

                                    ServerState ss = slaEngine.Monitor.GetServerState(server);
                                    ss.AddRtt(watch.ElapsedMilliseconds);
                                    slaEngine.Session.RecordObjectWritten(Name, Timestamp(blob), ss);
                            }

                            done = true;
                        }
                    }
                }

            }
            catch (StorageException se)
            {
                throw se;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginUploadFromStream(System.IO.Stream source, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginUploadFromStream(source, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginUploadFromStream(System.IO.Stream source, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginUploadFromStream(source, accessCondition, options, operationContext, callback, state);
        //}

        //public void EndUploadFromStream(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndUploadFromStream(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDownloadToStream(System.IO.Stream target, AsyncCallback callback, object state)
        //{
        //    // TODO: Use SLA to decide from which server to download.
        //    return strongBlob.BeginDownloadToStream(target, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDownloadToStream(System.IO.Stream target, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDownloadToStream(target, accessCondition, options, operationContext, callback, state);
        //}

        //public void EndDownloadToStream(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndDownloadToStream(asyncResult);
        //    slaEngine.SessionState.RecordObjectRead(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDownloadRangeToStream(System.IO.Stream target, long? offset, long? length, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDownloadRangeToStream(target, offset, length, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDownloadRangeToStream(System.IO.Stream target, long? offset, long? length, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDownloadRangeToStream(target, offset, length, accessCondition, options, operationContext, callback, state);
        //}

        //public void EndDownloadRangeToStream(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndDownloadRangeToStream(asyncResult);
        //    slaEngine.SessionState.RecordObjectRead(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginExists(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginExists(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginExists(Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginExists(options, operationContext, callback, state);
        //}

        //public bool EndExists(IAsyncResult asyncResult)
        //{
        //    bool result = strongBlob.EndExists(asyncResult);
        //    slaEngine.SessionState.RecordObjectRead(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //    return result;
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginFetchAttributes(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginFetchAttributes(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginFetchAttributes(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginFetchAttributes(accessCondition, options, operationContext, callback, state);
        //}

        //public void EndFetchAttributes(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndFetchAttributes(asyncResult);
        //    slaEngine.SessionState.RecordObjectRead(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public void SetMetadata(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    strongBlob.SetMetadata(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginSetMetadata(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginSetMetadata(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginSetMetadata(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginSetMetadata(accessCondition, options, operationContext, callback, state);
        //}

        //public void EndSetMetadata(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndSetMetadata(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public void SetProperties(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    strongBlob.SetProperties(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginSetProperties(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginSetProperties(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginSetProperties(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginSetProperties(accessCondition, options, operationContext, callback, state);
        //}

        //public void EndSetProperties(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndSetProperties(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, Timestamp(strongBlob), primaryServer);
        //}

        public void Delete(Microsoft.WindowsAzure.Storage.Blob.DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {

            bool isDone = false;

            do
            {
                try
                {

                    if (configuration.IsInFastMode())
                    {
                        DoDelete(deleteSnapshotsOption, accessCondition, options, operationContext);
                        isDone = true;
                    }
                    else
                    {
                        //We are not sure if reconfiguration is happening or not. We execute put in slow mode.
                        using (CloudBlobLease lease = new CloudBlobLease(configuration.Name, LeaseTakingPolicy.TryOnce))
                        {
                            if (lease.HasLease)
                            {
                                configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());
                                DoDelete(deleteSnapshotsOption, accessCondition, options, operationContext);
                                isDone = true;
                            }
                        }
                    }
                }
                catch (StorageException ex)
                {
                    throw ex;
                }
                catch (Exception ex)
                {
                    throw ex;
                }

            } while (!isDone);


        }

        private void DoDelete(Microsoft.WindowsAzure.Storage.Blob.DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        {
            bool done = false;
            while (!done)
            {
                using (PrimaryCloudBlobLease lease = new PrimaryCloudBlobLease(this.Name, configuration, true))
                {
                    if (lease.HasLease)
                    {
                        Dictionary<ICloudBlob, IAsyncResult> results = new Dictionary<ICloudBlob, IAsyncResult>();
                        foreach (string serverName in configuration.PrimaryServers)
                        {
                            watch.Start();
                            ICloudBlob blob = ClientRegistry.GetCloudBlob(serverName, configuration.Name, Name);
                            results[blob] = blob.BeginDelete(deleteSnapshotsOption, lease.getAccessConditionWithLeaseId(accessCondition), options, operationContext, null, null);
                            ServerState ss = slaEngine.Monitor.GetServerState(serverName);
                            ss.AddRtt(watch.ElapsedMilliseconds);
                            slaEngine.Session.RecordObjectWritten(Name, Timestamp(blob), ss);
                        }

                        foreach (ICloudBlob blob in results.Keys)
                        {
                            blob.EndDelete(results[blob]);
                        }
                        done = true;
                    }
                }
            }
        }
        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDelete(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDelete(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDelete(Microsoft.WindowsAzure.Storage.Blob.DeleteSnapshotsOption deleteSnapshotsOption, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDelete(deleteSnapshotsOption, accessCondition, options, operationContext, callback, state);
        //}

        //public void EndDelete(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndDelete(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, DateTimeOffset.Now, primaryServer);
        //}

        //public bool DeleteIfExists(Microsoft.WindowsAzure.Storage.Blob.DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    bool result = strongBlob.DeleteIfExists(deleteSnapshotsOption, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, DateTimeOffset.Now, primaryServer);
        //    return result;
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDeleteIfExists(AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDeleteIfExists(callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginDeleteIfExists(Microsoft.WindowsAzure.Storage.Blob.DeleteSnapshotsOption deleteSnapshotsOption, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginDeleteIfExists(deleteSnapshotsOption, accessCondition, options, operationContext, callback, state);
        //}

        //public bool EndDeleteIfExists(IAsyncResult asyncResult)
        //{
        //    bool result = strongBlob.EndDeleteIfExists(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten(strongBlob.Name, DateTimeOffset.Now, primaryServer);
        //    return result;
        //}

        //public string AcquireLease(TimeSpan? leaseTime, string proposedLeaseId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    string result = strongBlob.AcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginAcquireLease(leaseTime, proposedLeaseId, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginAcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext, callback, state);
        //}

        //public string EndAcquireLease(IAsyncResult asyncResult)
        //{
        //    return strongBlob.EndAcquireLease(asyncResult);
        //}

        //public void RenewLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    strongBlob.RenewLease(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginRenewLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginRenewLease(accessCondition, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginRenewLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginRenewLease(accessCondition, options, operationContext, callback, state);
        //}

        //public void EndRenewLease(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndRenewLease(asyncResult);
        //}

        //public string ChangeLease(string proposedLeaseId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    string result = strongBlob.ChangeLease(proposedLeaseId, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginChangeLease(proposedLeaseId, accessCondition, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginChangeLease(proposedLeaseId, accessCondition, options, operationContext, callback, state);
        //}

        //public string EndChangeLease(IAsyncResult asyncResult)
        //{
        //    return strongBlob.EndChangeLease(asyncResult);
        //}

        //public void ReleaseLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    strongBlob.ReleaseLease(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginReleaseLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginReleaseLease(accessCondition, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginReleaseLease(Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginReleaseLease(accessCondition, options, operationContext, callback, state);
        //}

        //public void EndReleaseLease(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndReleaseLease(asyncResult);
        //}

        //public TimeSpan BreakLease(TimeSpan? breakPeriod = null, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    TimeSpan result = strongBlob.BreakLease(breakPeriod, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginBreakLease(breakPeriod, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginBreakLease(breakPeriod, accessCondition, options, operationContext, callback, state);
        //}

        //public TimeSpan EndBreakLease(IAsyncResult asyncResult)
        //{
        //    return strongBlob.EndBreakLease(asyncResult);
        //}

        //public string StartCopyFromBlob(Uri source, Microsoft.WindowsAzure.Storage.AccessCondition sourceAccessCondition = null, Microsoft.WindowsAzure.Storage.AccessCondition destAccessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    return strongBlob.StartCopyFromBlob(source, sourceAccessCondition, destAccessCondition, options, operationContext);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginStartCopyFromBlob(Uri source, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginStartCopyFromBlob(source, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginStartCopyFromBlob(Uri source, Microsoft.WindowsAzure.Storage.AccessCondition sourceAccessCondition, Microsoft.WindowsAzure.Storage.AccessCondition destAccessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginStartCopyFromBlob(source, sourceAccessCondition, destAccessCondition, options, operationContext, callback, state);
        //}

        //public string EndStartCopyFromBlob(IAsyncResult asyncResult)
        //{
        //    return strongBlob.EndStartCopyFromBlob(asyncResult);
        //}

        //public void AbortCopy(string copyId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition = null, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options = null, Microsoft.WindowsAzure.Storage.OperationContext operationContext = null)
        //{
        //    strongBlob.AbortCopy(copyId, accessCondition, options, operationContext);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginAbortCopy(string copyId, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginAbortCopy(copyId, callback, state);
        //}

        //public Microsoft.WindowsAzure.Storage.ICancellableAsyncResult BeginAbortCopy(string copyId, Microsoft.WindowsAzure.Storage.AccessCondition accessCondition, Microsoft.WindowsAzure.Storage.Blob.BlobRequestOptions options, Microsoft.WindowsAzure.Storage.OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return strongBlob.BeginAbortCopy(copyId, accessCondition, options, operationContext, callback, state);
        //}

        //public void EndAbortCopy(IAsyncResult asyncResult)
        //{
        //    strongBlob.EndAbortCopy(asyncResult);
        //}

        //public Uri Uri
        //{
        //    get { return strongBlob.Uri; }
        //}

        //public Microsoft.WindowsAzure.Storage.Blob.CloudBlobDirectory Parent
        //{
        //    get { return strongBlob.Parent; }
        //}

        //public Microsoft.WindowsAzure.Storage.Blob.CloudBlobContainer Container
        //{
        //    get { return strongBlob.Container; }
        //}

    }
}
