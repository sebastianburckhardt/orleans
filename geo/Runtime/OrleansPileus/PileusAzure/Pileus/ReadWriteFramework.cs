using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{

    public  delegate void ReadOp(ICloudBlob blob);
    public  delegate void WriteOp(ICloudBlob blob);
        
    /// <summary>
    /// Procedures for correctly performing read and write operations in various conditions and configurations.
    /// </summary>
    public class ReadWriteFramework
    {
        // The name of the bob that is being read and written
        private string blobName;

        // The name of the replicated container in which this blob is stored
        private string containerName;

        // The Consistency-based SLA engine that makes decisions on which blob to use
        private ConsistencySLAEngine slaEngine;

        // Stopwatch for timing roundtrip latencies
        private Stopwatch watch;

        // The configuration that specifies the primary and secondary replicas.
        private ReplicaConfiguration configuration;

        // The blob that was accessed for the last read operation
        private ICloudBlob blobForRead;

        public ReadWriteFramework(string blobName, ReplicaConfiguration configuration, ConsistencySLAEngine engine)
        {
            this.blobName = blobName;
            this.containerName = configuration.Name;
            this.configuration = configuration;
            this.slaEngine = engine;
            this.blobForRead = null;
            this.watch = new Stopwatch();

        }

        /// <summary>
        /// Returns the blob for the main primary site of the current configuration.
        /// </summary>
        /// <returns>the primary blob (or null if unable to renew the lease on the configuration)</returns>
        public ICloudBlob MainPrimary()
        {
            ICloudBlob primary = null;
            if (configuration.IsInFastMode(renew: true))
            {
                string server = configuration.PrimaryServers.First();
                primary = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName);
            }
            return primary;
        }

        /// <summary>
        /// Returns the blob that was used for the previous read operation.
        /// This is useful if the client wants to read from the same blob again 
        /// or wants to access the properties/metadata of this blob.
        /// </summary>
        /// <returns>the previously read blob</returns>
        public ICloudBlob PrevRead()
        {
            return blobForRead;
        }

        #region Read operations

        public void Read(ReadOp op)
        {
            if (configuration.IsInFastMode())
            {
                FastRead(op);
            }
            else
            {
                SlowRead(op);
            }
        }

        /// <summary>
        /// Perform standard read and update server and session state.
        /// </summary>
        /// <param name="op">The read operation</param>
        /// <param name="blob">The blob being read</param>
        /// <param name="ss">The chosen server</param>
        private void FastRead(ReadOp op)
        {
            ServerState ss = slaEngine.FindServerToRead(blobName);
            blobForRead = ClientRegistry.GetCloudBlob(ss.Name, containerName, blobName);
            
            watch.Start();
            op(blobForRead);
            watch.Stop();
            
            slaEngine.RecordObjectRead(blobForRead.Name, Timestamp(blobForRead), ss, watch.ElapsedMilliseconds);
        }

        /// <summary>
        /// Perform read optimistically and then verify configuration.
        /// </summary>
        /// <param name="op">The read operation</param>
        /// <param name="blob">The blob being read</param>
        /// <param name="ss">The chosen server</param>
        /// <returns>whether the read succeeded; if not, then it should be tried again.</returns>
        private void SlowRead(ReadOp op)
        {
            // TODO: Deal with the case that we try to read from a replica that is no longer a replica and the read fails
            // In this case, the read should be retried after refreshing the configuration.
            ServerState ss = null;
            try
            {
                bool isDone = false;
                do  // until we enter fast mode or succeed at reading in slow mode with the correct configuration
                {
                    ss = slaEngine.FindServerToRead(blobName);
                    blobForRead = ClientRegistry.GetCloudBlob(ss.Name, containerName, blobName);

                    // TODO: this should really check if the reader wants strong consistency
                    // It could be that eventual consistency is fine but the SLA Engine just happened to choose a primary server
                    // In this case, we can do a fast mode read since we don't care if the chosen server is no longer a primary
                    if (configuration.IsInFastMode() || !configuration.PrimaryServers.Contains(ss.Name))
                    {
                        // it is fine to read from the selected secondary replica 
                        // or from a primary replica if we have now entered fast mode
                        FastRead(op);
                        isDone = true;
                    }
                    else
                    {
                        // read from the selected replica that we believe to be a primary replica 
                        watch.Start();
                        op(blobForRead);
                        watch.Stop();

                        // then see if the configuration has changed and the selected replica is no longer primary
                        configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());
                        
                        // TODO: check the epoch number on the configuration to make sure that we have not missed a configuration.
                        // i.e. we need to make sure that the server from which we read did not become a secondary during our read,
                        // and then back to primary when we checked.
                        // TODO: maybe we should check that the read delivered a non-zero utility.
                        // It is possible that the configuration was so stale that a much better server could have been chosen.
                        if (configuration.PrimaryServers.Contains(ss.Name))
                        {
                            //We have contacted the primary replica, hence we are good to go.
                            slaEngine.RecordObjectRead(blobForRead.Name, Timestamp(blobForRead), ss, watch.ElapsedMilliseconds);
                            isDone = true;
                        }
                        isDone = false;  // not done
                    }
                } while (!isDone);
            }
            // TODO: decide if we need to catch any exceptions here or just let then propagate through
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
                // storage exceptions are passed through to the caller
                throw;
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("Object reference not set to an instance of an object"))
                    return;
                throw ex;
            }
        }

        #endregion

        #region Write operations

        public void Write(WriteOp op, AccessCondition accessCondition, List<SessionState> sessions = null, ServerMonitor monitor = null)
        {
            // TODO: ensure that there is enough time left on the lease to complete the write
            // Can set a timeout using BlobRequestOptions and/or renew a least that is about to expire
            if (configuration.IsInFastMode())
            {
                if (configuration.PrimaryServers.Count == 1)
                {
                    FastWrite(op, sessions, monitor);
                }
                else
                {
                    MultiWrite(op, accessCondition, sessions, monitor);
                }
            }
            else
            {
                SlowWrite(op, accessCondition, sessions, monitor);
            }
        }

        private void FastWrite(WriteOp op, List<SessionState> sessions = null, ServerMonitor monitor = null)
        {
            // there should only be one primary server since we are in fast mode
            string server = configuration.PrimaryServers.First();
            ICloudBlob blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName, false);
            
            watch.Start();
            op(blob);
            watch.Stop();

            // update server and session state
            ServerState ss = (monitor == null) ? slaEngine.Monitor.GetServerState(server) : monitor.GetServerState(server);
            ss.AddRtt(watch.ElapsedMilliseconds);
            if (sessions == null)
            {
                slaEngine.Session.RecordObjectWritten(blobName, Timestamp(blob), ss);
            }
            else
            {
                foreach (SessionState session in sessions)
                {
                    session.RecordObjectWritten(blobName, Timestamp(blob), ss);
                }
            }
        }

        private void SlowWrite(WriteOp op, AccessCondition accessCondition, List<SessionState> sessions = null, ServerMonitor monitor = null)
        {
            // A reconfiguration is in progress, but may not have happened yet.
            // We could try to complete this write before the reconfiguration, but that would require locking
            // to prevent the configuration service from changing the configuration during this write.
            // Instead, we take the simple approach of waiting for the reconfiguration to complete.
            
            bool isDone = false;
            do  // while unable to lease the configuration
            {
                if (configuration.IsInFastMode(renew: true))
                {
                    if (configuration.PrimaryServers.Count == 1)
                    {
                        FastWrite(op, sessions, monitor);
                    }
                    else
                    {
                        MultiWrite(op, accessCondition, sessions, monitor);
                    }
                    isDone = true;
                }
                else
                {
                    Thread.Sleep(ConstPool.CONFIGURATION_ACTION_DURATION);
                }
            }
            while (!isDone);
        }

        private void MultiWrite(WriteOp op, AccessCondition access, List<SessionState> sessions = null, ServerMonitor monitor = null)
        {
            // This protocol uses three phases and ETags
            // It assumes that the client is in fast mode and remains so throughout the protocol
            // i.e. it assumes that the set of primaries does not change.
            // TODO: recover from failed clients that may leave a write partially completed

            ICloudBlob blob;
            Dictionary<string, string> eTags;

            // Phase 1.  Mark intention to write with write-in-progress flags
            eTags = SetWiPFlags();
            if (eTags == null)
            {
                // flags were not successfully set, so abort the protocol
                return;
            }

            // Phase 2.  Perform write at all primaries
            bool didAtLeastOne = false;
            foreach (string server in configuration.PrimaryServers)
            {
                blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName, false);
                access.IfMatchETag = eTags[server];
                watch.Start();
                try
                {
                    op(blob);
                }
                catch (StorageException)
                {
                    // If writing fails at some primary, then abort the protocol
                    // It could be that a concurrent writer is in progress
                    // Note that some writes may have already been performed
                    // If so, then we leave the WiP flags set so the recovery process will kick in
                    // or so a concurrent writer can complete its protocol and overwrite our writes
                    // If not, then we can clear the WiP flags
                    if (!didAtLeastOne)
                    {
                        ClearWiPFlags(eTags);
                    }
                    return;
                }                
                watch.Stop();
                eTags[server] = blob.Properties.ETag;
                didAtLeastOne = true;

                // update session and server state  
                ServerState ss = (monitor == null) ? slaEngine.Monitor.GetServerState(server) : monitor.GetServerState(server);
                ss.AddRtt(watch.ElapsedMilliseconds);
                if (sessions == null)
                {
                    slaEngine.Session.RecordObjectWritten(blobName, Timestamp(blob), ss);
                }
                else
                {
                    foreach (SessionState session in sessions)
                    {
                        session.RecordObjectWritten(blobName, Timestamp(blob), ss);
                    }
                }
            }

            // Phase 3.  Clear write-in-progress flags to indicate that write has completed
            ClearWiPFlags(eTags);
        }

        /// <summary>
        /// Sets the WiP flags on all of the primary blobs.
        /// </summary>
        /// <returns>a dictionary containing the returned eTags for each primary; 
        /// returns null if the flag-setting protocol was not successful</returns>
        private Dictionary<string, string> SetWiPFlags()
        {
            ICloudBlob blob;
            Dictionary<string, string> eTags = new Dictionary<string, string>();
            bool didAtLeastOne = false;

            foreach (string server in configuration.PrimaryServers)
            {
                blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName, false);
                blob.Metadata[ConstPool.WRITE_IN_PROGRESS] = ConstPool.WRITE_IN_PROGRESS;
                try
                {
                    blob.SetMetadata();
                    didAtLeastOne = true;
                }
                catch (StorageException)
                {
                    // If setting the flag fails at some primary, then abort the protocol
                    // Note that some WiP flags may have already been set, so we first try to clear them
                    if (didAtLeastOne)
                    {
                        ClearWiPFlags();
                    }
                    return null;
                }
                eTags[server] = blob.Properties.ETag;
            }
            return eTags;
        }

        /// <summary>
        /// Clears the WiP flags on all of the primary blobs.
        /// </summary>
        /// <param name="eTags">if not null, then the flags should be cleared conditionally</param>
        private void ClearWiPFlags(Dictionary<string, string> eTags = null)
        {
            ICloudBlob blob;
            AccessCondition access = AccessCondition.GenerateEmptyCondition();
            
            // Clear WiP flags at non-main primaries first
            foreach (string server in configuration.PrimaryServers.Skip(1))
            {
                blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName, false);
                blob.Metadata.Remove(ConstPool.WRITE_IN_PROGRESS);
                if (eTags != null)
                {
                    access = AccessCondition.GenerateIfMatchCondition(eTags[server]);
                }
                try
                {
                    blob.SetMetadata(access);
                }
                catch (StorageException)
                {
                    // Ignore failures since the only consequence is that the Wip flag remains set
                    // It could be that another write is still in progress
                    // The flag will be cleared eventually by another writer or by the recovery process
                }
            }

            //  Clear WiP on main primary last
            blob = ClientRegistry.GetCloudBlob(configuration.PrimaryServers.First(), configuration.Name, blobName, false);
            blob.Metadata.Remove(ConstPool.WRITE_IN_PROGRESS);
            if (eTags != null)
            {
                access = AccessCondition.GenerateIfMatchCondition(eTags[configuration.PrimaryServers.First()]);
            }
            try
            {
                blob.SetMetadata(access);
            }
            catch (StorageException)
            {
                // Ignore
            }
        }

        private void MultiWriteUsingBlobLease(WriteOp op)
        {
            // TODO: recover from failed clients that may leave a write partially completed
            // TODO: remove use of leases and replace with ETags
            // throw new Exception("Write to multiple primaries not yet implemented.");
            try
            {
                bool done = false;
                while (!done)
                {
                    // grab lease on blob to guard against concurrent writers
                    using (PrimaryCloudBlobLease lease = new PrimaryCloudBlobLease(blobName, configuration, true))
                    {
                        if (lease.HasLease)
                        {
                            // TODO: fix code for writing to multiple primaries (and remove it from here)
                            foreach (string server in configuration.PrimaryServers)
                            {
                                watch.Start();
                                ICloudBlob blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName, false);
                                op(blob);
                                watch.Stop();

                                    ServerState ss = slaEngine.Monitor.GetServerState(server);
                                    ss.AddRtt(watch.ElapsedMilliseconds);
                                    slaEngine.Session.RecordObjectWritten(blobName, Timestamp(blob), ss);
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

        /// <summary>
        /// Perform a local write operation, e.g. set a property, on all blobs holding a replica.
        /// This does not bother to check the lease on the configuration.  
        /// It should not be used for arbitrary writes that need to be performed atomically.
        /// </summary>
        /// <param name="op">the write operation being done</param>
        public void SetProperty(WriteOp op)
        {
            foreach (string server in configuration.PrimaryServers)
            {
                ICloudBlob blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName);
                op(blob);
            }
            foreach (string server in configuration.SecondaryServers)
            {
                ICloudBlob blob = ClientRegistry.GetCloudBlob(server, configuration.Name, blobName);
                op(blob);
            }
        }

        #endregion

        #region Miscellaneous

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

        // TODO: Virtualize ETags similar to timestamps.
        // Problem: Clients may read from one server and receive an ETag, and then perform a conditional read/write using that ETag.
        // This certainly will not work if the ETag was received from a different server than where it is used.
        // Solution: Either use the main primary's ETag for all replicas (ala timestamps) or just use timestamps as ETags.
        // This means that we need to return the main ETag when a client checks the blob's properties.
        // And when a read/write is performed using an ETag, we need a two step protocol:
        // 1. Read from the selected server and see if it's virtual ETag matches the desired ETag.
        // 2. If match, then perform read/write using the read ETag returned in step 1.
        // Note that there may be a way to optimize this when the client only uses ETags that are received from the main primary.

        #endregion
    }
}
