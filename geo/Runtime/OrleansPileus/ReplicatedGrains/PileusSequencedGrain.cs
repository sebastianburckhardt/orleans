
using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using System.Text;
using Orleans;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using GeoOrleans.Runtime.Common;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Core;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.RetryPolicies;
using Microsoft.WindowsAzure.Storage.Pileus;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;
using Microsoft.WindowsAzure.Diagnostics;
using System.Diagnostics;
using GeoOrleans.Runtime.OrleansPileus.Common;
using GeoOrleans.Runtime.OrleansPileus.Interfaces;

namespace GeoOrleans.Runtime.OrleansPileus.ReplicatedGrains
{

    public interface IAppliesTo<StateObject>
    {
        void Update(StateObject state);
    }

    /// <summary>
    /// A generic grain API for eventually consistent replication.  
    /// </summary>
    public abstract class PileusSequencedGrain<StateObject> : Orleans.Grain<IGlobalState>, Orleans.IGrainWithStringKey
        where StateObject : class, new()
    {

        protected string containerName;
        protected string localContainerName;
        protected string globalContainerName;

        protected CapCloudBlobContainer localCapContainer = null;
        protected IConfigurator configuratorLocalGrain = null;
        protected Dictionary<string, CloudBlobContainer> localContainers = new Dictionary<string,CloudBlobContainer>();

        protected CapCloudBlobContainer globalCapContainer = null;
        protected IConfigurator configuratorGlobalGrain = null;
        protected Dictionary<string,CloudBlobContainer> globalContainers = new Dictionary<string,CloudBlobContainer>();

        /* This is duplicate state with Configurator */
        private Dictionary<string, CloudStorageAccount> storageAccounts;
        private ConfigurationCloudStore localBackingStore;
        private ConfigurationCloudStore globalBackingStore;
        private string configStorageSite = "devstoreaccount1";

        private ServiceLevelAgreement slaGlobal = null;
        private ServiceLevelAgreement slaLocal = null;

        private ReplicaConfiguration localReplicaConfig = null;
        private ReplicaConfiguration globalReplicaConfig = null;

        private bool isInitialisedConfig = false;

        #region Interface


        public override Task OnActivateAsync()
        {
            try
            {
                this.GetPrimaryKey(out containerName);
                localContainerName = containerName + "local";
                globalContainerName = containerName + "global";

                /* Initialise accounts TODO: duplicate code*/

                Trace.TraceInformation("Initialise storage accounts");
                storageAccounts = OrleansPileus.Common.Utils.GetStorageAccounts(false);

                Trace.TraceInformation("Initialise Configuration Cache");
                /* Initialises Configuration Cache */
                ClientRegistry.Init(storageAccounts, storageAccounts[configStorageSite]);



            }
            catch (Exception e)
            {
                Trace.TraceError(e.ToString());
            }
            return base.OnActivateAsync();

        }

        protected async Task initialiseGrain()
        {
            try
            {
                /* Create configurations */
                Trace.TraceInformation("Initialise Configurator");
                configuratorLocalGrain = ConfiguratorFactory.GetGrain(localContainerName);
                configuratorGlobalGrain = ConfiguratorFactory.GetGrain(globalContainerName);
                await configuratorGlobalGrain.startConfigurator();
                await configuratorLocalGrain.startConfigurator();

                Trace.TraceInformation("Initialise Containers");

                /* Create Containers */
       
                    foreach (string site in storageAccounts.Keys)
                {
                    CloudBlobClient blobClient = storageAccounts[site].CreateCloudBlobClient();
                    CloudBlobContainer blobContainer = blobClient.GetContainerReference(localContainerName);
                    blobContainer.CreateIfNotExists();
                    localContainers.Add(site, blobContainer);
                    blobContainer.SetPermissions(new BlobContainerPermissions()
                    {
                        PublicAccess = BlobContainerPublicAccessType.Container
                    });
                }
                    foreach (string site in storageAccounts.Keys)
                {
                    CloudBlobClient blobClient = ClientRegistry.GetAccount(site).CreateCloudBlobClient();

//                    CloudBlobClient blobClient = storageAccounts[site].CreateCloudBlobClient();

                    CloudBlobContainer blobContainer = blobClient.GetContainerReference(globalContainerName);

                    blobContainer.SetPermissions(new BlobContainerPermissions()
                    {
                        PublicAccess = BlobContainerPublicAccessType.Container
                    });

                    blobContainer.CreateIfNotExists();

                    globalContainers.Add(site, blobContainer);
                }

                Trace.TraceInformation("Get Containers obtained");
                /* Todo fix to other. Currently sets the first one as primary*/
                localCapContainer = new CapCloudBlobContainer(localContainers, "devstoreaccount1");
                globalCapContainer = new CapCloudBlobContainer(globalContainers, "devstoreaccount1");

                Trace.TraceInformation("Initialise Configurations");

                localReplicaConfig = new ReplicaConfiguration(localContainerName);
                localBackingStore = new ConfigurationCloudStore(storageAccounts[configStorageSite], localReplicaConfig);
                localBackingStore.ReadConfiguration(false);

                // Register reminder here

                globalReplicaConfig = new ReplicaConfiguration(globalContainerName);
                globalBackingStore = new ConfigurationCloudStore(storageAccounts[configStorageSite], globalReplicaConfig);
                globalBackingStore.ReadConfiguration(false);


                Trace.TraceInformation("Initialise Consistency SLA");

                slaGlobal = Utils.CreateConsistencySla(Consistency.Strong);
                slaLocal = Utils.CreateConsistencySla(Consistency.Strong);
                localCapContainer.SLA = slaLocal;
                globalCapContainer.SLA = slaGlobal;

                Trace.TraceInformation("Initialise sequenced code");

                /* Replicated Grain Code */
                Timestamp = DateTime.UtcNow;
                StalenessBound = int.MaxValue;
                worker = new BackgroundWorker(() => WriteQueuedUpdatesToStorage());
                UpdateCacheFromRaw();

                isInitialisedConfig = true;
            }
            catch (Exception e)
            {
                Trace.TraceError(e.ToString());
            }
        }

        /// Returns the current global state of this grain. May require global coordination.
        protected async Task<StateObject> GetGlobalStateAsync()
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }

            using (new TraceInterval("SequencedGrain - GetGlobalState", 0))
            {
                await RefreshLocalStateAsync(true);
            }
            return LocalState;
        }

        /// <summary>
        /// Returns the local state of this grain.
        /// This state is an aggregation of the global state (possibly somewhat stale) and tentatively performed local updates.
        /// </summary>
        /// <returns>the grain state object</returns>
        protected async Task<StateObject> GetLocalStateAsync()
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            if (!isSynchronous)
            {
                using (new TraceInterval("SequencedGrain - GetLocalState - RefreshLocalState", 0))
                {
                    await RefreshLocalStateAsync();
                }
                return LocalState;
            }
            else
            {
                // When isSynchronous flag set, actually call GetGlobalStateAsync
                return GetGlobalStateAsync().Result;
            }
        }

        /// <summary>
        /// Staleness bound: if greater than zero, allows the local state to be stale up to the specified number of milliseconds.
        /// The default setting is long.MaxValue (no staleness bound).
        /// </summary>
        protected double StalenessBound { get; set; }

        private bool isSynchronous = false;

        /// <summary>
        /// UTILITY method only. If synchronous flag is set,
        /// all operations will be "synchronous",
        /// ak: GetLocalState will call GetGlobalState
        ///     updateLocally will call UpdateGlobally
        /// </summary>
        /// <param name="pSynchronous"></param>
        public void setSynchronous(bool pSynchronous)
        {
            this.isSynchronous = pSynchronous;
        }

        private async Task RefreshLocalStateAsync(bool force = false)
        {

            Console.Write("Stateleness Bound {0} ", StalenessBound);
            using (new TraceInterval("SequencedGrain - Refresh LocalState", 0))
            {
                if (force
                    || LocalState == null
                    || StalenessBound == 0
                    || Timestamp.AddMilliseconds(StalenessBound) < DateTime.UtcNow)
                {
                    await ReadFromPrimary();
                    UpdateCacheFromRaw();
                }
            }
        }

        /// <summary>
        /// Apply update to local state immediately, and queue it for global propagation.
        /// <param name="update">An object representing the update</param>
        /// <param name="save">whether to save update to local storage before returning</param>
        /// </summary>
        public async Task UpdateLocallyAsync(IAppliesTo<StateObject> update, bool save)
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            using (new TraceInterval("SequencedGrain - Update locally", 0))
            {
                if (!isSynchronous)
                {
                    Exception ee = null;

                    try
                    {
                        using (new TraceInterval("SequencedGrain - Update locally apply update", 0))
                        {
                            update.Update(LocalState);
                        }
                    }
                    catch (Exception e)
                    {
                        ee = e;
                    }

                    if (ee != null)
                    {
                        // need to reload local state since it may have been corrupted
                        await RefreshLocalStateAsync(true);
                        throw ee;
                    }

                    pending.Add(update);
                    using (new TraceInterval("SequencedGrain - Update locally notify", 0))
                    {
                        worker.Notify();
                    }

                    using (new TraceInterval("SequencedGrain - Update locally save", 0))
                    {
                        if (save)
                            await SaveLocallyAsync();
                    }
                }
                else
                {
                    // Actually update globally if isSynchronous flag is set
                    await UpdateGloballyAsync(update);
                }
            }
        }

        private Task SaveLocallyAsync()
        {
            //note: in current impl, this save is going to master, thus not any faster than global update
            // in future impl, this will go to local storage and thus be faster
            using (new TraceInterval("SaveLocallyAsync"))
            {
                return worker.WaitForCompletion();
            }
        }


        /// <summary>
        /// Wait for all local updates to finish, and retrieve latest global state. May require global coordination.
        /// </summary>
        /// <returns></returns>
        protected async Task SynchronizeStateAsync()
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            using (new TraceInterval("SynchronizeStateAsync"))
            {
                await worker.WaitForCompletion();

                await this.RefreshLocalStateAsync(true);
            }
        }


        /// <summary>
        /// Update the global grain state directly. May require global coordination.
        /// </summary>
        protected async Task UpdateGloballyAsync(IAppliesTo<StateObject> update)
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            using (new TraceInterval("SequencedGrain - Update Globally", 0))
            {
                await worker.WaitForCompletion(); // wait for pending stores to complete

                await UpdatePrimaryStorage<bool>((StateObject state) =>
                {
                    update.Update(state);
                    return true; // dummy return value
                });
            }
        }


        /// <summary>
        /// Update the global grain state directly. May require global coordination.
        /// </summary>
        protected async Task UpdateGloballyAsync<ResultType>(Action<StateObject> update)
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            using (new TraceInterval("UpdateGloballyAsync"))
            {
                await worker.WaitForCompletion(); // wait for pending stores to complete

                await UpdatePrimaryStorage<bool>((StateObject state) =>
                {
                    update(state);
                    return true; // dummy return value
                });
            }
        }


        /// <summary>
        /// Update the global grain state directly, and return a result. May require global coordination.
        /// </summary>
        protected async Task<ResultType> UpdateGloballyAsync<ResultType>(Func<StateObject, ResultType> update)
        {
            if (!isInitialisedConfig)
            {
                await initialiseGrain();
            }


            using (new TraceInterval("UpdateGloballyAsync"))
            {
                await worker.WaitForCompletion(); // wait for pending stores to complete

                return await UpdatePrimaryStorage<ResultType>(update);
            }
        }


        /// <summary>
        /// Returns the queue of locally performed updates that are waiting to be propagated globally.
        /// </summary>
        /// <returns></returns>
        protected IEnumerable<IAppliesTo<StateObject>> PendingUpdates
        {
            get
            {
                return pending;
            }
        }

        #endregion


        #region Implementation


        public override async System.Threading.Tasks.Task OnDeactivateAsync()
        {
            var t = worker.CurrentTask();
            if (t != null) await t;
            await worker.WaitForCompletion();
            await base.OnDeactivateAsync();
        }

        private void UpdateCacheFromRaw()
        {

            using (new TraceInterval("SequencedGrain - UpdateCacheFromRaw", 0))
            {
                LocalState = ReadRawState();

                // apply all the pending updates to the cached state
                foreach (var u in pending)
                {
                    using (new TraceInterval("SequencedGrain - Apply update", 0))
                    {
                        u.Update(LocalState);
                    }
                }
            }
        }


        // the currently pending updates. 
        // we may make this persistent in future.
        private List<IAppliesTo<StateObject>> pending = new List<IAppliesTo<StateObject>>();


        private StateObject LocalState;
        private DateTime Timestamp;


        private StateObject ReadRawState()
        {
            using (new TraceInterval("SequencedGrain - Read Rawstate deserialize", 0))
            {
                var begin = DateTime.Now;

                if (this.State.Raw == null)
                    return new StateObject();
                var formatter = new BinaryFormatter();
                using (var ms = new MemoryStream(this.State.Raw))
                {
                    StateObject o = (StateObject)formatter.Deserialize(ms);
                    return o;
                }
            }
        }
        private void WriteRawState(StateObject s)
        {
            using (new TraceInterval("SequencedGrain - WriteRawState", 0))
            {
                var formatter = new BinaryFormatter();
                using (var ms = new MemoryStream())
                {
                    formatter.Serialize(ms, s);
                    ms.Position = 0;
                    this.State.Raw = ms.GetBuffer();
                    Util.Assert(this.State.Raw != null);
                }
            }

        }


        private async Task ReadFromPrimary()
        {

            using (new TraceInterval("SequencedGrain - ReadFromPrimary", 0))
            {
         //       await this.State.ReadStateAsync();
                byte[] data = await Task.Run(() => Utils.GetBlob(globalContainerName, globalCapContainer));
                if (data != null)
                {
                    this.State.Raw = data;
                }
                this.Timestamp = DateTime.UtcNow; // would be better to use Azure time stamp here
            }
        }

        private async Task WriteToPrimary()
        {

            using (new TraceInterval("SequencedGrain - Write to primary", 0))
            {
                try
                {
                     await Task.Run(() => Utils.PutBlob(globalContainerName, this.State.Raw, globalCapContainer));
        //            await this.State.WriteStateAsync();
                    this.Timestamp = DateTime.UtcNow; // would be better to use Azure time stamp here
                }
                finally
                {
                }
            }   

        }

        private async Task WriteQueuedUpdatesToStorage()
        {


            using (new TraceInterval("SequencedGrain - WriteQueuedUpdatesToStorage"))
            {
                if (pending.Count == 0)
                    return;


                int numupdates = 0;

                await UpdatePrimaryStorage<bool>((StateObject s) =>
                {
                    numupdates = pending.Count;

                    foreach (var u in pending)
                        u.Update(s);

                    return true; // dummy return value
                });

                // remove committed updates, and apply new updates to cache
                pending.RemoveRange(0, numupdates);
                UpdateCacheFromRaw();
            }
        }


        private async Task<ResultType> UpdatePrimaryStorage<ResultType>(Func<StateObject, ResultType> update)
        {


            using (new TraceInterval("UpdatePrimaryStorage"))
            {
                int retries = 10;
                while (retries-- > 0)
                {
                    // get master state
                    var s = ReadRawState();

                    // apply the update function  (or take an exception)
                    var rval = update(s);

                    // try to update master
                    try
                    {
                        WriteRawState(s);
                        await WriteToPrimary();
                        // we succeededed
                        LocalState = s;
                        return rval;
                    }
                    catch (StorageException e)
                    {

                        Console.Write("Error {0}", e.ToString());

                    }
                    catch (Exception e)
                    {
                        Console.Write("Error {0}", e.ToString());
                    } //TODO perhaps be more selective on what to catch here

                    // TODO perhaps add backoff delay

                    // on etag failure, reload and retry
                    await ReadFromPrimary();
                }

                throw new Exception("could not update primary storage");

            }
        }



        private BackgroundWorker worker;

        #endregion
    }

    // for now, we use a single storage account as the backing store for all activations
    public interface IGlobalState : IGrainState
    {
        byte[] Raw { get; set; }
    }
}