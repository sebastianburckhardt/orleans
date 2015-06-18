using System.Collections.Generic;
using System.Threading.Tasks;
using System;
using System.Linq;
using System.Text;
using Orleans;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using GeoOrleans.Runtime.Common;
using GeoOrleans.Runtime.ClusterProtocol;
using GeoOrleans.Runtime.ClusterProtocol.Grains;
using Orleans.Streams;

#pragma warning disable 1998

namespace GeoOrleans.Runtime.Strawman.ReplicatedGrains
{
  
 
    public interface IAppliesTo<StateObject>
    {
        void Update(StateObject state);
    }

    
    /// <summary>
    /// A generic grain API for eventually consistent replication.  
    /// </summary>
    public abstract class SequencedGrain<StateObject> : Orleans.Grain<IGlobalState>
        where StateObject : class, new() 
    {
        #region Interface

        
        /// Returns the current global state of this grain. May require global coordination.
        protected async Task<StateObject> GetGlobalStateAsync()
        {

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
        public async Task UpdateLocallyAsync(IAppliesTo<StateObject> update, bool save )
        {

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
                        writebackworker.Notify();
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
            using (new TraceInterval("SaveLocallyAsync")) {
            return writebackworker.WaitForCompletion();
        }
        }


        /// <summary>
        /// Wait for all local updates to finish, and retrieve latest global state. May require global coordination.
        /// </summary>
        /// <returns></returns>
        protected async Task SynchronizeStateAsync()
        {
            using (new TraceInterval("SynchronizeStateAsync")) { 
            await writebackworker.WaitForCompletion();

            await this.RefreshLocalStateAsync(true);
        }
        }


        /// <summary>
        /// Update the global grain state directly. May require global coordination.
        /// </summary>
        protected async Task UpdateGloballyAsync(IAppliesTo<StateObject> update)
        {

            using (new TraceInterval("SequencedGrain - Update Globally", 0))
            {
                await writebackworker.WaitForCompletion(); // wait for pending stores to complete

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
            using (new TraceInterval("UpdateGloballyAsync"))
            {
                await writebackworker.WaitForCompletion(); // wait for pending stores to complete

                await UpdatePrimaryStorage<bool>((StateObject state) =>
                {
                    update(state);
                    return true; // dummy return value
                });

                if (notificationworker != null)
                    notificationworker.Notify();
            }
        }
      

        /// <summary>
        /// Update the global grain state directly, and return a result. May require global coordination.
        /// </summary>
        protected async Task<ResultType> UpdateGloballyAsync<ResultType>(Func<StateObject, ResultType> update)
        {
            using (new TraceInterval("UpdateGloballyAsync"))
            {
                await writebackworker.WaitForCompletion(); // wait for pending stores to complete

                var result = await UpdatePrimaryStorage<ResultType>(update);

                if (notificationworker != null)
                    notificationworker.Notify();

                return result;
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


        private BackgroundWorker writebackworker;

        private BackgroundWorker notificationworker;
        private LocalVersion? lastnotification;

 
        public override async System.Threading.Tasks.Task OnActivateAsync()
        {
            Timestamp = DateTime.UtcNow;
            await base.OnActivateAsync();
            StalenessBound = int.MaxValue ;
            writebackworker = new BackgroundWorker(() => WriteQueuedUpdatesToStorage());
            UpdateCacheFromRaw();
        }

        public override async System.Threading.Tasks.Task OnDeactivateAsync()
        {
            var t = writebackworker.CurrentTask();
            if (t != null) await t;
            await writebackworker.WaitForCompletion();
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
        private long Version;
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
                    GeoOrleans.Runtime.Common.Util.Assert(this.State.Raw != null);
                }
            }

        }

        private async Task ReadFromPrimary()
        {

            using (new TraceInterval("SequencedGrain - ReadFromPrimary", 0))
            {
                try
                {
                    await this.State.ReadStateAsync();
                    this.Timestamp = DateTime.UtcNow; // would be better to use Azure time stamp here
                    this.Version = this.State.Version;

                    SiloRep.Instance.RecordActivity("read from primary", false);
                }
                catch (Exception e)
                {
                    SiloRep.Instance.RecordActivity("read from primary", true);

                    throw e;
                }
            }
        }
        
        private async Task WriteToPrimary()
        {

            using (new TraceInterval("SequencedGrain - Write to primary", 0))
            {
                try
                {
                    await this.State.WriteStateAsync();
                    this.Timestamp = DateTime.UtcNow; // would be better to use Azure time stamp here
                    this.Version = this.State.Version;

                    SiloRep.Instance.RecordActivity("write to primary", false);
                }
                catch (Exception e)
                {
                    SiloRep.Instance.RecordActivity("write to primary", true);

                    throw e;
                }
            }

        }
        
        private async Task WriteQueuedUpdatesToStorage()
        {

            using (new TraceInterval("SequencedGrain - WriteQueuedUpdatesToStorage")) { 
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

                if (notificationworker != null)
                    notificationworker.Notify();
            }
        }
        

        private async Task<ResultType> UpdatePrimaryStorage<ResultType>(Func<StateObject,ResultType> update)
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

                    // increment version
                    State.Version++;

                    // try to update master
                    try
                    {
                        WriteRawState(s);
                        await WriteToPrimary();

                        // we succeededed
                        LocalState = s;

                        // return result
                        return rval;
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

       /*

        public void AddOrConfirmListener(IViewMaintenanceListener<StateObject> listener)
        {
            //TODO
            if (listeners == null)
            {
                ReceiveStateChangeNotifications(true);
            }
        }

        public bool RemoveListener(IViewMaintenanceListener listener)
        {
            //TODO
        }
        
        private Dictionary<string,IViewMaintenanceListener<StateObject>> listeners;
        * */
   
        private void ReceiveStateChangeNotifications(bool enable)
        {
            if (enable & notificationworker == null)
            {
                notificationworker = new BackgroundWorker(() => TalkToListeners());
                var bg = StartNotificationsOnNextTurn();
            }
            else if (!enable && notificationworker != null)
                notificationworker = null;
        }

        private async Task StartNotificationsOnNextTurn()
        {
            await Task.Delay(0);
            if (notificationworker != null)
                notificationworker.Notify();
        }

        private async Task TalkToListeners()
        {
            var currentversion = new LocalVersion()
            {
                GlobalVersion = Version,
                NumLocalUpdates = this.pending.Count()
            };

            // if the current version is not newer than the last sent version, we are done
            if (lastnotification.HasValue
                && lastnotification.Value.CompareTo(currentversion) >= 0)
                return;

            try
            {
                // TODO OnStateChangeAsync(currentversion);
                lastnotification = currentversion;
            }
            catch (Exception e)
            {
                // TODO handle better
                Console.Write("NotifyNewStateVersion() Error {0}", e.ToString());
            }
        }


        #endregion
    }

       // for now, we use a single storage account as the backing store for all activations
    public interface IGlobalState : IGrainState
    {
        long Version { get; set; }
        byte[] Raw { get; set; }
    }

    public struct LocalVersion : IComparable<LocalVersion>
    {
        public long GlobalVersion;
        public long NumLocalUpdates;

        public override string ToString()
        {
            if (NumLocalUpdates == 0)
                return GlobalVersion.ToString();
            else
                return string.Format("{0}.{1}", GlobalVersion, NumLocalUpdates);
        }

        public int CompareTo(LocalVersion other)
        {
            int x = GlobalVersion.CompareTo(other.GlobalVersion);
            if (x == 0)
                x = NumLocalUpdates.CompareTo(other.NumLocalUpdates);
            return x;
        }
 
    }

    public interface IViewMaintenanceListener<ViewType>
    {
        string Identity { get; }
        Task OnInit(ViewType state);
        Task OnDelta(IAppliesTo<ViewType> delta);
        Task OnUndo(IAppliesTo<ViewType> delta);
        Task OnEnd(Exception e = null);

    }

}
