//#define REREAD_STATE_AFTER_WRITE_FAILED
//#define SHOW_EXECUTION_ENV

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Orleans.AzureUtils;
using Orleans.Counters;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans
{
    /// <summary>
    /// The abstract base class for all grains.
    /// </summary>
    public abstract class GrainBase : IAddressable
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        internal GrainState GrainState { get; set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        internal ActivationData _Data;

        /// <summary>
        /// This grain's unique identifier.
        /// </summary>
        internal GrainId Identity
        {
            get { return _Data.Grain; }
        }

        /// <summary>
        /// String representation of grain's identity including type and primary key.
        /// </summary>
        public string IdentityString
        {
            get { return _Data.Grain.ToDetailedString(); }
        }

        /// <summary>
        /// A unique identifier for the current silo.
        /// There is no semantic content to this string, but it may be useful for logging.
        /// </summary>
        public string RuntimeIdentity
        {
            get { return _Data.Silo.ToLongString(); }
        }

        /// <summary>
        /// Registers a timer to send periodic callbacks to this grain.
        /// </summary>
        /// <remarks>
        /// <para>
        /// This timer will not prevent the current grain from being deactivated.
        /// If the grain is deactivated, then the timer will be discarded.
        /// </para>
        /// <para>
        /// Until the Task returned from the asyncCallback is resolved, 
        /// the next timer tick will not be scheduled. 
        /// That is to say, timer callbacks never interleave their turns.
        /// </para>
        /// <para>
        /// The timer may be stopped at any time by calling the <c>Dispose</c> method 
        /// on the timer handle returned from this call.
        /// </para>
        /// <para>
        /// Any exceptions thrown by or faulted Task's returned from the asyncCallback 
        /// will be logged, but will not prevent the next timer tick from being queued.
        /// </para>
        /// </remarks>
        /// <param name="asyncCallback">Callback function to be invoked when timr ticks.</param>
        /// <param name="state">State object that will be passed as argument when calling the asyncCallback.</param>
        /// <param name="dueTime">Due time for first timer tick.</param>
        /// <param name="period">Period of subsequent timer ticks.</param>
        /// <returns>Handle for this Timer.</returns>
        /// <seealso cref="IOrleansTimer"/>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected IOrleansTimer RegisterTimer(Func<object, Task> asyncCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            var timer = OrleansTimerInsideGrain.FromTaskCallback(asyncCallback, state, dueTime, period);
            timer.Start();
            return timer;
        }

        /// <summary>
        /// Registers a persistent, reliable reminder to send regular notifications (reminders) to the grain.
        /// The grain must implement the <c>Orleans.IRemindable</c> interface, and reminders for this grain will be sent to the <c>ReceiveReminder</c> callback method.
        /// If the current grain is deactivated when the timer fires, a new activation of this grain will be created to receive this reminder.
        /// If an existing reminder with the same name already exists, that reminder will be overwritten with this new reminder.
        /// Reminders will always be received by one activation of this grain, even if multiple activations exist for this grain.
        /// </summary>
        /// <param name="reminderName">Name of this reminder</param>
        /// <param name="dueTime">Due time for this reminder</param>
        /// <param name="period">Frequence period for this reminder</param>
        /// <returns>Promise for Reminder handle.</returns>
        protected Task<IOrleansReminder> RegisterOrUpdateReminder(string reminderName, TimeSpan dueTime, TimeSpan period)
        {
            if (!(this is IRemindable))
            {
                //logger.Warn(ErrorCode.RS_Register_NotRemindable, msg);
                throw new InvalidOperationException(string.Format("Grain {0} is not 'IRemindable'. A grain should implement IRemindable to use the persistent reminder service", IdentityString));
            }
            if (period < Constants.MIN_REMINDER_PERIOD)
            {
                string msg = string.Format("Cannot register reminder {0}=>{1} as requested period ({2}) is less than minimum allowed reminder period ({3})", IdentityString, reminderName, period, Constants.MIN_REMINDER_PERIOD);
                //logger.Warn(ErrorCode.RS_Register_InvalidPeriod, msg);
                throw new ArgumentException(msg);
            }
            return GrainClient.InternalCurrent.RegisterOrUpdateReminder(reminderName, dueTime, period).AsTask();
        }

        /// <summary>
        /// Unregisters a previously registered reminder.
        /// </summary>
        /// <param name="reminder">Reminder to unregister.</param>
        /// <returns>Completion promise for this operation.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected Task UnregisterReminder(IOrleansReminder reminder)
        {
            return GrainClient.InternalCurrent.UnregisterReminder(reminder).AsTask();
        }

        /// <summary>
        /// Returns a previously registered reminder.
        /// </summary>
        /// <param name="reminderName">Reminder to return</param>
        /// <returns>Promise for Reminder handle.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected Task<IOrleansReminder> GetReminder(string reminderName)
        {
            return GrainClient.InternalCurrent.GetReminder(reminderName).AsTask();
        }

        /// <summary>
        /// Returns a list of all reminders registered by the grain.
        /// </summary>
        /// <returns>Promise for list of Reminders registered for this grain.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected Task<List<IOrleansReminder>> GetReminders()
        {
            return GrainClient.InternalCurrent.GetReminders().AsTask();
        }

#if !DISABLE_STREAMS

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected IEnumerable<Streams.IStreamProvider> GetStreamProviders()
        {
            return GrainClient.InternalCurrent.CurrentStreamProviderManager.GetStreamProviders();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected Streams.IStreamProvider GetStreamProvider(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException("name");
            return GrainClient.InternalCurrent.CurrentStreamProviderManager.GetProvider(name) as Streams.IStreamProvider;
        }

#endif

        /// <summary>
        /// Deactivate this activation of the grain after the current grain method call is completed.
        /// This call will mark this activation of the current grain to be deactivated and removed at the end of the current method.
        /// The next call to this grain will result in a different activation to be used, which typical means a new activation will be created automatically by the runtime.
        /// </summary>
        protected void DeactivateOnIdle()
        {
            GrainClient.InternalCurrent.DeactivateOnIdle(_Data.ActivationId);
        }

        /// <summary>
        /// Delay Deactivation of this activation at least for the specified time duration.
        /// A positive <c>timeSpan</c> value means “prevent GC of this activation for that time span”.
        /// A negative <c>timeSpan</c> value means “unlock, and make this activation available for GC again”.
        /// DeactivateOnIdle method would undo / override any current “keep alive” setting, 
        /// making this grain immediately available  for deactivation.
        /// </summary>
        protected void DelayDeactivation(TimeSpan timeSpan)
        {
            _Data.DelayDeactivation(timeSpan);
        }

        /// <summary>
        /// This method is called at the end of the process of activating a grain.
        /// It is called before any messages have been dispatched to the grain.
        /// For grains with declared persistent state, this method is called after the State property has been populated.
        /// </summary>
        public virtual Task ActivateAsync()
        {
            return TaskDone.Done;
        }

        /// <summary>
        /// This method is called at the begining of the process of deactivating a grain.
        /// </summary>
        public virtual Task DeactivateAsync()
        {
            return TaskDone.Done;
        }

        /// <summary>
        /// Returns a logger object that this grain's code can use for tracing.
        /// </summary>
        /// <returns>Name of the logger to use.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        protected OrleansLogger GetLogger(string loggerName)
        {
            return Logger.GetLogger(loggerName, Logger.LoggerType.Grain);
        }

        /// <summary>
        /// Returns a logger object that this grain's code can use for tracing.
        /// The name of the logger will be derived from the grain class name.
        /// </summary>
        /// <returns>A logger for this grain.</returns>
        protected OrleansLogger GetLogger()
        {
            string loggerName = this.GetType().Name;
            if (loggerName.EndsWith(
                GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix, 
                StringComparison.Ordinal))
            {
                loggerName = loggerName.Substring(0, loggerName.Length - GrainClientGenerator.GrainInterfaceData.ActivationClassNameSuffix.Length);
            }
            return GetLogger(loggerName);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic")]
        internal string CaptureRuntimeEnvironment()
        {
            return GrainClient.InternalCurrent.CaptureRuntimeEnvironment();
        }
    }

    /// <summary>
    /// Base class for a Grain with declared persistent state.
    /// </summary>
    /// <typeparam name="TGrainState">The interface of the persistent state object</typeparam>
    public class GrainBase<TGrainState> : GrainBase
        where TGrainState : class, IGrainState
    {
        /// <summary>
        /// Strongly typed accessor for the grain state 
        /// </summary>
        protected TGrainState State
        {
            get { return base.GrainState as TGrainState; }
        }
    }

    /// <summary>
    /// Base interface for interfaces that define persistent state properties of a grain.
    /// </summary>
    public interface IGrainState
    {
        /// <summary>
        /// Opaque value set by the storage provider representing an 'Etag' setting for the last time the state data was read from backing store.
        /// </summary>
        string Etag { get; set; }

        /// <summary>
        /// Async method to cause the current grain state data to be cleared and reset. 
        /// This will usually mean the state record is deleted from backin store, but the specific behavior is defined by the storage provider instance configured for this grain.
        /// If Etags do not match, then this operation will fail; Set Etag = <c>null</c> to indicate "always delete".
        /// </summary>
        Task ClearStateAsync();

        /// <summary>
        /// Async method to cause write of the current grain state data into backin store.
        /// If Etags do not match, then this operation will fail; Set Etag = <c>null</c> to indicate "always overwrite".
        /// </summary>
        Task WriteStateAsync();

        /// <summary>
        /// Async method to cause refresh of the current grain state data from backin store.
        /// Any previous contents of the grain state data will be overwritten.
        /// </summary>
        Task ReadStateAsync();

        /// <summary>
        /// Return a snapshot of the current grain state data, as a Dictionary of Key-Value pairs.
        /// </summary>
        Dictionary<string, object> AsDictionary();

        /// <summary>
        /// Update the current grain state data with the specified Dictionary of Key-Value pairs.
        /// </summary>
        void SetAll(Dictionary<string, object> values);
    }

    /// <summary>
    /// Base class for generated grain state classes.
    /// </summary>
    [Serializable]
    public abstract class GrainState : IGrainState
    {
        private readonly string grainTypeName;

        /// <summary>
        /// For internal (run-time) use only.
        /// NOTE: This is used for serializing the state, so ALL base class fields must be here
        /// </summary>
        internal Dictionary<string, object> AsDictionaryInternal()
        {
            var result = AsDictionary();
            return result;
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// NOTE: This is used for serializing the state, so ALL base class fields must be here
        /// </summary>
        internal void SetAllInternal(Dictionary<string, object> values)
        {
            if (values == null) values = new Dictionary<string, object>();
            SetAll(values);
        }

        /// <summary>
        /// Orleans Runtime Internal Use Only
        /// </summary>
        internal void InitState(Dictionary<string, object> values)
        {
            SetAllInternal(values); // Overwrite grain state with new values
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// Called from generated code.
        /// </summary>
        /// <returns>Deep copy of this grain state object.</returns>
        public GrainState DeepCopy()
        {
            // NOTE: Cannot use SerializationManager.DeepCopy[Inner] functionality here without StackOverflowException!
            Dictionary<string, object> values = this.AsDictionaryInternal();
            var copiedData = SerializationManager.DeepCopyInner(values) as Dictionary<string, object>;
            var copy = (GrainState)this.MemberwiseClone();
            copy.SetAllInternal(copiedData);
            return copy;
        }

        private static readonly Type WireFormatType = typeof(Dictionary<string, object>);

        /// <summary>
        /// For internal (run-time) use only.
        /// Called from generated code.
        /// </summary>
        /// <param name="stream">Stream to serialize this grain state object to.</param>
        public void SerializeTo(BinaryTokenStreamWriter stream)
        {
            Dictionary<string, object> values = this.AsDictionaryInternal();
            SerializationManager.SerializeInner(values, stream, WireFormatType);
        }

        /// <summary>
        /// For internal (run-time) use only.
        /// Called from generated code.
        /// </summary>
        /// <param name="stream">Stream to recover / repopulate this grain state object from.</param>
        public void DeserializeFrom(BinaryTokenStreamReader stream)
        {
            Dictionary<string, object> values = (Dictionary<string, object>)SerializationManager.DeserializeInner(WireFormatType, stream);
            this.SetAllInternal(values);
        }

        /// <summary>
        /// Constructs a new grain state object for a grain.
        /// </summary>
        /// <param name="reference">The type of the associated grains that use this GrainState object. Used to initialize the <c>GrainType</c> property.</param>
        protected GrainState(string grainTypeFullName)
        {
            grainTypeName = grainTypeFullName;
        }

        #region IGrainState properties & methods

        /// <summary>
        /// Opaque value set by the storage provider representing an 'Etag' setting for the last time the state data was read from backing store.
        /// </summary>
        public string Etag { get; set; }

        /// <summary>
        /// Async method to cause refresh of the current grain state data from backin store.
        /// Any previous contents of the grain state data will be overwritten.
        /// </summary>
        public async Task ReadStateAsync()
        {
            const string what = "ReadState";
            Stopwatch sw = Stopwatch.StartNew();
            GrainId grainId = GrainClient.InternalCurrent.CurrentActivation.Grain;
            IStorageProvider storage = GetCheckStorageProvider(what);
            try
            {
                await storage.ReadStateAsync(grainTypeName, GrainReference.FromGrainId(grainId) , this);
                StorageStatisticsGroup.OnStorageRead(storage, grainTypeName, grainId, sw.Elapsed);
            }
            catch (Exception exc)
            {
                StorageStatisticsGroup.OnStorageReadError(storage, grainTypeName, grainId);
                string errMsg = MakeErrorMsg(what, grainId, exc);

                storage.Log.Error((int)ErrorCode.StorageProvider_ReadFailed, errMsg, exc);
                throw new OrleansException(errMsg, exc);
            }
            finally
            {
                sw.Stop();
            }
        }

        /// <summary>
        /// Async method to cause write of the current grain state data into backin store.
        /// </summary>
        public async Task WriteStateAsync()
        {
            const string what = "WriteState";
            Stopwatch sw = Stopwatch.StartNew();
            GrainId grainId = GrainClient.InternalCurrent.CurrentActivation.Grain;
            IStorageProvider storage = GetCheckStorageProvider(what);

            Exception errorOccurred;
            try
            {
                await storage.WriteStateAsync(grainTypeName, GrainReference.FromGrainId(grainId), this);
                StorageStatisticsGroup.OnStorageWrite(storage, grainTypeName, grainId, sw.Elapsed);
                errorOccurred = null;
            }
            catch (Exception exc)
            {
                errorOccurred = exc;
            }
            // Note, we can't do this inside catch block above, because await is not permitted there.
            if (errorOccurred != null)
            {
                StorageStatisticsGroup.OnStorageWriteError(storage, grainTypeName, grainId);
                string errMsgToLog = MakeErrorMsg(what, grainId, errorOccurred);
#if DEBUG && SHOW_EXECUTION_ENV
                string env = GrainClient.InternalCurrent.CaptureRuntimeEnvironment();
                errMsgToLog = string.Format("{0} \n\n Call Execution Environment=\n\n {1}", errMsgToLog, env);
#endif
                storage.Log.Error((int)ErrorCode.StorageProvider_WriteFailed, errMsgToLog, errorOccurred);
                errorOccurred = new OrleansException(errMsgToLog, errorOccurred);

#if REREAD_STATE_AFTER_WRITE_FAILED
                // Force rollback to previously stored state
                try
                {
                    sw.Restart();
                    storage.Log.Warn((int)ErrorCode.StorageProvider_ForceReRead, "Forcing re-read of last good state for grain Type={0}", grainTypeName);
                    await storage.ReadStateAsync(grainTypeName, grainId, this);
                    StorageStatisticsGroup.OnStorageRead(storage, grainTypeName, grainId, sw.Elapsed);
                }
                catch (Exception exc)
                {
                    StorageStatisticsGroup.OnStorageReadError(storage, grainTypeName, grainId);
                    // TODO: Not sure whether it is best to ignore this secondary error, and just return the original one?
                    errMsg = MakeErrorMsg("re-read state from store after write error", grainId, exc);
                    errorOccurred = new OrleansException(errMsg, exc);
                }
#endif
            }
            sw.Stop();
            if (errorOccurred != null)
            {
                throw errorOccurred;
            }
        }

        /// <summary>
        /// Async method to cause write of the current grain state data into backin store.
        /// </summary>
        public async Task ClearStateAsync()
        {
            const string what = "ClearState";
            Stopwatch sw = Stopwatch.StartNew();
            GrainId grainId = GrainClient.InternalCurrent.CurrentActivation.Grain;
            IStorageProvider storage = GetCheckStorageProvider(what);
            try
            {
                // Clear (most likely Delete) state from external storage
                await storage.ClearStateAsync(grainTypeName, GrainReference.FromGrainId(grainId), this);
                // Null out the in-memory copy of the state
                SetAll(null);
                // Update counters
                StorageStatisticsGroup.OnStorageDelete(storage, grainTypeName, grainId, sw.Elapsed);
            }
            catch (Exception exc)
            {
                StorageStatisticsGroup.OnStorageDeleteError(storage, grainTypeName, grainId);
                string errMsg = MakeErrorMsg(what, grainId, exc);

                storage.Log.Error((int)ErrorCode.StorageProvider_DeleteFailed, errMsg, exc);
                throw new OrleansException(errMsg, exc);
            }
            finally
            {
                sw.Stop();
            }
        }

        /// <summary>
        /// Converts this property bag into a dictionary.
        /// Overridded with type-specific implementation in generated code.
        /// </summary>
        /// <returns>A Dictionary from string property name to property value.</returns>
        // TODO: Eventually should be able to make this method abstract
        public virtual Dictionary<string, object> AsDictionary()
        {
            var result = new Dictionary<string, object>();
            return result;
        }

        /// <summary>
        /// Populates this property bag from a dictionary.
        /// Overridded with type-specific implementation in generated code.
        /// </summary>
        /// <param name="values">The Dictionary from string to object that contains the values
        /// for this property bag.</param>
        // TODO: Eventually should be able to make this method abstract
        public virtual void SetAll(Dictionary<string, object> values)
        {
            // Nothing to do here. 
            // All relevant implementation logic for handling application data will be in sub-class.
            // All system data is handled by SetAllInternal method, which calls this.
        }
        #endregion

        private string MakeErrorMsg(string what, GrainId grainId, Exception exc)
        {
            string errorCode = AzureStorageUtils.ExtractRestErrorCode(exc);
            return string.Format("Error from storage provider during {0} for grain Type={1} Pk={2} Id={3} Error={4}\n {5}",
                what, grainTypeName, grainId.ToDetailedString(), grainId, errorCode, Logger.PrintException(exc));
        }

        private IStorageProvider GetCheckStorageProvider(string what)
        {
            IStorageProvider storage = GrainClient.InternalCurrent.CurrentStorageProvider;
            if (storage == null)
            {
                throw new OrleansException(string.Format(
                    "Cannot {0} - No storage provider configured for grain Type={1}", what, grainTypeName));
            }
            return storage;
        }
    }
}
