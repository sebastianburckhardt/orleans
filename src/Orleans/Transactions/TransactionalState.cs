
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Core;
using Orleans.Facet;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Storage;

namespace Orleans.Transactions
{
    /// <summary>
    /// Stateful facet that respects Orleans transaction semantics
    /// </summary>
    /// <typeparam name="TState"></typeparam>
    public class TransactionalState<TState> : ITransactionalState<TState>, IConfigurableTransactionalState, ITransactionalResource, IGrainBinder
        where TState : class, new()
    {
        private readonly ITransactionAgent transactionAgent;
        private readonly SerializationManager serializationManager;

        private TransactionalStateConfiguration config;
        private Grain grain;
        private IStorage<TransactionalStateRecord<TState>> storage;

        private Logger logger;
        private ITransactionalResource transactionalResource;

        // For each transaction, the copy of the state it is currently acting upon.
        private readonly Dictionary<long, TState> transactionCopy;

        // Access to base.State must always be protected by this lock.
        // Prevent multiple concurrent writes to persistence store.
        // Note that the TransactionalGrain methods are always interleaved.
        private readonly AsyncLock persistenceLock;

        // In-memory version of the persistent state.
        private readonly SortedDictionary<long, LogRecord<TState>> log;
        private TState value;
        private TransactionalResourceVersion version;
        private long stableVersion;

        private long writeLowerBound;

        public TState State => GetState();

        public TransactionalState(ITransactionAgent transactionAgent, SerializationManager serializationManager)
        {
            this.transactionAgent = transactionAgent;
            this.serializationManager = serializationManager;
            transactionCopy = new Dictionary<long, TState>();
            persistenceLock = new AsyncLock();
            log = new SortedDictionary<long, LogRecord<TState>>();
        }

        public void Configure(TransactionalStateConfiguration transactionalStateConfiguration)
        {
            this.config = transactionalStateConfiguration;
        }

        public void Configure(FacetConfiguration facetConfiguration)
        {
            Configure(new TransactionalStateConfiguration(facetConfiguration));
        }

        public async Task<bool> Prepare(long transactionId, TransactionalResourceVersion? writeVersion,
            TransactionalResourceVersion? readVersion)
        {
            this.transactionCopy.Remove(transactionId);

            long wlb = 0;
            if (readVersion.HasValue)
            {
                this.writeLowerBound = Math.Max(this.writeLowerBound, readVersion.Value.TransactionId - 1);
                wlb = this.writeLowerBound;
            }

            if (!ValidateWrite(writeVersion))
            {
                return false;
            }

            if (!ValidateRead(transactionId, readVersion))
            {
                return false;
            }

            bool success = true;

            // Note that the checks above will need to be done again
            // after we aquire the lock because things could change in the meantime.
            using (await this.persistenceLock.LockAsync())
            {
                try
                {
                    if (!ValidateWrite(writeVersion))
                    {
                        return false;
                    }

                    if (!ValidateRead(transactionId, readVersion))
                    {
                        return false;
                    }

                    // check if we need to do a log write
                    if (this.storage.State.LastPrepared >= transactionId && this.storage.State.WriteLowerBound >= wlb)
                    {
                        // Logs already persisted, nothing to do here
                        return true;
                    }

                    // For simplicity and (potentially) efficiency we write everyting we have even
                    // though for correctness we just need logs for t and earlier unprepared transactions
                    await Persist(this.storage.State.StableVersion, wlb);
                }
                catch (Exception ex)
                {
                    logger.Error(ErrorCode.Transactions_PrepareFailed, $"Prepare of transaction {transactionId} failed.", ex);
                    success = false;
                }
            }

            if (!success)
            {
                await Abort(transactionId);
            }

            return success;
        }

        /// <summary>
        /// Implementation of ITransactionalGrain Abort method. See interface documentation for more details.
        /// </summary>
        public Task Abort(long transactionId)
        {
            // Rollback t if it has changed the grain
            if (this.log.ContainsKey(transactionId))
            {
                Rollback(transactionId);
            }
            return TaskDone.Done;
        }

        /// <summary>
        /// Implementation of ITransactionalGrain Commit method. See interface documentation for more details.
        /// </summary>
        public async Task Commit(long transactionId)
        {
            // Learning that t is committed implies that all pending transactions before t also committed
            if (transactionId > this.stableVersion)
            {
                using (await this.persistenceLock.LockAsync())
                {
                    if (transactionId <= this.storage.State.StableVersion)
                    {
                        // Transaction commit already persisted.
                        return;
                    }

                    // Trim the logs to remove old versions. 
                    // Note that we try to keep the highest version that is below or equal to the ReadOnlyTransactionId
                    // so that we can use it to serve read only transactions.
                    long highestKey = transactionId;
                    foreach (var key in this.log.Keys)
                    {
                        if (key > this.transactionAgent.ReadOnlyTransactionId)
                        {
                            break;
                        }

                        highestKey = key;
                    }

                    if (this.log.Count != 0)
                    {
                        List<KeyValuePair<long, LogRecord<TState>>> records = this.log.TakeWhile(kvp => kvp.Key < highestKey).ToList();
                        records.ForEach(kvp => this.log.Remove(kvp.Key));
                    }

                    await Persist(transactionId, this.writeLowerBound);
                }
            }
        }


        /// <summary>
        /// Find the appropriate version of the state to serve for this transaction.
        /// We enforce reads in transaction id order, hence we find the version written by the highest 
        /// transaction less than or equal to this one
        /// </summary>
        private bool TryGetVersion(long transactionId, out TState readState, out TransactionalResourceVersion readVersion)
        {
            readState = this.value;
            readVersion = this.version;
            bool versionAvailable = this.version.TransactionId <= transactionId;

            LogRecord<TState> logRecord = null;
            foreach (KeyValuePair<long, LogRecord<TState>> kvp in this.log)
            {
                if (kvp.Key > transactionId)
                {
                    break;
                }
                logRecord = kvp.Value;
            }

            if (logRecord == null) return versionAvailable;

            readState = logRecord.NewVal;
            readVersion = logRecord.Version;

            return true;
        }

        /// <summary>
        /// Transactional Read procedure.
        /// </summary>
        private TState GetState()
        {
            Restore();

            var info = TransactionContext.GetTransactionInfo();

            TState state;
            if (this.transactionCopy.TryGetValue(info.TransactionId, out state))
            {
                return state;
            }

            TState readState;
            TransactionalResourceVersion readVersion;
            if (!TryGetVersion(info.TransactionId, out readState, out readVersion))
            {
                // This can only happen if old versions are gone due to checkpointing.
                throw new OrleansTransactionVersionDeletedException(info.TransactionId);
            }

            if (info.IsReadOnly && readVersion.TransactionId > this.stableVersion)
            {
                throw new OrleansTransactionUnstableVersionException(info.TransactionId);
            }

            if (readVersion.TransactionId == info.TransactionId)
            {
                // Just reading our own write here.
                // Sanity check to see if there's a lost write.
                int resourceWriteNumber;
                if(info.WriteSet.TryGetValue(transactionalResource, out resourceWriteNumber) && resourceWriteNumber > readVersion.WriteNumber)
                {
                    // Context has record of more writes than we have, some writes must be lost.
                    throw new OrleansTransactionAbortedException(info.TransactionId, "Lost Write");
                }
            }
            else
            {
                TransactionalResourceVersion resourceReadVersion;
                if (info.ReadSet.TryGetValue(transactionalResource, out resourceReadVersion) && resourceReadVersion != readVersion)
                {
                    // Uh-oh. Read two different versions of the grain.
                    throw new OrleansValidationFailedException(info.TransactionId);
                }

                info.ReadSet[transactionalResource] = readVersion;

                if (readVersion.TransactionId != info.TransactionId &&
                    readVersion.TransactionId > this.storage.State.StableVersion)
                {
                    info.DependentTransactions.Add(readVersion.TransactionId);
                }
            }

            writeLowerBound = Math.Max(writeLowerBound, info.TransactionId - 1);

            var copy = (TState) this.serializationManager.DeepCopy(readState);

            if (!info.IsReadOnly)
            {
                this.transactionCopy[info.TransactionId] = copy;
            }

            return copy;
        }

        /// <summary>
        /// Transactional Write procedure.
        /// </summary>
        public void Save()
        {
            var info = TransactionContext.GetTransactionInfo();
            if (info.IsReadOnly)
            {
                // For obvious reasons...
                throw new OrleansReadOnlyViolatedException(info.TransactionId);
            }

            Restore();

            var copiedValue = this.transactionCopy[info.TransactionId];

            //
            // Validation
            //

            if (this.version.TransactionId > info.TransactionId || this.writeLowerBound >= info.TransactionId)
            {
                // Prevent cycles. Wait-die
                throw new OrleansTransactionWaitDieException(info.TransactionId);
            }

            TransactionalResourceVersion nextVersion = TransactionalResourceVersion.Create(info.TransactionId,
                this.version.TransactionId == info.TransactionId ? this.version.WriteNumber + 1 : 1);

            //
            // Update Transaction Context
            //
            int writeNumber;
            info.WriteSet.TryGetValue(this.transactionalResource, out writeNumber);
            info.WriteSet[this.transactionalResource] = writeNumber + 1;

            if (this.version.TransactionId != info.TransactionId && this.version.TransactionId > this.stableVersion)
            {
                info.DependentTransactions.Add(this.version.TransactionId);
            }

            //
            // Modify the State
            //
            if (!this.log.ContainsKey(info.TransactionId))
            {
                LogRecord<TState> r = new LogRecord<TState>();
                this.log[info.TransactionId] = r;
            }

            LogRecord<TState> logRecord = this.log[info.TransactionId];
            logRecord.NewVal = copiedValue;
            logRecord.Version = nextVersion;
            this.value = copiedValue;
            this.version = nextVersion;

            this.transactionCopy.Remove(info.TransactionId);
        }

        /// <summary>
        /// Undo writes to restore state to pre transaction value.
        /// </summary>
        private void Rollback(long transactionId)
        {
            List<KeyValuePair<long,LogRecord<TState>>> records = this.log.SkipWhile(kvp => kvp.Key < transactionId).ToList();
            foreach(KeyValuePair<long, LogRecord<TState>> kvp in records)
            {
                this.log.Remove(kvp.Key);
                this.transactionCopy.Remove(kvp.Key);
            }

            if (this.log.Count > 0)
            {
                LogRecord<TState> lastLogRecord = this.log.Values.Last();
                this.version = lastLogRecord.Version;
                this.value = lastLogRecord.NewVal;
            }
            else
            {
                this.version = TransactionalResourceVersion.Create(0, 0);
                this.value = new TState();
            }
        }

        /// <summary>
        /// Check with the transaction agent and rollback any aborted transaction.
        /// </summary>
        private void Restore()
        {
            foreach (var transactionId in this.log.Keys)
            {
                if (transactionId > this.storage.State.StableVersion && transactionAgent.IsAborted(transactionId))
                {
                    Rollback(transactionId);
                    return;
                }
            }
        }

        /// <summary>
        /// Write log in the format needed for the persistence framework and copy to the persistent state interface.
        /// </summary>
        private void RecordInPersistedLog()
        {
            this.storage.State.Logs.Clear();
            foreach (KeyValuePair<long, LogRecord<TState>> kvp in this.log)
            {
                this.storage.State.Logs[kvp.Key] = kvp.Value.NewVal;
            }
        }

        /// <summary>
        /// Read Log from persistent state interface.
        /// </summary>
        private void RevertToPersistedLog()
        {
            this.log.Clear();
            foreach (KeyValuePair<long, TState> kvp in this.storage.State.Logs)
            {
                this.log[kvp.Key] = new LogRecord<TState>
                {
                    NewVal = kvp.Value,
                    Version = TransactionalResourceVersion.Create(kvp.Key, 1)
                };
            }
        }

        private async Task Persist(long newStableVersion, long newWriteLowerBound)
        {
            try
            {
                RecordInPersistedLog();

                // update storage state
                TransactionalStateRecord<TState> storageState = this.storage.State;
                storageState.Value = this.value;
                storageState.Version = this.version;
                storageState.LastPrepared = this.storage.State.Version.TransactionId;
                storageState.StableVersion = newStableVersion;
                storageState.WriteLowerBound = newWriteLowerBound;

                await this.storage.WriteStateAsync();

                this.stableVersion = newStableVersion;
            }
            catch (Exception)
            {
                try
                {
                    await this.storage.ReadStateAsync();
                    DoRecovery();
                }
                catch (Exception)
                {
                    //TODO: can't call deactivate on idle here, need to throw exception that triggers deactivation. - jbragg
                    grain.Runtime.DeactivateOnIdle(grain);
                    throw;
                }
                throw;
            }
        }

        private bool ValidateWrite(TransactionalResourceVersion? writeVersion)
        {
            if (!writeVersion.HasValue)
                return true;

            // Validate that we still have all of the transaction's writes.
            LogRecord<TState> logRecord;
            return this.log.TryGetValue(writeVersion.Value.TransactionId, out logRecord) && logRecord.Version == writeVersion.Value;
        }

        private bool ValidateRead(long transactionId, TransactionalResourceVersion? readVersion)
        {
            if (!readVersion.HasValue)
                return true;

            foreach (var key in this.log.Keys)
            {
                if (key >= transactionId)
                {
                    break;
                }

                if (key > readVersion.Value.TransactionId && key < transactionId)
                {
                    return false;
                }
            }

            if (readVersion.Value.TransactionId == 0) return readVersion.Value.WriteNumber == 0;
            LogRecord<TState> logRecord;
            // If version read by the transaction is lost, return false.
            if (!this.log.TryGetValue(readVersion.Value.TransactionId, out logRecord)) return false;
            // If version is not same it was overridden by the same transaction that originally wrote it.
            return logRecord.Version == readVersion.Value;
        }

        private void DoRecovery()
        {
            TransactionalStateRecord<TState> storageState = this.storage.State;
            this.stableVersion = storageState.StableVersion;
            this.writeLowerBound = storageState.WriteLowerBound;
            this.version = storageState.Version;
            this.value = storageState.Value;
            RevertToPersistedLog();

            writeLowerBound = 0;

            // Rollback any known aborted transactions
            Restore();
        }

        /// <summary>
        /// Bind facet to grain
        /// </summary>
        /// <param name="containerGrain"></param>
        /// <returns></returns>
        public async Task BindAsync(Grain containerGrain)
        {
            this.grain = containerGrain;
            this.logger = LogManager.GetLogger(grain.GetType().Name).GetSubLogger(GetType().Name);

            // bind extension to grain
            IProviderRuntime runtime = grain.Runtime.ServiceProvider.GetRequiredService<IProviderRuntime>();
            Tuple<TransactionalExtension, ITransactionalExtension> boundExtension = await runtime.BindExtension<TransactionalExtension, ITransactionalExtension>(() => new TransactionalExtension());
            boundExtension.Item1.Register(this.config.StateName, this);
            this.transactionalResource = boundExtension.Item2.AsTransactionalResource(this.config.StateName);

            // wire up storage provider
            IStorageProvider storageProvider = string.IsNullOrWhiteSpace(this.config.StorageProviderName)
                ? grain.Runtime.ServiceProvider.GetRequiredService<IStorageProvider>()
                : grain.Runtime.ServiceProvider.GetServiceByKey<string, IStorageProvider>(this.config.StorageProviderName);
            this.storage = new StateStorageBridge<TransactionalStateRecord<TState>>(StoredName(), grain.GrainReference, storageProvider);

            // load inital state
            await this.storage.ReadStateAsync();

            // recover state
            DoRecovery();
        }

        private string StoredName()
        {
            // TODO: Improve naming.  FullName includes assembly names and versions if type is generic. - jbragg
            return $"{this.grain.GetType().FullName}-{this.config.StateName}";
        }

        public bool Equals(ITransactionalResource other)
        {
            return transactionalResource.Equals(other);
        }
    }

    [Serializable]
    public class TransactionalStateRecord<TState>
        where TState : class, new()
    {
        // The transactionId of the transaction that wrote the current value
        public TransactionalResourceVersion Version { get; set; }

        // The last known committed version
        public long StableVersion { get; set; }

        // The transactionId of the last transaction prepared
        public long LastPrepared { get; set; }

        // Writes of transactions with Id equal or below this will be rejected
        public long WriteLowerBound { get; set; }

        public SortedDictionary<long, TState> Logs { get; set; } = new SortedDictionary<long, TState>();

        public TState Value { get; set; } = new TState();
    }
}
