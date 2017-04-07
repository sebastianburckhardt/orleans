
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans.Serialization;
using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Transactions
{

    public class TransactionalGrainState<T>
    {
        // The transactionId of the transaction that wrote the current value
        public TransactionalResourceVersion Version { get; set; }

        // The last known committed version
        public long StableVersion { get; set; }

        // The transactionId of the last transaction prepared
        public long LastPrepared { get; set; }

        // Writes of transactions with Id equal or below this will be rejected
        public long WriteLowerBound { get; set; }

        public SortedDictionary<long, T> Logs { get; set; }

        public T Value { get; set; }
    }

    /// <summary>
    /// Orleans Transactional grain implementation class.
    /// </summary>
    public abstract class TransactionalGrain<TGrainState> : Grain<TransactionalGrainState<TGrainState>>, ITransactionalGrain where TGrainState : new()
    {
        protected new TGrainState State => GetState();

        private ITransactionAgent transactionAgent;
        private ITransactionalResource grainAsTransactionalResource;

        // For each transaction, the copy of the state it is currently acting upon.
        private readonly Dictionary<long, TGrainState> transactionCopy;

        // Access to base.State must always be protected by this lock.
        // Prevent multiple concurrent writes to persistence store.
        // Note that the TransactionalGrain methods are always interleaved.
        private readonly AsyncLock persistenceLock;

        private SerializationManager serializationManager;

        // In-memory version of the persistent state.
        private SortedDictionary<long, LogRecord<TGrainState>> log { get; set; }
        private TGrainState value;
        private TransactionalResourceVersion version;
        private long stableVersion;

        private long writeLowerBound;


        protected TransactionalGrain()
        {
            transactionCopy = new Dictionary<long, TGrainState>();
            persistenceLock = new AsyncLock();
            log = new SortedDictionary<long, LogRecord<TGrainState>>();
        }

        public override Task OnActivateAsync()
        {
            this.grainAsTransactionalResource = this.AsReference<ITransactionalGrain>().AsTransactionalResource();
            this.transactionAgent = ServiceProvider.GetRequiredService<ITransactionAgent>();
            this.serializationManager = ServiceProvider.GetRequiredService<SerializationManager>();

            Recovery();

            return TaskDone.Done;
        }

        #region ITransactionalGrain
        /// <summary>
        /// Implementation of ITransactionalGrain Prepare method. See interface documentation for more details.
        /// </summary>
        public async Task<bool> Prepare(long transactionId, TransactionalResourceVersion? writeVersion, TransactionalResourceVersion? readVersion)
        {
            if (this.transactionCopy.ContainsKey(transactionId))
            {
                this.transactionCopy.Remove(transactionId);
            }

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
                    if (base.State.LastPrepared >= transactionId && base.State.WriteLowerBound >= wlb)
                    {
                        // Logs already persisted, nothing to do here
                        return true;
                    }

                    // For simplicity and (potentially) efficiency we write everyting we have even
                    // though for correctness we just need logs for t and earlier unprepared transactions
                    await Persist(base.State.StableVersion, wlb);
                }
                catch (Exception)
                {
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
                    if (transactionId <= base.State.StableVersion)
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

                    while (this.log.Count > 0 && this.log.Keys.First() < highestKey)
                    {
                        long tId = this.log.Keys.First();
                        this.log.Remove(tId);
                    }

                    await Persist(transactionId, this.writeLowerBound);
                }
            }
        }

        #endregion

        /// <summary>
        /// Transactional Read procedure.
        /// </summary>
        private TGrainState GetState()
        {
            Restore();

            var info = TransactionContext.GetTransactionInfo();

            if (this.transactionCopy.ContainsKey(info.TransactionId))
            {
                return this.transactionCopy[info.TransactionId];
            }

            // Find the appropriate version of the state to serve for this transaction.
            // We enforce reads in transaction id order, hence we find the version written by the highest 
            // transaction less than or equal to this one
            var readState = this.value;
            var readVersion = this.version;
            bool versionAvailable = this.version.TransactionId <= info.TransactionId;

            foreach (var key in this.log.Keys)
            {
                if (key > info.TransactionId)
                {
                    break;
                }

                versionAvailable = true;
                readState = this.log[key].NewVal;
                readVersion = this.log[key].Version;
            }

            if (!versionAvailable)
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
                if (info.WriteSet.ContainsKey(grainAsTransactionalResource) && info.WriteSet[grainAsTransactionalResource] > readVersion.WriteNumber)
                {
                    // Context has record of more writes than we have, some writes must be lost.
                    throw new OrleansTransactionAbortedException(info.TransactionId, "Lost Write");
                }
            }
            else
            {
                if (info.ReadSet.ContainsKey(grainAsTransactionalResource) && info.ReadSet[grainAsTransactionalResource] != readVersion)
                {
                    // Uh-oh. Read two different versions of the grain.
                    throw new OrleansValidationFailedException(info.TransactionId);
                }

                info.ReadSet[grainAsTransactionalResource] = readVersion;

                if (readVersion.TransactionId != info.TransactionId && readVersion.TransactionId > base.State.StableVersion)
                {
                    info.DependentTransactions.Add(readVersion.TransactionId);
                }
            }

            writeLowerBound = Math.Max(writeLowerBound, info.TransactionId - 1);

            var copy = (TGrainState)this.serializationManager.DeepCopy(readState);

            if (!info.IsReadOnly)
            {
                this.transactionCopy[info.TransactionId] = copy;
            }

            return copy;
        }

        /// <summary>
        /// Transactional Write procedure.
        /// </summary>
        protected void SaveState()
        {
            var info = TransactionContext.GetTransactionInfo();
            if (info.IsReadOnly)
            {
                // For obvious reasons...
                throw new OrleansReadOnlyViolatedException(info.TransactionId);
            }

            Restore();

            var transactionValue = this.transactionCopy[info.TransactionId];
            
            //
            // Validation
            //

            if (this.version.TransactionId > info.TransactionId || this.writeLowerBound >= info.TransactionId)
            {
                // Prevent cycles. Wait-die
                throw new OrleansTransactionWaitDieException(info.TransactionId);
            }

            TransactionalResourceVersion newVersion = TransactionalResourceVersion.Create(info.TransactionId,
                this.version.TransactionId == info.TransactionId ? this.version.WriteNumber + 1 : 1);

            //
            // Update Transaction Context
            //
            if (!info.WriteSet.ContainsKey(this.grainAsTransactionalResource))
            {
                info.WriteSet.Add(this.grainAsTransactionalResource, 0);
            }
            info.WriteSet[this.grainAsTransactionalResource]++;

            if (this.version.TransactionId != info.TransactionId && this.version.TransactionId > this.stableVersion)
            {
                info.DependentTransactions.Add(this.version.TransactionId);
            }

            //
            // Modify the State
            //
            if (!this.log.ContainsKey(info.TransactionId))
            {
                LogRecord<TGrainState> r = new LogRecord<TGrainState>();
                this.log[info.TransactionId] = r;
            }

            this.log[info.TransactionId].NewVal = transactionValue;
            this.log[info.TransactionId].Version = newVersion;
            this.value = transactionValue;
            this.version = newVersion;

            this.transactionCopy.Remove(info.TransactionId);
        }

        /// <summary>
        /// Undo writes to restore state to pre transaction value.
        /// </summary>
        private void Rollback(long transactionId)
        {
            while (this.log.Count > 0 && this.log.Keys.Last() >= transactionId)
            {
                long tId = this.log.Keys.Last();

                this.log.Remove(tId);


                this.transactionCopy.Remove(tId);
            }

            if (this.log.Count > 0)
            {
                this.version = this.log.Values.Last().Version;
                this.value = this.log.Values.Last().NewVal;
            }
            else
            {
                this.version = TransactionalResourceVersion.Create(0,0);
                this.value = new TGrainState();
            }
        }

        /// <summary>
        /// Check with the transaction agent and rollback any aborted transaction.
        /// </summary>
        private void Restore()
        {
            foreach (var transactionId in this.log.Keys)
            {
                if (transactionId > base.State.StableVersion && transactionAgent.IsAborted(transactionId))
                {
                    Rollback(transactionId);
                    return;
                }
            }
        }

        /// <summary>
        /// Write log in the format needed for the persistence framework and copy to the persistent state interface.
        /// </summary>
        private void SerializeLogs()
        {
            base.State.Logs.Clear();
            foreach (var key in this.log.Keys)
            {
                var record = this.log[key];
                base.State.Logs[key] = record.NewVal;
            }
        }

        /// <summary>
        /// Read Log from persistent state interface.
        /// </summary>
        private void DeserializeLogs()
        {
            this.log.Clear();
            if (base.State.Logs == null)
            {
                base.State.Logs = new SortedDictionary<long, TGrainState>();
            }

            foreach (var key in base.State.Logs.Keys)
            {
                this.log[key] = new LogRecord<TGrainState>
                {
                    NewVal = base.State.Logs[key],
                    Version = TransactionalResourceVersion.Create(key, 1)
                };
            }
        }

        private async Task Persist(long persistVersion, long persistWriteLowerBound)
        {
            Exception error = null;

            try
            {
                SerializeLogs();
                base.State.Value = this.value;
                base.State.Version = this.version;
                base.State.LastPrepared = base.State.Version.TransactionId;
                base.State.StableVersion = persistVersion;
                base.State.WriteLowerBound = persistWriteLowerBound;

                await base.WriteStateAsync();

                this.stableVersion = persistVersion;
            }
            catch (Exception e)
            {
                error = e;
                // TODO: Log the error here to capture original stack trace
            }

            if (error != null)
            {
                // TODO: what happens if we also get an exception here? state is not guaranteed to be in sync with storage.
                await base.ReadStateAsync();
                Recovery();
                throw error;
            }
        }

        private bool ValidateWrite(TransactionalResourceVersion? writeVersion)
        {
            if (!writeVersion.HasValue)
                return true;

            // Validate that we still have all of the transaction's writes.
            var transactionId = writeVersion.Value.TransactionId;
            if (this.log.ContainsKey(transactionId))
            {
                return this.log[transactionId].Version == writeVersion.Value;
            }

            return false;
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
            // Version read by the transaction is lost.
            if (!this.log.ContainsKey(readVersion.Value.TransactionId)) return false;
            // If version is not same it was overridden by the same transaction that originally wrote it.
            return this.log[readVersion.Value.TransactionId].Version == readVersion.Value;
        }

        private void Recovery()
        {
            if (base.State.Version.TransactionId == 0)
            {
                base.State.Value = new TGrainState();
            }

            this.stableVersion = base.State.StableVersion;
            this.writeLowerBound = base.State.WriteLowerBound;
            this.version = base.State.Version;
            this.value = base.State.Value;
            DeserializeLogs();

            Debug.Assert(base.State.Version.TransactionId == base.State.LastPrepared);

            writeLowerBound = 0;

            // Rollback any known aborted transactions
            Restore();
        }

    }
}
