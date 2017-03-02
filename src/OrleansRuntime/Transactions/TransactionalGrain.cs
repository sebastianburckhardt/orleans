using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Text;
using Orleans;
using Orleans.Serialization;
using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;

namespace Orleans.Transactions
{

    public class TransactionalGrainState<T>
    {
        // The transactionId of the transaction that wrote the current value
        public GrainVersion Version { get; set; }

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
        new protected TGrainState State { get { return GetState(); } }

        private ITransactionAgent transactionAgent;

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
        private GrainVersion version;
        private long lastPrepared;
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
            this.transactionAgent = Silo.CurrentSilo.LocalTransactionAgent;
            this.serializationManager = ServiceProvider.GetRequiredService<SerializationManager>();

            Recovery();

            return TaskDone.Done;
        }

        #region ITransactionalGrain
        /// <summary>
        /// Implementation of ITransactionalGrain Prepare method. See interface documentation for more details.
        /// </summary>
        public async Task<bool> Prepare(long transactionId, GrainVersion? writeVersion, GrainVersion? readVersion)
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
            var thisRef = this.AsReference<ITransactionalGrain>();

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

            if (info.IsReadOnly)
            {
                // Sanity check to make sure this is indeed a stable version.
                Debug.Assert(readVersion.TransactionId <= this.stableVersion);
            }

            if (readVersion.TransactionId == info.TransactionId)
            {
                // Just reading our own write here.
                // Sanity check to see if there's a lost write.
                if (info.WriteSet.ContainsKey(thisRef) && info.WriteSet[thisRef] > readVersion.WriteNumber)
                {
                    // Context has record of more writes than we have, some writes must be lost.
                    throw new OrleansTransactionAbortedException(info.TransactionId, "Lost Write");
                }
            }
            else
            {
                if (info.ReadSet.ContainsKey(thisRef) && info.ReadSet[thisRef] != readVersion)
                {
                    // Uh-oh. Read two different versions of the grain.
                    throw new OrleansValidationFailedException(info.TransactionId);
                }

                info.ReadSet[thisRef] = readVersion;

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

            var value = this.transactionCopy[info.TransactionId];
            ITransactionalGrain thisRef = this.AsReference<ITransactionalGrain>();
            
            //
            // Validation
            //

            if (this.version.TransactionId > info.TransactionId || this.writeLowerBound >= info.TransactionId)
            {
                // Prevent cycles. Wait-die
                throw new OrleansTransactionWaitDieException(info.TransactionId);
            }

            GrainVersion version;
            version.TransactionId = info.TransactionId;
            version.WriteNumber = 1;
            if (this.version.TransactionId == info.TransactionId)
            {
                version.WriteNumber = this.version.WriteNumber + 1;
            }

            //
            // Update Transaction Context
            //
            if (!info.WriteSet.ContainsKey(thisRef))
            {
                info.WriteSet.Add(thisRef, 0);
            }
            info.WriteSet[thisRef]++;

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

            this.log[info.TransactionId].NewVal = value;
            this.log[info.TransactionId].Version = version;
            this.value = value;
            this.version = version;

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
                LogRecord<TGrainState> log = this.log[tId];

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
                GrainVersion initialVersion;
                initialVersion.TransactionId = 0;
                initialVersion.WriteNumber = 0;

                this.version = initialVersion;
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
                LogRecord<TGrainState> record = new LogRecord<TGrainState>();
                record.NewVal = base.State.Logs[key];

                GrainVersion version;
                version.TransactionId = key;
                version.WriteNumber = 1;
                record.Version = version;
                this.log[key] = record;
            }
        }

        private async Task Persist(long stableVersion, long writeLowerBound)
        {
            Exception error = null;

            try
            {
                SerializeLogs();
                base.State.Value = this.value;
                base.State.Version = this.version;
                base.State.LastPrepared = base.State.Version.TransactionId;
                base.State.StableVersion = stableVersion;
                base.State.WriteLowerBound = writeLowerBound;

                await base.WriteStateAsync();

                this.lastPrepared = base.State.LastPrepared;
                this.stableVersion = stableVersion;
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

        private bool ValidateWrite(GrainVersion? version)
        {
            if (!version.HasValue)
                return true;

            // Validate that we still have all of the transaction's writes.
            var transactionId = version.Value.TransactionId;
            if (this.log.ContainsKey(transactionId))
            {
                return this.log[transactionId].Version == version.Value;
            }

            return false;
        }

        private bool ValidateRead(long transactionId, GrainVersion? version)
        {
            if (!version.HasValue)
                return true;

            foreach (var key in this.log.Keys)
            {
                if (key >= transactionId)
                {
                    break;
                }

                if (key > version.Value.TransactionId && key < transactionId)
                {
                    return false;
                }
            }

            if (version.Value.TransactionId == 0)
            {
                Debug.Assert(version.Value.WriteNumber == 0);
                return true;
            }
            else if (this.log.ContainsKey(version.Value.TransactionId))
            {
                if (this.log[version.Value.TransactionId].Version != version.Value)
                {
                    // Version read was overriden by the same transaction that originally wrote it.
                    return false;
                }

                return true;
            }
            else
            {
                // Version read by the transaction is lost.
                return false;
            }
        }

        private void Recovery()
        {
            if (base.State.Version.TransactionId == 0)
            {
                base.State.Value = new TGrainState();
            }

            this.lastPrepared = base.State.LastPrepared;
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
