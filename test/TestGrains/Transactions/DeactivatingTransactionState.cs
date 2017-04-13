
using System.Threading.Tasks;
using Orleans;
using Orleans.Facet;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Transactions;
using UnitTests.GrainInterfaces;

namespace Tester.TransactionsTests
{
    public class DeactivatingTransactionState<TState> : TransactionalState<TState>, IDeactivatingTransactionState<TState>, ITransactionalResource, IGrainBinder
        where TState : class, new()
    {
        private Grain containerGrain;
        private readonly IGrainRuntime runtime;
        private Logger trace;

        public TransactionDeactivationPhase DeactivationPhase { get; set; }

        public new TState State => GetState();

        public DeactivatingTransactionState(IGrainRuntime runtime, ITransactionAgent transactionAgent, SerializationManager serializationManager)
            : base(transactionAgent, serializationManager)
        {
            this.runtime = runtime;
        }

        async Task<bool> ITransactionalResource.Prepare(long transactionId, TransactionalResourceVersion? writeVersion, TransactionalResourceVersion? readVersion)
        {
            this.trace.Info($"Grain {this.containerGrain} preparing transaction {transactionId}");
            bool result = await base.Prepare(transactionId, writeVersion, readVersion);
            if (this.DeactivationPhase == TransactionDeactivationPhase.AfterPrepare)
            {
                this.runtime.DeactivateOnIdle(containerGrain);
                this.DeactivationPhase = TransactionDeactivationPhase.None;
                this.trace.Info($"Grain {this.containerGrain} deactivating after transaction {transactionId} prepare");
            }
            return result;
        }

        async Task ITransactionalResource.Commit(long transactionId)
        {
            this.trace.Info($"Grain {this.containerGrain} commiting transaction {transactionId}");
            await base.Commit(transactionId);
            if (this.DeactivationPhase == TransactionDeactivationPhase.AfterCommit)
            {
                this.runtime.DeactivateOnIdle(containerGrain);
                this.DeactivationPhase = TransactionDeactivationPhase.None;
                this.trace.Info($"Grain {this.containerGrain} deactivating after transaction {transactionId} Commit");
            }
        }

        Task IGrainBinder.BindAsync(Grain grain)
        {
            this.trace = this.runtime.GetLogger(grain.GetType().Name).GetSubLogger(GetType().Name);
            this.trace.Info($"{GetType().Name} binding to grain {this.containerGrain}.");
            this.containerGrain = grain;
            return base.BindAsync(grain);
        }

        private TState GetState()
        {
            TState state = base.State;
            if (this.DeactivationPhase == TransactionDeactivationPhase.AfterCall)
            {
                this.runtime.DeactivateOnIdle(containerGrain);
                this.DeactivationPhase = TransactionDeactivationPhase.None;
                var info = TransactionContext.GetTransactionInfo();
                this.trace.Info($"Grain {this.containerGrain} deactivating after transaction {info.TransactionId} call");
            }
            return state;
        }
    }
}
