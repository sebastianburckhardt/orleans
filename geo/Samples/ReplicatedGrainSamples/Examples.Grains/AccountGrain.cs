using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans.Providers;
using Orleans.Replication;
#pragma warning disable 1998

namespace Examples.Grains
{
    /// <summary>
    /// The state of the account grain
    /// </summary>
    public class AccountState : GrainState
    {
        // the current account balance.
        // it's a public property so it gets serialized/deserialized
        public uint Balance { get; set; }

        // a list of ids of rejected withdrawal operations
        public List<Guid> RejectedWithdrawals { get; set; }

        // we use a defaultconstructor so the list is never null
        public AccountState()
        {
            RejectedWithdrawals = new List<Guid>();
        }
    }

    
    /// <summary>
    /// The class that defines the deposit operation
    /// </summary>
    [Serializable]
    public class DepositOperation : IUpdateOperation<AccountState>
    {
        /// <summary>
        /// The posted score.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public uint Amount { get; set; }

        /// <summary>
        /// Effect of the deposit on the account state
        /// </summary>
        /// <param name="state"></param>
        public void Update(AccountState state)
        {
            state.Balance += Amount;
        }
    }

    /// <summary>
    /// The class that defines the withdrawal operation
    /// </summary>
    [Serializable]
    public class WithdrawalOperation : IUpdateOperation<AccountState>
    {
        /// <summary>
        /// The posted score.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public uint Amount { get; set; }

        /// <summary>
        /// The unique identifier for this Withdrawal Operation.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public Guid Guid { get; set; }

        /// <summary>
        /// Effect of the withdrawal on the account state
        /// </summary>
        /// <param name="state"></param>
        public void Update(AccountState state)
        {
            if (state.Balance >= Amount)
            {
                state.Balance -= Amount; // succeed
            }
            else
            {
                state.RejectedWithdrawals.Add(Guid); // fail
            }
        }
    }

 
     

    [StorageProvider(ProviderName = "AzureStore")]
    public class AccountGrain : QueuedGrain<AccountState>, IAccountGrain
    {

        // we use a random number generator to create unique identifiers
        private System.Security.Cryptography.RandomNumberGenerator rng;


         
        public async Task<uint> EstimatedBalance()
        {
            // return the current balance, including unconfirmed operations
            return TentativeState.Balance;
        }

        public async Task<uint> ActualBalance()
        {
            // first, retrieve latest global state
            await SynchronizeNowAsync();
            // then return it
            return ConfirmedState.Balance;
        }

        // Grain Update Operations

        public async Task ReliableDeposit(uint amount)
        {
            // first, queue a deposit operation
            EnqueueUpdate(new DepositOperation() { Amount = amount });
            // then, wait for it to be confirmed
            await CurrentQueueHasDrained();
        }


        public Task UnreliableDeposit(uint amount)
        {
            // queue a deposit operation
            EnqueueUpdate(new DepositOperation() { Amount = amount });
            // don't wait for confirmation
            return TaskDone.Done;
        }


        public async Task<bool> Withdraw(uint amount)
        {
            // create a unique id for this operation
            var data = new byte[16];
            rng.GetBytes(data);
            var guid = new Guid(data);

            // queue a withdrawal operation
            EnqueueUpdate(new WithdrawalOperation() { Amount = amount, Guid = guid });
            
            // wait for the queue to drain, i.e. to confirm the update
            await CurrentQueueHasDrained();

            // determine success
            var success = ! ConfirmedState.RejectedWithdrawals.Contains(guid);

            return success;
        }
    }
}
