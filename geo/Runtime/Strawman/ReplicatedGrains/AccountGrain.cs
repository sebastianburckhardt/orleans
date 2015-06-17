using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicatedGrains
{
    public interface IAccountGrain {
        Task Add(int amount);
        Task<int> Balance();
    }
    public class AccountGrain : SequencedGrain<AccountState, AccountUpdate>
    {
        public async Task Add(int amount)
        {
            Sequencer.Submit(new AccountGrainUpdate() { Amount = amount });
        }

        /// <summary>
        ///  returns the current (tentative) balance.
        /// </summary>
        /// <returns></returns>
        public async Task<int> Balance()
        {
            return State.Balance;
        }

    }

    public class AccountState : SequencedState<AccountState, AccountUpdate>
    {
        public int Balance { get; set; }
    }

    public class AccountGrainUpdate : SequencedUpdate<AccountState, AccountUpdate>
    {
        public int Amount { get; set; }

        public void Update(AccountGrainState state)
        {
            state.Current += Amount;

        }
    }

  
}
