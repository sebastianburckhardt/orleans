using Orleans;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains.Examples.Interfaces;
using Orleans.Providers;
#pragma warning disable 1998

namespace GeoOrleans.Runtime.Strawman.ReplicatedGrains.Examples.Grains
{

    [StorageProvider(ProviderName = "AzureStore")]
    public class AccountGrain : SequencedGrain<AccountGrain.GrainState>, IAccountGrain
    {

        [Serializable]
        public class GrainState
        {
            // current account balance
            public uint Balance { get; set; }
        }

        // Grain Query Operations
        public async Task<uint> EstimatedBalance()
        {
            // return the current estimated balance (within staleness bound), including pending deposits
            return (await GetLocalStateAsync()).Balance;
        }

        public async Task<uint> ActualBalance()
        {
            // return the current estimated balance (within staleness bound), including pending deposits
            return (await GetGlobalStateAsync()).Balance;
        }

        // Grain Update Operations

        public async Task ReliableDeposit(uint amount)
        {
            await UpdateLocallyAsync(new DepositOperation() { Amount = amount },false);
        }


        public async Task UnreliableDeposit(uint amount)
        {
            // unreliable because we do not wait for ack from persistent storage
            await UpdateLocallyAsync(new DepositOperation() { Amount = amount }, save: false);
        }

        [Serializable]
        public class DepositOperation : IAppliesTo<GrainState>
        {
            public uint Amount { get; set; }

            public void Update(GrainState state)
            {
                state.Balance += Amount;
            }
        }

        public async Task<bool> Withdraw(uint amount)
        {
            // withdraw is not performed locally, but globally, since it has a return value
            return await UpdateGloballyAsync((state) =>
                {
                    if (state.Balance >= amount)
                    {
                        state.Balance -= amount;
                        return true;
                    }
                    else
                        return false;
                });
        }
    }
}
