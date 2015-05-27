using Orleans;
using Hello.Interfaces;
using System.Threading.Tasks;
using ReplicatedGrains;
using System.Collections.Generic;
using System;
using Orleans.Providers;
#pragma warning disable 1998

namespace Hello.Grains
{
    [Serializable]
    public class HelloGrainState
    {
        private List<string> messages = new List<string>();

        // current account balance
        public List<string> LastTenMessages { get { return messages; } }

        public void AddMessage(String message)
        {
            while (messages.Count >= 10)
            {
                messages.RemoveAt(0);
            }
            messages.Add(message);
        }
    }

    public class AddOperation : IAppliesTo<HelloGrainState>
    {
        public AddOperation(String message)
        {
            this.Message = message;
        }

        public void Update(HelloGrainState state)
        {
            state.AddMessage(this.Message);
        }

        public string Message { get; set; }
    }

    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    [StorageProvider(ProviderName = "AzureStore")] 
    public class ReplicatedHelloGrain : SequencedGrain<HelloGrainState>, IReplicatedHelloGrain
    {
        public async Task Hello(string arg)
        {
            await UpdateLocallyAsync(new AddOperation(arg));
        }

        public async Task<string> GetTopMessagesAsync(bool syncGlobal)
        {
            List<string> messages;
            if (syncGlobal)
            {
                messages = (await GetGlobalStateAsync()).LastTenMessages;
            }
            else
            {
                messages = (await GetLocalStateAsync()).LastTenMessages;
            }
            return await Task.FromResult(String.Join("", messages));
        }
    }
}
