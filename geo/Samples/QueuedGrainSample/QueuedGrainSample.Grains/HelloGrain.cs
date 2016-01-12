using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using QueuedGrainSample.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.QueuedGrains;
using Orleans.Concurrency;


namespace QueuedGrainSample.Grains
{
    [Serializable]
    public class SampleGrainState
    {
        public List<string> Messages { get; set; }

        public SampleGrainState()
        {
            Messages = new List<string>();
        }
    }

    [Serializable]
    public class AppendMessage : IUpdateOperation<SampleGrainState>
    {
        public string Message { get; set; }

        public void Update(SampleGrainState state)
        {
            state.Messages.Add(Message);
        }
        public override string ToString()
        {
            return string.Format("[AppendMessage \"{0}\"]", Message);
        }
    }

    [Serializable]
    public class ClearAll : IUpdateOperation<SampleGrainState>
    {
        
        public void Update(SampleGrainState state)
        {
            state.Messages.Clear();
        }
        public override string ToString()
        {
            return string.Format("[ClearAll]");
        }
    }

    [StorageProvider(ProviderName = "GloballySharedAzureAccount")]
    public class HelloGrain : QueuedGrain<SampleGrainState>, IHelloGrainInterface
    {
        public Task<LocalState> AppendMessage(string msg)
        {
            EnqueueUpdate(new AppendMessage() { Message = msg });
            return GetLocalState();
        }

        public Task<LocalState> ClearAll()
        {
            EnqueueUpdate(new ClearAll());
            return GetLocalState();
        }

        public Task<LocalState> GetLocalState()
        {
            return Task.FromResult(new LocalState()
            {
                TentativeState = this.TentativeState.Messages,
                ConfirmedState = this.ConfirmedState.Messages,
                UnconfirmedUpdates = this.UnconfirmedUpdates.Select((u) => u.ToString()).ToList()
            });
        }
    }
}
