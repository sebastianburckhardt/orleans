using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Core;
using Orleans.Providers;
using Orleans.Runtime;

namespace Orleans.Replication
{
    public abstract class ReplicationProviderBase : IReplicationProvider
    {
        protected readonly Dictionary<GrainReference, IProtocolState> perGrainProtoState = new Dictionary<GrainReference, IProtocolState>();

        private void UpdateState(GrainReference reference, IProtocolState state)
        {
            perGrainProtoState[reference] = state;
        }

        private IProtocolState GetState(GrainReference reference )
        {
            return perGrainProtoState[reference];
        }

        public abstract Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config);



        public abstract Task ReadAsync<T>(string grainType, GrainReference grainReference, IGrainState grainState) where T : class, IGrainState, new();

        public abstract Task WriteAsync<T>(string grainType, GrainReference grainReference, IGrainState grainState, TaggedUpdate<T> update) where T : class, IGrainState, new();

        public abstract Task WriteAsync<T>(string grainType, GrainReference grainReference, IGrainState grainState, List<TaggedUpdate<T>> update) where T : class, IGrainState, new();

        public abstract Task<IProtocolMessage<T>> OnMessageReceived<T>(string grainType, GrainReference grainReference, IProtocolMessage<T> payload) where T : class, IGrainState, new();

        public abstract Task Close();

        public Logger Log { get; private set; }


        public string Name
        {
            get;
            private set;
        }
    }

}
