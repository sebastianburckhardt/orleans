using Orleans;
using GeoOrleans.Benchmarks.Hello.Interfaces;
using System.Threading.Tasks;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains;
using System.Collections.Generic;
using System;
using Orleans.Providers;
using Orleans.Streams;
#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Hello.Grains
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
            await UpdateLocallyAsync(new AddOperation(arg), false);
        }

        public async Task<string[]> GetTopMessagesAsync(bool syncGlobal)
        {

            if (syncGlobal)
            {
                return (await GetGlobalStateAsync()).LastTenMessages.ToArray();
            }
            else
            {
                return (await GetLocalStateAsync()).LastTenMessages.ToArray();
            }

        }




        /*

        private IAsyncStream<String[]> _notificationstream;

        protected IAsyncStream<String[]> NotificationStream
        {
            get
            {
                if (_notificationstream == null)
                {
                    IStreamProvider streamProvider = base.GetStreamProvider("SimpleMessageStreamProvider");
                    _notificationstream = streamProvider.GetStream<String[]>(new Guid(), "topmessages-" + this.IdentityString);
                }
                return _notificationstream;
            }
        }

        public async Task<IViewStream<String[]>> GetTopMessagesStreamAsync()
        {
            // make sure to receive from now on
            ReceiveStateChangeNotifications(true);

            return new TopMessagesStream()
            {
                Grain = this,
                Stream = NotificationStream
            };
        }

        public override async Task OnStateChangeAsync(LocalVersion version)
        {
            var cur = (await GetLocalStateAsync()).LastTenMessages.ToArray();

            await NotificationStream.OnNextAsync(cur);
        }


        [Serializable]
        public class TopMessagesStream : IViewStream<String[]>
        {
            public IReplicatedHelloGrain Grain { get; set; }
            public IAsyncStream<String[]> Stream { get; set; }


            private bool started;

            public async Task<string[]> Latest()
            {
                if (!started)
                {
                    started = true;
                    return await Grain.GetTopMessagesAsync();
                }
            }

            public Task Unsubscribe()
            {
                throw new NotImplementedException();
            }
        }
         * */
    }


     
 
}
