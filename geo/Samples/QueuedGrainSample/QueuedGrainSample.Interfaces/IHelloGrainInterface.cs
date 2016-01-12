using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using Orleans.Concurrency;

namespace QueuedGrainSample.Interfaces
{
    public interface IHelloGrainInterface : IGrainWithIntegerKey
    {
        Task<LocalState> AppendMessage(string name);

        Task<LocalState> ClearAll();

        Task<LocalState> GetLocalState();
 
    }

    [Serializable]
    public class LocalState
    {
        public List<string> TentativeState { get; set; }

        public List<string> ConfirmedState { get; set; }

        public List<string> UnconfirmedUpdates { get; set; }
    }
}
