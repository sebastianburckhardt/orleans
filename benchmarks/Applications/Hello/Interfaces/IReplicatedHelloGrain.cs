using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Hello.Interfaces
{   
    public interface IReplicatedHelloGrain : IGrain
    {
        Task Hello(String msg);

        Task<String> GetTopMessagesAsync(bool syncGlobal);
    }
}
