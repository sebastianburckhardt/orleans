using Orleans;
using Orleans.Streams;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GeoOrleans.Runtime.Strawman.ReplicatedGrains;

namespace GeoOrleans.Benchmarks.Hello.Interfaces
{   
    public interface IReplicatedHelloGrain : IGrain
    {
        Task Hello(String msg);

        Task<String[]> GetTopMessagesAsync(bool syncGlobal);

        //Task<IViewStream<String[]>> GetTopMessagesStreamAsync();

    }


  
}
