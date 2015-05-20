using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface ISiloGrain : IGrain
    { 
        Task<bool> AddDeploymentNeighbor(ISiloGrain g); // add a silo as a neighbor in the same deployment
        Task<bool> CheckDeploymentMembership(ISiloGrain g); // check whether this silo is in the same deployment as the silo g
        Task<string> ReceiveMessage(string s); // accept a message from any other silo 
    }
}
