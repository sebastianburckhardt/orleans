using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface IDeploymentGrain : ISiloGrain
    {
        Task<bool> ReturnSilosInDeployment();
        Task<List<string>> SendBroadcastMessage(int grain, string messagetype);
        Task<string> ReceiveMessage(int g, string s, IDeploymentGrain deployment);
        Task<string> Register(int grain);
        Task<string> Lookup(int grain);
    }
}
