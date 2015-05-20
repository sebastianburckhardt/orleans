using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface ITestSiloGrain : IGrain // this interface is for testing silo grains, the protocol functionality interface of silo grains are in ISiloGrain
    {
        Task<string> ReturnLastReceivedMessage(); // this method is for testing only
        Task<string> SendMessage(ISiloGrain isg, string s); // send a message to another silo
    }
}
