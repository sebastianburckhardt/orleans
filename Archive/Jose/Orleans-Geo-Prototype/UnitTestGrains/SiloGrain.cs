using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTestGrains
{
    [Reentrant]
    public class SiloGrain : GrainBase, ISiloGrain, ITestSiloGrain
    {

        // set and get simulated SiloAddress for the silo simulated by this grain
        //public Task<int> GetSiloAddress()
        //{
        //    return Task.FromResult(1234);
        //}

        //public Task<int> GetDeploymentId() // get and set the deploymentId for this silo. It represents the deployment in which the silo belongs
        //{
        //    return Task.FromResult(4321);
        //}

        

        private string _lastReceivedMessage;
        private List<ISiloGrain> deploymentNeighbors = new List<ISiloGrain>(); // list of other silos in the same deployment 
       

        //private SiloGrain()
        //{
        //    deploymentNeighbors = new List<ISiloGrain>();
            //deploymentNeighbors.Add(this);
        //}
        
        public Task<bool> AddDeploymentNeighbor(ISiloGrain g)
        {
            deploymentNeighbors.Add(g);
            return Task.FromResult(true);
        }

        public Task<bool> CheckDeploymentMembership(ISiloGrain g) // check whether this silo is in the same deployment as the silo g
        {
            //Assert.IsTrue(deploymentNeighbors.);
            if (deploymentNeighbors.Contains(g))
            {
                return Task.FromResult(true);
            }
            return Task.FromResult(false);
        }

        public Task<string> ReturnLastReceivedMessage()
        {
            return Task.FromResult(_lastReceivedMessage);
        }

        public Task<string> ReceiveMessage(string msg)
        {
            Console.WriteLine("Message received: {0}", msg);
            _lastReceivedMessage = (string)msg.Clone();
            return Task.FromResult(msg);
        }

        public Task<string> SendMessage(string s)
        {
            return SendMessage(null, s);
        }

        public Task<string> SendMessage(ISiloGrain isg, string s)
        {
            if (deploymentNeighbors.Contains(isg))
            {
                isg.ReceiveMessage(s);
            }
            return Task.FromResult(s);
        }
    }
}
