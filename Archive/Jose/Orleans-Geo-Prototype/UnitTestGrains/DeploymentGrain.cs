using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTestGrainInterfaces;
using Orleans;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace UnitTestGrains
{
    [Reentrant]
    public class DeploymentGrain : SiloGrain, IDeploymentGrain, ITestDeploymentGrain
    {
        //private ISiloGrain _gateway; //this represents the gateway silo in this deployment, talked with Sergey: the deployment grain itself can be the gateway
        private int _n; // number of silos in this deployment
        private int deploymentId;
        private List<ISiloGrain> _silos; // list of silos in this deployment 
        //private List<KeyValuePair<IGrain, int>> _grainStatus;
        private Dictionary<int, int> _grainStatus; // cached status of g, right now we just use an int to represent the grain id (key), the value is the status of the grain in this deployment
        // status values: 0 = not interested, 1 = interested, 2 = registered somewhere else, 3 = i win and i registered the grain
        private List<IDeploymentGrain> _deployments; // list of all deployments in the system. this list will be populated by the deployment level membership service 

        public Task<bool> LoadDeployment(int d, int n)
        {
            deploymentId = d; // save the guid for this deployment
            _n = n;
            _silos = new List<ISiloGrain>();
            //_grainStatus = new List<KeyValuePair<IGrain, int>>();
            _grainStatus = new Dictionary<int, int>();
            for (int i = 0; i < n; i++) // create n silos and add them to the list
            {
                var g = SiloGrainFactory.GetGrain(Guid.NewGuid());
                _silos.Add(g);

            }
            //_gateway = _silos.ElementAt(0); // just for test purposes, set the first silo in the list as the gateway silo, new design: the deployment grain itself is the gateway
            return Task.FromResult(true);
        }

        public Task<bool> SetListOfDeployments(List<IDeploymentGrain> deployments ) // this method loads the list of deployments
        {
            _deployments = deployments;
            return Task.FromResult(true);
        }

        //public void SetGatewaySilo(ISiloGrain gateway)
        //{
        //    _gateway = gateway;
        //}

        public Task<bool> ReturnSilosInDeployment()
        {
            return Task.FromResult(true);
        }

        public Task<string> ReceiveMessage(int g, string message, IDeploymentGrain dId) // this respresents a broadcast message being sent to this deployment
        {
            // this seems like the place to implement the state machine, so we should probably change the name later
            //if (_grainStatus. == 0 || (_grainStatus(g) && _grainStatus. == )) check whether this deployment know anything about g
            // we have decided not to return from cached entries. so a deployment will reply anything other than "not interested", only if either it already registered g or it is itself intersted to register g
            //return "not interested"; // for now just return "not intersted", actually will return value after checking status of g
            // this deployment got a message from another deployment about g. So now we have to update the state of g in this deployment based on the incoming message
            if (_grainStatus == null)
            {
                return Task.FromResult("not interested"); // this is for both lookup and register messages
            }
        
            if (!_grainStatus.ContainsKey(g) || _grainStatus[g] == 0)
            {
                // there is no status for this grain or the status is not interested
                return Task.FromResult("not interested"); // this is for both lookup and register messages
            } 
            if (message.Equals("register") && _grainStatus != null && _grainStatus[g] == 1)
            {
                // this means both deployments want to register and we have a race condition
                // now check if the calling deployment wins, in that case, set state to "registered somewhere else", in later version we will save the winning deployment
                if (dId.GetPrimaryKeyLong().CompareTo(this.GetPrimaryKeyLong()) < 0)
                {
                    //the sending deployment wins
                    _grainStatus[g] = 2;
                    return Task.FromResult("not interested");
                }
                else
                {
                    // i won against this deployment, so i will reply that I am interested, but i cannot be sure that some one else might win
                    return Task.FromResult("register");
                }
            }
            return Task.FromResult("not interested");
        }

        // sends a message to all deployments, returns a list of responses
        public async Task<List<string>> SendBroadcastMessage(int grain, string messagetype) // this deployment sends a broadcast message to all deployments
        {
            
            if (messagetype == null) throw new ArgumentNullException("messagetype");
            var responselist = new List<string>();
            foreach (var deployment in _deployments)
            {
                string response = await deployment.ReceiveMessage(grain, messagetype, this);
                if (response != null) responselist.Add(response);
            }
            return responselist;

        }

        private int GetPrimaryKey()
        {
            //throw new NotImplementedException();
            return deploymentId;
        }

        public int GetDeploymentId()
        {
            return deploymentId;
        }

        public Task<string> Lookup(int grain)
        {
            return Task.FromResult("lookup success");
        }

        public async Task<string> Register(int grain)
        {

            // at this point this deployment knows that the grain is not registered in this deployment and also the lookup failed. so it wants to register, and it has to send a broadcast message
            //foreach (var grain in grains)
            //{
                _grainStatus[grain] = 1; // set the grain status that this deployment is interested, now it has to check whether everyone else agrees
            //}
            // before sending the broadcast, first check whether I am the winner. We will detect winner by just comparing primary key of deployments. the winner is the deployment with the lowest primary key
            // a later optimization would be to just send broadcast message to thos deployments that have higher primary key
            //int winner = 1;
            //if (_deployments.Any(deployment => deployment. < deploymentId))
            //{
            //    winner = 0;
            //}
            /*foreach (var dep in _deployments)
            {
                if (dep.GetPrimaryKeyLong().CompareTo(this.GetPrimaryKeyLong()) < 0)
                {
                    winner = 0;
                }
            }*/
            /*if (winner == 1)
            {
                // no need to broadcast, already a winner
                _grainStatus[grain] = 3; // i won and will register the grain
            }*/
            var responseList = await SendBroadcastMessage(grain, "register");
            if (responseList.Any(response => response.Equals("register")))
            {
                _grainStatus[grain] = 2;
            }
            else
            {
                // everyone replied "not interested", so i won
                _grainStatus[grain] = 3;
                return "registered";
            }

            // now check the response list and make a decision based on that
            return "registration failed";
        }
    }
}
