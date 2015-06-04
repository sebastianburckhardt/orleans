using Common;
using Hello.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Benchmark
{

    public class TCPHello : IScenario
    {
        public TCPHello(int numrobots, int numreqs)
        {
            this.numworkers = numrobots;
            this.numreqs = numreqs;
        }
        private int numworkers;
        private int numreqs;

        public string Name { get { return string.Format("tcphello{0}x{1}", numworkers, numreqs); } }

        public int NumRobots { get { return numworkers; } }

        public async Task<string> ConductorScript(IConductorContext context)
        {
            var workerrequests = new Task<string>[numworkers];
            for (int i = 0; i < numworkers; i++)
                workerrequests[i] = context.RunRobot(i, "");

            await Task.WhenAll(workerrequests);

            return string.Join(",", workerrequests.Select((t) => t.Result));
        }

        public async Task<string> RobotScript(IRobotContext context, int workernumber, string parameters)
        {
            Task<string>[] requests = new Task<string>[numreqs];

            for (int i = 0; i < numreqs; i++)
                requests[i] = context.ServiceRequest(new HelloTcpRequest(numreqs * workernumber + i, workernumber));

            Task.WaitAll(requests);

            //verify that all responses are same. this wont work with current implementation. 
            /*string r0 = responses[0];
            for (int i = 1; i < numreqs; i++)
            {
                if (!responses[i].Equals(r0, StringComparison.InvariantCultureIgnoreCase))
                {
                    return "not ok. The responses do not match.";
                }
            }*/

            var responses = string.Join(",", requests.Select((t) => t.Result));

            return "ok:" + workernumber + ":" + responses;
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetService(workernumber);
        }
    }

    public class HelloTcpRequest : IHttpRequest
    {
        public HelloTcpRequest(int nr, int wr)
        {
            this.nr = nr;
            this.wr = wr;
        }

        private int nr;
        private int wr;

        public string Signature
        {
            get { return string.Format("GET hello?nr={0}&command={1}&wr={2}", nr, "tcp",wr); }
        }

        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {

            string endpoint = Common.Endpoints.GetService(wr);
            if (endpoint.Equals(Common.Endpoints.ServiceDeployments.OrleansGeoUsWest.ToString()))
            {
                var senderGrain = TCPSenderGrainFactory.GetGrain(0);
                await senderGrain.SayHello("Hello there");
            }
            else
            {
                var receiverGrain = TCPReceiverGrainFactory.GetGrain(0);
                await receiverGrain.listenMessages();
            }
            
            return "ok";
        }

        public async Task<string> ProcessResponseOnClient(string response)
        {

            return response;
        }

        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }

}
