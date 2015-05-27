using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Benchmark
{

    public class HttpHello : IScenario
    {
        public HttpHello(int numrobots, int numreqs)
        {
            this.numworkers = numrobots;
            this.numreqs = numreqs;
        }
        private int numworkers;
        private int numreqs;

        public string Name { get { return string.Format("http{0}x{1}", numworkers, numreqs); } }

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
            for (int i = 0; i < numreqs; i++)
                await context.ServiceRequest(new GetRequest(numreqs * workernumber + i));

            return "ok";
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetDefaultService();
        }
    }

    public class GetRequest : IHttpRequest
    {
        public GetRequest(int nr)
        {
            this.nr = nr;
        }

        private int nr;

        public string Signature
        {
            get { return string.Format("GET hello?nr={0}&command={1}", nr, "http"); }
        }

        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            return "Hello #" + nr;
        }

        public async Task ProcessResponseOnClient(string response)
        {
            Util.Assert(response == "Hello #" + nr, "incorrect response");
        }

        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }

}
