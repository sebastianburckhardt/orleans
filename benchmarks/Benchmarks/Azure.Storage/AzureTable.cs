using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;

#pragma warning disable 1998

namespace Azure.Storage
{

    public class AzureTableStorage : IScenario
    {

        // scenario parameters
        // sync read operations = get exact top 10
        // async read operations = get approx top 10
        // sync write operations = post now
        // async write operations = postlater
        public AzureTableStorage(int pNumRobots, int pNumReqs)
        {
            this.numRobots = pNumRobots;
            this.numReqs = pNumReqs;
  
        }

        private int numRobots;
        private int numReqs;

    

        public enum OperationType
        {
            READ_SYNC,
            READ_ASYNC,
            WRITE_SYNC,
            WRITE_ASYNC
        };

        public String RobotServiceEndpoint(int workernumber)
        {

            return Endpoints.GetDefaultService();

        }

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xsreads{2}xasreads{3}xswrites{4}xaswrites{5}xsize{6}", numRobots, numReqs); } }

        public int NumRobots { get { return numRobots; } }

        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numRobots];

            // start each robot
            for (int i = 0; i < numRobots; i++)
                robotrequests[i] = context.RunRobot(i, numReqs.ToString());

            // wait for all robots
            await Task.WhenAll(robotrequests);

            // check robot responses
            for (int i = 0; i < numRobots; i++)
            {
                Console.Write("Finished: {0} \n", robotrequests[i].Result);
            }

            return "ok";
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);

            string[] param = parameters.Split('-');

              //          await context.ServiceRequest(new HttpRequestSequencedSize(numReqs * robotnumber + i, false));
                   
            return parameters;
        }




    }

   

    public class HttpRequestAzureTable : IHttpRequest
    {

        /// <summary>
        /// Constructor for READ calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestAzureTable(int pNumReq, bool async)
        {
            if (async)
            {
                this.requestType = Azure.Storage.AzureTableStorage.OperationType.READ_SYNC;
            }
            else
            {
                this.requestType = Azure.Storage.AzureTableStorage.OperationType.READ_ASYNC;
            }
            this.numReq = pNumReq;
        }

        public HttpRequestAzureTable(int pNumReq, string pPayload, bool async)
        {
            if (async)
            {
                this.requestType = Azure.Storage.AzureTableStorage.OperationType.WRITE_ASYNC;
            }
            else
            {
                this.requestType = Azure.Storage.AzureTableStorage.OperationType.WRITE_SYNC;
            }
            this.numReq = pNumReq;
            this.payload = pPayload;

        }



        // Request number
        private int numReq;
        // Request type, get or post
        private Azure.Storage.AzureTableStorage.OperationType requestType;
        private string payload;



        public string Signature
        {
            get
            {
                if (requestType == Azure.Storage.AzureTableStorage.OperationType.READ_SYNC || 
                    requestType == Azure.Storage.AzureTableStorage.OperationType.READ_ASYNC)
                {
                    return "GET size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq;
                }
                else
                {
                    return "POST size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq;
                }
            }
        }


        public string Body
        {
            get
            {
                return payload;
            }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            // Talk to Azure here
            return "ok";
        }



        public Task<string> ProcessResponseOnClient(string response)
        {
            Console.Write("{0} Req # {1} \n ", response, numReq);
            return Task.FromResult(response);
        }


        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }


}


