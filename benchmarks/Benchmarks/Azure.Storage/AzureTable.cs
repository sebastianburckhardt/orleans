using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Azure.Storage.AzureCommon;

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
        private int percentReads;
        private int percentWrites;
        private int samePartition;
        private int sameRow;
    

        public String RobotServiceEndpoint(int workernumber)
        {

            return Endpoints.GetDefaultService();

        }

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xreads{2}xpkey{1}xrkey{1}", numRobots, numReqs,percentReads,samePartition,sameRow); } }

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

            CloudTableClient azureClient = AzureCommon.getTableClient();
            CloudTable table = AzureCommon.createTable(azureClient, "testTable");




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
        public HttpRequestAzureTable( AzureCommon.OperationType pRequestType, int pNumReq, string pTable, string pPartitionKey, string pRowKey, ByteEntity pEntity)
        {
       
                this.requestType = pRequestType;
                this.numReq = pNumReq;
                this.tableName = pTable;
                this.partitionKey = pPartitionKey;
                this.rowKey = pRowKey;
                this.payload = pEntity;
            
        }


        // Request number
        private int numReq;
        // Request type, get or post
        private AzureCommon.OperationType requestType;
        private ByteEntity payload;
        private string partitionKey;
        private string rowKey;
        private string tableName;

        public string Signature
        {
            get
            {
                if (requestType == AzureCommon.OperationType.CREATE)
                {
                    return "GET size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName;
                }
                if (requestType == AzureCommon.OperationType.READ)
                {
                    return "GET size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey + "&rkey=" + rowKey;
                }
                else if (requestType == AzureCommon.OperationType.READ_RANGE)
                {
                    return "GET size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey;

                }
                else if (requestType == AzureCommon.OperationType.READ_BATCH)
                {
                    throw new NotImplementedException();
                }
                else if (requestType == AzureCommon.OperationType.INSERT)
                {
                    throw new NotImplementedException();

                }
                else if (requestType == AzureCommon.OperationType.INSERT_BATCH)
                {
                    throw new NotImplementedException();

                }
                else if (requestType == AzureCommon.OperationType.UPDATE)
                {
                    return "POST size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq;
                } 
                else if (requestType == AzureCommon.OperationType.UPDATE_BATCH)
                {
                   throw new NotImplementedException();

                }
                return null;
               
            }
        }


        public string Body
        {
            get
            {
               // return payload;
                if (payload == null) return null;
                else return ByteEntity.FromEntityToString(payload);

            }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            CloudTableClient tableClient = AzureCommon.getTableClient();
            string result = "ok";

            //TODO(natacha): don't know how costly retrieving this is for
            // every request. Make this persistent somehow?
            if (requestType == AzureCommon.OperationType.CREATE)
            {
                bool ret = AzureCommon.createTableCheck(tableClient, tableName);
                if (ret) result = "Table Created";
                else result = "Could not create table";
            }
            if (requestType == AzureCommon.OperationType.READ)
            {
                TableResult res = 
                    await AzureCommon.findEntity(tableClient, tableName, partitionKey, rowKey);
                if (res.HttpStatusCode == 404)
                {
                    result = "Error ";
                }
                else {
                    ByteEntity entity = (ByteEntity) res.Result;
                    result = ByteEntity.FromEntityToString(entity);
                }
            }
            else if (requestType == AzureCommon.OperationType.READ_RANGE)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureCommon.OperationType.READ_BATCH)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureCommon.OperationType.INSERT)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureCommon.OperationType.INSERT_BATCH)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureCommon.OperationType.UPDATE)

            {
                TableResult res = 
                        await AzureCommon.updateEntity(tableClient, tableName, payload);
                return res.HttpStatusCode.ToString();
            }
            else if (requestType == AzureCommon.OperationType.UPDATE_BATCH)
            {
                return "Unimplemented";
            }
            
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


