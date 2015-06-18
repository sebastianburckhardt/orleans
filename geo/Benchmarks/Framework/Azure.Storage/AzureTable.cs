using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using System.Diagnostics;

#pragma warning disable 1998

namespace Azure.Storage
{

    public class AzureTableStorage : IScenario
    {


        private int numRobots;
        private int runTime;
        private int percentReads;
        private int percentWrites;
        private int samePartition;
        private int sameRow;
        private int payloadSize;


        private Random rnd = new Random();

        private const string DEFAULT_PARTITION_KEY = "hello";
        private const string DEFAULT_ROW_KEY = "world";

        private const int PARTITION_KEY_SIZE = 16;
        private const int ROW_KEY_SIZE = 16;


        public AzureTableStorage(int pNumRobots, int pRunTime, int pPercentReads, int pSamePartition, int pSameRow, int pPayloadSize)
        {
            this.numRobots = pNumRobots;
            this.runTime = pRunTime;
            this.percentReads = pPercentReads;
            this.percentWrites = 100 - pPercentReads;
            this.samePartition = pSamePartition;
            this.sameRow = pSameRow;
            this.payloadSize = pPayloadSize;


        }


        public String RobotServiceEndpoint(int workernumber)
        {

            return Endpoints.GetDefaultService();

        }

        public string Name { get { return string.Format("http-robots{0}xnr{1}xreads{2}xpkey{3}xrkey{4}xsize{5}", numRobots, runTime, percentReads, samePartition, sameRow, payloadSize); } }

        public int NumRobots { get { return numRobots; } }

        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numRobots];


                // start each robot
                for (int i = 0; i < numRobots; i++)
                    robotrequests[i] = context.RunRobot(i, "");

                // wait for all robots
                await Task.WhenAll(robotrequests);

                int totalOps = 0;
                double throughput = 0.0;
                // check robot responses
                for (int i = 0; i < numRobots; i++)
                {
                    string response = "";
                    try
                    {
                        response = robotrequests[i].Result;
                        string[] res = response.Split('-');
                        totalOps += int.Parse(res[0]);
                    }
                    catch (Exception e)
                    {
                        throw new Exception("Robot failed to return totOps value " + response + " " + e.ToString());
                    }
                }
                throughput = totalOps / runTime;
                return throughput.ToString();
           
  
        }


        private AzureUtils.OperationType generateOperationType()
        {
            AzureUtils.OperationType retType;
            int nextInt;

            nextInt = rnd.Next(1, 100);

            if (nextInt <= percentReads)
            {
                retType = AzureUtils.OperationType.READ;
            }
            else
            {
                retType = AzureUtils.OperationType.UPDATE;
            }
            return retType;
        }

        private string generatePartitionKey()
        {
            if (samePartition == 1)
            {
                return DEFAULT_PARTITION_KEY;
            }
            else
            {
                return AzureUtils.generateKey(PARTITION_KEY_SIZE);
            }
            throw new Exception("Parameter out of bound" + samePartition);
        }

        private string generateRowKey()
        {
            if (sameRow == 1)
            {
                return DEFAULT_ROW_KEY;
            }
            else
            {
                return AzureUtils.generateKey(PARTITION_KEY_SIZE);
            }
            throw new Exception("Parameter out of bound" + sameRow);
        }


        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);

            AzureUtils.OperationType nextOp;
            int totReads = 0;
            int totWrites = 0;
            byte[] nextPayload = new byte[payloadSize];
            string testTable = "helloworld";
            ByteEntity nextEntity = null;
            string nextResult = null;
            int totOps = 0;

            nextResult = await context.ServiceRequest(new HttpRequestAzureTable(AzureUtils.OperationType.CREATE, totOps * robotnumber, testTable, null, null, null));

            Stopwatch s = new Stopwatch();
            s.Start();
            while (true)
            {
                s.Stop();

                if (s.ElapsedMilliseconds > runTime * 1000) break;

                s.Start();

                nextOp = generateOperationType();
              
                switch (nextOp)
                {
                    case AzureUtils.OperationType.READ:
                        nextResult = await context.ServiceRequest(new HttpRequestAzureTable(nextOp, totOps * robotnumber, testTable, generatePartitionKey(), generateRowKey(), null));
                        totReads++;
                        if (nextResult.Equals("404"))
                        {
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        if (nextResult.Equals(""))
                        {
                            Console.Write("Empty Entity \n ");
                        }
                        else
                        {
                            ByteEntity entity = ByteEntity.FromJsonToEntity(nextResult);
                        }
                        totOps++;
                        break;
                    case AzureUtils.OperationType.UPDATE:
                        rnd.NextBytes(nextPayload);
                        nextEntity = new ByteEntity(generatePartitionKey(), generateRowKey(), nextPayload);
                        nextResult = await context.ServiceRequest(new HttpRequestAzureTable(nextOp, totOps * robotnumber, testTable, generatePartitionKey(), generateRowKey(), nextEntity));
                        totWrites++;
                        if (!nextResult.Equals("204"))
                        {
                            
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        totOps++;
                        break;
                    default:
                        throw new NotImplementedException();
                } // end switch
            }

            string result = string.Format("Executed {0}% Reads {0}% Writes \n ", ((double)totReads / (double)totOps) * 100, ((double)totWrites / (double)totOps) * 100);

            return totOps.ToString() + "-" + s.ElapsedMilliseconds;

        }




    }



    public class HttpRequestAzureTable : IHttpRequest
    {

        /// <summary>
        /// Constructor for HTTP Calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestAzureTable(AzureUtils.OperationType pRequestType, int pNumReq, string pTable, string pPartitionKey, string pRowKey, ByteEntity pEntity)
        {

            this.requestType = pRequestType;
            this.numReq = pNumReq;
            this.tableName = pTable;
            this.partitionKey = pPartitionKey;
            this.rowKey = pRowKey;
            this.payload = pEntity;

            if (tableName == null) throw new Exception("Incorrect parameters");
        }


        // Request number
        private int numReq;
        // Request type
        private AzureUtils.OperationType requestType;
        // Payload type (used in UPDATE requests)
        private ByteEntity payload;
        // Desired partition key 
        private string partitionKey;
        // Desired row key (used in GET requests)
        private string rowKey;
        // Table name
        private string tableName;

        public string Signature
        {
            get
            {
                if (requestType == AzureUtils.OperationType.CREATE)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName;
                }
                if (requestType == AzureUtils.OperationType.READ)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey + "&rkey=" + rowKey;
                }
                else if (requestType == AzureUtils.OperationType.READ_RANGE)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey;

                }
                else if (requestType == AzureUtils.OperationType.READ_BATCH)
                {
                    throw new NotImplementedException();
                }
                else if (requestType == AzureUtils.OperationType.INSERT)
                {
                    throw new NotImplementedException();

                }
                else if (requestType == AzureUtils.OperationType.INSERT_BATCH)
                {
                    throw new NotImplementedException();

                }
                else if (requestType == AzureUtils.OperationType.UPDATE)
                {
                    return "POST azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName;
                }
                else if (requestType == AzureUtils.OperationType.UPDATE_BATCH)
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
                else return ByteEntity.FromEntityToJsonString(payload);

            }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            CloudTableClient tableClient = AzureUtils.getTableClient();
            string result = "ok";

            //TODO(natacha): don't know how costly retrieving this is for
            // every request. Make this persistent somehow?
            if (requestType == AzureUtils.OperationType.CREATE)
            {
                CloudTable table = AzureUtils.createTable(tableClient, tableName);
                //todo : handle error
                return "ok";
            }
            if (requestType == AzureUtils.OperationType.READ)
            {
                TableResult res =
                    await AzureUtils.findEntity<ByteEntity>(tableClient, tableName, partitionKey, rowKey);
                if (res.HttpStatusCode == 404)
                {
                    result = "";
                }
                else
                {
                    ByteEntity entity = (ByteEntity)res.Result;
                    result = ByteEntity.FromEntityToJsonString(entity);
                }
                return result;

            }
            else if (requestType == AzureUtils.OperationType.READ_RANGE)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureUtils.OperationType.READ_BATCH)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureUtils.OperationType.INSERT)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureUtils.OperationType.INSERT_BATCH)
            {
                return "Unimplemented";
            }
            else if (requestType == AzureUtils.OperationType.UPDATE)
            {
                TableResult res =
                        await AzureUtils.updateEntity<ByteEntity>(tableClient, tableName, payload);
                return res.HttpStatusCode.ToString();
            }
            else if (requestType == AzureUtils.OperationType.UPDATE_BATCH)
            {
                return "Unimplemented";
            }

            return "ok";
        }



        public Task<string> ProcessResponseOnClient(string response)
        {
            return Task.FromResult(response);
        }


        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }


}


