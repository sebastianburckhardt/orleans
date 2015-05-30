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


        private int numRobots;
        private int numReqs;
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


        public AzureTableStorage(int pNumRobots, int pNumReqs, int pPercentReads, int pSamePartition, int pSameRow, int pPayloadSize)
        {
            this.numRobots = pNumRobots;
            this.numReqs = pNumReqs;
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

        public string Name { get { return string.Format("http-robots{0}xnr{1}xreads{2}xpkey{3}xrkey{4}xsize{5}", numRobots, numReqs, percentReads, samePartition, sameRow, payloadSize); } }

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


        private AzureCommon.OperationType generateOperationType()
        {
            AzureCommon.OperationType retType;
            int nextInt;

            nextInt = rnd.Next(1, 100);

            if (nextInt <= percentReads)
            {
                retType = AzureCommon.OperationType.READ;
            }
            else
            {
                retType = AzureCommon.OperationType.UPDATE;
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
                return AzureCommon.generateKey(PARTITION_KEY_SIZE);
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
                return AzureCommon.generateKey(PARTITION_KEY_SIZE);
            }
            throw new Exception("Parameter out of bound" + sameRow);
        }


        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);

            string[] param = parameters.Split('-');
            AzureCommon.OperationType nextOp;
            int totReads = 0;
            int totWrites = 0;
            byte[] nextPayload = new byte[payloadSize];
            string testTable = "testTable";
            ByteEntity nextEntity = null;
            string nextResult = null;

            nextResult = await context.ServiceRequest(new HttpRequestAzureTable(AzureCommon.OperationType.CREATE, numReqs * robotnumber, testTable, null, null, null));

            for (int i = 0; i < numReqs; i++)
            {
                nextOp = generateOperationType();
                switch (nextOp)
                {
                    case AzureCommon.OperationType.READ:
                        nextResult = await context.ServiceRequest(new HttpRequestAzureTable(nextOp, numReqs * robotnumber + i, testTable, generatePartitionKey(), generateRowKey(), null));
                        totReads++;
                        if (!nextResult.Equals("200"))
                        {
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        break;
                    case AzureCommon.OperationType.UPDATE:
                        rnd.NextBytes(nextPayload);
                        nextEntity = new ByteEntity(generatePartitionKey(), generateRowKey(), nextPayload);
                        nextResult = await context.ServiceRequest(new HttpRequestAzureTable(nextOp, numReqs * robotnumber + i, testTable, generatePartitionKey(), generateRowKey(), nextEntity));
                        totWrites++;
                        if (!nextResult.Equals("200"))
                        {
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                } // end switch
            }

            string result = string.Format("Executed {0}% Reads {0}% Writes \n ", ((double)totReads / (double)numReqs) * 100, ((double)totWrites / (double)numReqs) * 100);

            return "ok: " + result;
        }




    }



    public class HttpRequestAzureTable : IHttpRequest
    {

        /// <summary>
        /// Constructor for HTTP Calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestAzureTable(AzureCommon.OperationType pRequestType, int pNumReq, string pTable, string pPartitionKey, string pRowKey, ByteEntity pEntity)
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
        // Request type
        private AzureCommon.OperationType requestType;
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
                if (requestType == AzureCommon.OperationType.CREATE)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName;
                }
                if (requestType == AzureCommon.OperationType.READ)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey + "&rkey=" + rowKey;
                }
                else if (requestType == AzureCommon.OperationType.READ_RANGE)
                {
                    return "GET azure?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&table=" + tableName + "&pkey=" + partitionKey;

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
                    await AzureCommon.findEntity<ByteEntity>(tableClient, tableName, partitionKey, rowKey);
                if (res.HttpStatusCode == 404)
                {
                    result = res.HttpStatusCode.ToString();
                }
                else
                {
                    ByteEntity entity = (ByteEntity)res.Result;
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
                        await AzureCommon.updateEntity<ByteEntity>(tableClient, tableName, payload);
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
            return Task.FromResult(response);
        }


        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }


}


