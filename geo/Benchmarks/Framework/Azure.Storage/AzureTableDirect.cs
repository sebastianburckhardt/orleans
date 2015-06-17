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

    public class AzureTableDirect : IScenario
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


        public AzureTableDirect(int pNumRobots, int pRunTime, int pPercentReads, int pSamePartition, int pSameRow, int pPayloadSize)
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

        public string Name { get { return string.Format("direct-robots{0}xnr{1}xreads{2}xpkey{3}xrkey{4}xsize{5}", numRobots, runTime, percentReads, samePartition, sameRow, payloadSize); } }

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

            string[] param = parameters.Split('-');
            AzureUtils.OperationType nextOp;
            int totReads = 0;
            int totWrites = 0;
            int totOps = 0;

            byte[] nextPayload = new byte[payloadSize];
            string testTable = "testTable";
            ByteEntity nextEntity = null;
            TableResult nextResult = null;


            CloudTableClient azureClient = AzureUtils.getTableClient();
            bool created = AzureUtils.createTableCheck(azureClient, testTable);

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
                        nextResult = await AzureUtils.findEntity<ByteEntity>(azureClient, testTable, generatePartitionKey(), generateRowKey());
                        totReads++;
 
                            ByteEntity b = (ByteEntity)nextResult.Result;
                            if (b != null)
                            {
                                Console.Write("READ: {0} {1} {2} ", b.PartitionKey, b.RowKey, Encoding.ASCII.GetString(b.payload));
                            }
 
                        totOps++;
                        break;
                    case AzureUtils.OperationType.UPDATE:
                        rnd.NextBytes(nextPayload);
                        nextEntity = new ByteEntity(generatePartitionKey(), generateRowKey(), nextPayload);
                        Console.Write("UPDATE: {0} {1} {2} ", nextEntity.PartitionKey, nextEntity.RowKey, Encoding.ASCII.GetString(nextEntity.payload));
                        nextResult = await AzureUtils.updateEntity<ByteEntity>(azureClient, "testTable", nextEntity);
                        totWrites++;
                        if (nextResult == null || nextResult.HttpStatusCode != 204)
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



}


