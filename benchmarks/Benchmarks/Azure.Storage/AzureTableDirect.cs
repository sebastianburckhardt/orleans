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

    public class AzureTableDirect : IScenario
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

      
        public AzureTableDirect(int pNumRobots, int pNumReqs, int pPercentReads,  int pSamePartition, int pSameRow, int pPayloadSize)
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

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xreads{2}xpkey{3}xrkey{4}xsize{5}", numRobots, numReqs,percentReads,samePartition,sameRow, payloadSize); } }

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
                TableResult nextResult = null;

                
                CloudTableClient azureClient = AzureCommon.getTableClient();
                bool created = AzureCommon.createTableCheck(azureClient, testTable);
           
            for (int i = 0; i < numReqs; i++)
            {
                nextOp = generateOperationType();
                switch (nextOp)
                {
                    case AzureCommon.OperationType.READ:
                        nextResult = await AzureCommon.findEntity<ByteEntity>(azureClient, testTable, generatePartitionKey(), generateRowKey());
                        totReads++;
                        if (!nextResult.Equals("200"))
                        {
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        break;
                    case AzureCommon.OperationType.UPDATE:
                        rnd.NextBytes(nextPayload);
                        nextEntity = new ByteEntity(generatePartitionKey(), generateRowKey(), nextPayload);
                        nextResult = await AzureCommon.updateEntity<ByteEntity>(azureClient, "testTable", nextEntity);
                        totWrites++;
                        if (nextResult == null || nextResult.HttpStatusCode!=204)
                        {
                            throw new Exception("HTTP Return Code " + nextResult);
                        }
                        break;
                    default:
                        throw new NotImplementedException();
                } // end switch
            }

            Console.Write("Executed {0} Reads {0} Writes \n ", ((double)totReads / (double)numReqs) * 100, ((double)totWrites / (double)numReqs) * 100);
                   
            return "ok";
        }




    }

   

}


