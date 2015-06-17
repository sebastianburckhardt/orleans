using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common;
using Azure.Storage;

#pragma warning disable 1998

namespace Azure.Storage
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "azure"; } }

        // list of scenarios for this benchmark
        public IEnumerable<IScenario> Scenarios { get { return scenarios; } }

        private IScenario[] scenarios = new IScenario[] 
        {
            
            /* Robots generate read/write requests in the proportions specified below.
             * Requests are generated in an open-loop and are not currently rate-controlled
             * All robots execute the same load.
             * Staleness bound is set to int.maxValue
             */ 

            new AzureTableStorage(1,60,100,1,1,100),
            new AzureTableStorage(1,60,100,1,0,100),
            new AzureTableStorage(1,60,100,0,0,100),
            new AzureTableStorage(1,60,0,1,1,100),
            new AzureTableStorage(1,60,0,1,0,100),
            new AzureTableStorage(1,60,0,0,0,100),

            new AzureTableStorage(5,60,100,1,1,100),
            new AzureTableStorage(5,60,100,1,0,100),
            new AzureTableStorage(5,60,100,0,0,100),
            new AzureTableStorage(5,60,0,1,1,100),
            new AzureTableStorage(5,60,0,1,0,100),
            new AzureTableStorage(5,60,0,0,0,100),

            new AzureTableStorage(10,60,100,1,1,100),
            new AzureTableStorage(10,60,100,1,0,100),
            new AzureTableStorage(10,60,100,0,0,100),
            new AzureTableStorage(10,60,0,1,1,100),
            new AzureTableStorage(10,60,0,1,0,100),
            new AzureTableStorage(10,60,0,0,0,100),

            new AzureTableStorage(20,60,100,1,1,100),
            new AzureTableStorage(20,60,100,1,0,100),
            new AzureTableStorage(20,60,100,0,0,100),
            new AzureTableStorage(20,60,0,1,1,100),
            new AzureTableStorage(20,60,0,1,0,100),
            new AzureTableStorage(20,60,0,0,0,100),

            new AzureTableStorage(50,60,100,1,1,100),
            new AzureTableStorage(50,60,100,1,0,100),
            new AzureTableStorage(50,60,100,0,0,100),
            new AzureTableStorage(50,60,0,1,1,100),
            new AzureTableStorage(50,60,0,1,0,100),
            new AzureTableStorage(50,60,0,0,0,100),

              new AzureTableStorage(100,60,100,1,1,100),
            new AzureTableStorage(100,60,100,1,0,100),
            new AzureTableStorage(100,60,100,0,0,100),
            new AzureTableStorage(100,60,0,1,1,100),
            new AzureTableStorage(100,60,0,1,0,100),
            new AzureTableStorage(100,60,0,0,0,100),

               new AzureTableStorage(500,60,100,1,1,100),
            new AzureTableStorage(500,60,100,1,0,100),
            new AzureTableStorage(500,60,100,0,0,100),
            new AzureTableStorage(500,60,0,1,1,100),
            new AzureTableStorage(500,60,0,1,0,100),
            new AzureTableStorage(500,60,0,0,0,100),

            new AzureTableDirect(1,60,100,1,1,100),
            new AzureTableDirect(1,60,100,1,0,100),
            new AzureTableDirect(1,60,100,0,0,100),
            new AzureTableDirect(1,60,0,1,1,100),
            new AzureTableDirect(1,60,0,1,0,100),
            new AzureTableDirect(1,60,0,0,0,100),

            new AzureTableDirect(5,60,100,1,1,100),
            new AzureTableDirect(5,60,100,1,0,100),
            new AzureTableDirect(5,60,100,0,0,100),
            new AzureTableDirect(5,60,0,1,1,100),
            new AzureTableDirect(5,60,0,1,0,100),
            new AzureTableDirect(5,60,0,0,0,100),

            new AzureTableDirect(10,60,100,1,1,100),
            new AzureTableDirect(10,60,100,1,0,100),
            new AzureTableDirect(10,60,100,0,0,100),
            new AzureTableDirect(10,60,0,1,1,100),
            new AzureTableDirect(10,60,0,1,0,100),
            new AzureTableDirect(10,60,0,0,0,100),

            new AzureTableDirect(20,60,100,1,1,100),
            new AzureTableDirect(20,60,100,1,0,100),
            new AzureTableDirect(20,60,100,0,0,100),
            new AzureTableDirect(20,60,0,1,1,100),
            new AzureTableDirect(20,60,0,1,0,100),
            new AzureTableDirect(20,60,0,0,0,100),

            new AzureTableDirect(50,60,100,1,1,100),
            new AzureTableDirect(50,60,100,1,0,100),
            new AzureTableDirect(50,60,100,0,0,100),
            new AzureTableDirect(50,60,0,1,1,100),
            new AzureTableDirect(50,60,0,1,0,100),
            new AzureTableDirect(50,60,0,0,0,100),

              new AzureTableDirect(100,60,100,1,1,100),
            new AzureTableDirect(100,60,100,1,0,100),
            new AzureTableDirect(100,60,100,0,0,100),
            new AzureTableDirect(100,60,0,1,1,100),
            new AzureTableDirect(100,60,0,1,0,100),
            new AzureTableDirect(100,60,0,0,0,100),

               new AzureTableDirect(500,60,100,1,1,100),
            new AzureTableDirect(500,60,100,1,0,100),
            new AzureTableDirect(500,60,100,0,0,100),
            new AzureTableDirect(500,60,0,1,1,100),
            new AzureTableDirect(500,60,0,1,0,100),
            new AzureTableDirect(500,60,0,0,0,100),

           
        };

        public IEnumerable<IScenario> generateScenariosFromJSON(string pJsonFile)
        {
            throw new NotImplementedException();
        }

        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {

            if (verb == "WS" && string.Join("/", urlpath) == "azure")
            {
                throw new NotImplementedException();
            }

            if (verb == "GET" && string.Join("/", urlpath) == "azure")
            {


                Console.Write("{0}", arguments);
                AzureUtils.OperationType requestType = (AzureUtils.OperationType)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);
                string partitionKey = arguments["pkey"];
                string table = arguments["table"];

                HttpRequestAzureTable request = null;
                if (requestType == AzureUtils.OperationType.READ)
                {
                    // READ type
                    string rowKey = arguments["rkey"];
                    request = new HttpRequestAzureTable(requestType, numReq, table, partitionKey, rowKey, null);
                }
                else if (requestType == AzureUtils.OperationType.READ_RANGE)
                {
                    // READ RANGE type
                    request = new HttpRequestAzureTable(requestType, numReq, table, partitionKey, null, null);
                }
                else if (requestType == AzureUtils.OperationType.CREATE)
                {
                    // CREATE type
                    request = new HttpRequestAzureTable(requestType, numReq, table, null, null, null);
                }
                else
                {
                    Util.Fail("Should be of post type");
                }

                return request;
            }
            else if (verb == "POST" && string.Join("/", urlpath) == "azure")
            {
                Console.Write("{0}", arguments);
                AzureUtils.OperationType requestType = (AzureUtils.OperationType)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);
                string table = arguments["table"];

                HttpRequestAzureTable request = null;
                if (requestType == AzureUtils.OperationType.UPDATE)
                {
                    ByteEntity entity = Azure.Storage.ByteEntity.FromJsonToEntity(body);
                    request = new HttpRequestAzureTable(requestType, numReq, table, null, null, entity);
                }
                else
                {
                    throw new NotImplementedException();
                }

                return request;


            }

            return null; // URL not recognized
        }

    }


}
