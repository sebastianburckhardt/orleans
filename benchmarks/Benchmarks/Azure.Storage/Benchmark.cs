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

            

           
        };

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
                    AzureCommon.OperationType requestType = (AzureCommon.OperationType)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);
                    string partitionKey = arguments["pkey"];
                    string rowKey = arguments["rkey"];

                    HttpRequestAzureTable request = null;
                    if (requestType == AzureCommon.OperationType.READ)
                    {
                        // READ type
                         request = new HttpRequestAzureTable(numReq,partitionKey, rowKey);
                    } else if (requestType == AzureCommon.OperationType.READ_RANGE)
                    {
                        // READ type
                         request = new HttpRequestAzureTable(numReq,partitionKey);
                    }
                    else if (requestType == AzureCommon.OperationType.INSERT)
                    {
                        // INSERT type
                        Util.Assert(false, "Should be of POST type");
                    } else if (requestType == AzureCommon.OperationType.INSERT_BATCH) {
                        Util.Assert(false, "Should be of POST type");
                    }

                    return request;
                }
            else if (verb == "POST" && string.Join("/", urlpath) == "size")
            {
                Console.Write("{0}", arguments);
                AzureCommon.OperationType requestType = (AzureCommon.OperationType)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);

                    HttpRequestAzureTable request = null;
                    if (requestType == AzureCommon.OperationType.UPDATE)
                    {      
                       // request = new HttpRequestAzureTable(numReq, body,false);
                    }
                    else
                    {
                        Util.Assert(false, "Incorrect message type");
                    }

                    return request;

              
            }

            return null; // URL not recognized
        }

    }


}
