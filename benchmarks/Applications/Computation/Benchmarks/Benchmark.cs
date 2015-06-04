using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common;
using Computation.Interfaces;

#pragma warning disable 1998

namespace Computation.Benchmark
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "computation"; } }

        // list of scenarios for this benchmark
        public IEnumerable<IScenario> Scenarios { get { return scenarios; } }

        private IScenario[] scenarios = new IScenario[] 
        {
            
            /* Robots generate read/write requests in the proportions specified below.
             * Requests are generated in an open-loop and are not currently rate-controlled
             * All robots execute the same load.
             * Staleness bound is set to int.maxValue
             */ 
   
             // TODO: add other robot values


             /* 3. FOR 100 ROBOT */

            /* 3.1 Read-Only Benchmarks */
            /* 3.1.1 1 ms */
            // No replication
            new NoReplicationComputation(100, 180,100,1),
            // Sequenced Grain. All Global Reads
            new SequencedComputation(100,180,100,0,0,0,1),
            // Sequenced Grain, All Local Reads
            new SequencedComputation(100,180,0,100,0,0,1),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,75,25,0,0,1),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,50,50,0,0,1),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,25,75,0,0,1),
             /* 3.1.2 10 ms*/
            // No replication
            new NoReplicationComputation(100, 180,100,10),
            // Sequenced Grain. All Global Reads
            new SequencedComputation(100,180,100,0,0,0,10 ),
            // Sequenced Grain, All Local Reads
            new SequencedComputation(100,180,0,100,0,0,10 ),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,75,25,0,0,10 ),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,50,50,0,0,10 ),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,25,75,0,0,10 ),
         /* 3.1.3 100ms*/
            // No replication
            new NoReplicationComputation(100, 180,100, 100),
            // Sequenced Grain. All Global Reads
            new SequencedComputation(100,180,100,0,0,0, 100),
            // Sequenced Grain, All Local Reads
            new SequencedComputation(100,180,0,100,0,0, 100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,75,25,0,0, 100),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,50,50,0,0, 100),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,25,75,0,0, 100),
        
            /* 3.2 Write-Only Benchmarks */
             /* 3.2.1 1ms*/
            // No replication
            new NoReplicationComputation(100, 180,0,1),
            // Sequenced Grain. All Global writes
            new SequencedComputation(100,180,0,0,100,0,1),
            // Sequenced Grain, All Local writes
            new SequencedComputation(100,180,0,0,0,100,1),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,0,0,75,25,1),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,0,0,50,50,1),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,0,0,25,75,1),
             /* 3.2.2 10ms*/
            new NoReplicationComputation(100, 180,0,10 ),
            // Sequenced Grain. All Global writes
            new SequencedComputation(100,180,0,0,100,0,10),
            // Sequenced Grain, All Local writes
            new SequencedComputation(100,180,0,0,0,100,10),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,0,0,75,25,10),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,0,0,50,50,10),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,0,0,25,75,10),
             /* 3.2.3 100ms*/
            new NoReplicationComputation(100, 180,0, 100),
            // Sequenced Grain. All Global writes
            new SequencedComputation(100,180,0,0,100,0, 100),
            // Sequenced Grain, All Local writes
            new SequencedComputation(100,180,0,0,0,100, 100),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedComputation(100,180,0,0,75,25, 100),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedComputation(100,180,0,0,50,50, 100),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedComputation(100,180,0,0,25,75, 100),

            /* 3.3 Read-Write Benchmarks */
            /* 3.3.4 Read mostly (ratio rw: 90/10) */

              // no replication
              new NoReplicationComputation(100, 180,90,1),
              // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,90,0,10,0,1),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,90,0,10,1),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,45,45,5,5,1),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,90,10,0,1),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,90,0,0,10,1),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,45,45,0,10,1),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,45,45,10,0,1),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,90,0,5,5,1),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,90,5,5,1),

                 // no replication
              new NoReplicationComputation(100, 180,90, 10),
              // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,90,0,10,0, 10),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,90,0,10, 10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,45,45,5,5, 10),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,90,10,0, 10),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,90,0,0,10, 10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,45,45,0,10, 10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,45,45,10,0, 10),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,90,0,5,5, 10),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,90,5,5, 10),

                 // no replication
              new NoReplicationComputation(100, 180,90, 100),
              // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,90,0,10,0, 100),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,90,0,10, 100),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,45,45,5,5, 100),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,90,10,0, 100),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,90,0,0,10, 100),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,45,45,0,10, 100),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,45,45,10,0, 100),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,90,0,5,5, 100),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,90,5,5, 100),

            /* 3.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationComputation(100, 180,70, 1),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,70,0,30,0, 1),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,70,0,30, 1),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,35,35,15,15, 1),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,70,30,0, 1),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,70,0,0,30, 1),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,35,35,0,30, 1),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,35,35,30,0, 1),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,70,0,15,15, 1),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,70,15,15, 1),

                  new NoReplicationComputation(100, 180,70, 10),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,70,0,30,0, 10),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,70,0,30, 10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,35,35,15,15, 10),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,70,30,0, 10),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,70,0,0,30, 10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,35,35,0,30, 10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,35,35,30,0, 10),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,70,0,15,15, 10),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,70,15,15, 10),

                  new NoReplicationComputation(100, 180,70, 100),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,70,0,30,0, 100),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,70,0,30, 100),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,35,35,15,15, 100),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,70,30,0, 100),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,70,0,0,30, 100),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,35,35,0,30, 100),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,35,35,30,0, 100),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,70,0,15,15, 100),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,70,15,15, 100),


            /* 2.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationComputation(100, 180,50, 1),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,50,0,50,0, 1),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,50,0,50, 1),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,25,25,25,25, 1),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,50,50,0, 1),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,50,0,0,50, 1),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,25,25,0,50, 1),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,25,25,50,0, 1),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,50,0,25,25, 1),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,50,25,25, 1),

             new NoReplicationComputation(100, 180,50, 10),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,50,0,50,0, 10),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,50,0,50, 10),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,25,25,25,25, 10),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,50,50,0, 10),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,50,0,0,50, 10),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,25,25,0,50, 10),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,25,25,50,0, 10),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,50,0,25,25, 10),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,50,25,25, 10),

              new NoReplicationComputation(100, 180,50, 100),
                // Sequenced Grain. All Global ops
             new SequencedComputation(100,180,50,0,50,0, 100),
              // Sequenced Grain. All Local ops
             new SequencedComputation(100,180,0,50,0,50, 100),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedComputation(100,180,25,25,25,25, 100),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedComputation(100,180,0,50,50,0, 100),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedComputation(100,180,50,0,0,50, 100),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedComputation(100,180,25,25,0,50, 100),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedComputation(100,180,25,25,50,0, 100),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,50,0,25,25, 100),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedComputation(100,180,0,50,25,25, 100),



           
        };

        public IEnumerable<IScenario> generateScenariosFromJSON(string pJsonFile)
        {
            throw new NotImplementedException();
        }


        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {

            if (verb == "WS" && string.Join("/", urlpath) == "computation")
            {
                throw new NotImplementedException();
            }

            if (verb == "GET" && string.Join("/", urlpath) == "computation")
            {

                if (int.Parse(arguments["rep"]) == 0)
                {
                    Console.Write("{0}", arguments);
                    ComputationRequestT requestType = (ComputationRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestComputation request = null;
                    if (requestType == ComputationRequestT.READ_SYNC)
                    {
                        // READ type
                        request = new HttpRequestComputation(numReq);
                    }
                    else if (requestType == ComputationRequestT.WRITE_SYNC)
                    {
                        // WRITE type
                        Util.Assert(false, "Should be of POST type");
                    }

                    return request;
                }

                else
                {
                    Console.Write("{0}", arguments);
                    ComputationRequestT requestType = (ComputationRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestSequencedComputation request = null;
                    if (requestType == ComputationRequestT.READ_SYNC)
                    {
                        // ReadCurrent type
                        request = new HttpRequestSequencedComputation(numReq, false);
                    }
                    else if (requestType == ComputationRequestT.READ_ASYNC)
                    {

                        // ReadLater type
                        request = new HttpRequestSequencedComputation(numReq, true);

                    }

                    return request;
                }
            }
            else if (verb == "POST" && string.Join("/", urlpath) == "computation")
            {
                Console.Write("{0}", arguments);
                ComputationRequestT requestType = (ComputationRequestT)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);
                int timeUpdate = int.Parse(arguments["time"]);

                if (int.Parse(arguments["rep"]) == 0)
                {

                    HttpRequestComputation request = null;
                    if (requestType == ComputationRequestT.WRITE_SYNC)
                    {
                        request = new HttpRequestComputation(numReq, Encoding.ASCII.GetBytes(body), timeUpdate);
                    }
                    else
                    {
                        Util.Assert(false, "Incorrect message type");
                    }

                    return request;
                }
                else
                {
                    HttpRequestSequencedComputation request = null;

                    if (requestType == ComputationRequestT.WRITE_SYNC)
                    {
                        // Write Now Type
                        request = new HttpRequestSequencedComputation(numReq, Encoding.ASCII.GetBytes(body), timeUpdate, false);
                    }
                    else if (requestType == ComputationRequestT.WRITE_ASYNC)
                    {
                        // Write Later Type
                        request = new HttpRequestSequencedComputation(numReq, Encoding.ASCII.GetBytes(body), timeUpdate, true);
                    }
                    return request;
                }
            }

            return null; // URL not recognized
        }

    }


}
