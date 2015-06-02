using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common;
using Size.Interfaces;

#pragma warning disable 1998

namespace Size.Benchmark
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "size"; } }

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
            /* 3.1.1 128B*/
            // No replication
            new NoReplicationSize(100, 180,100,128),
            // Sequenced Grain. All Global Reads
            new SequencedSize(100,180,100,0,0,0,128),
            // Sequenced Grain, All Local Reads
            new SequencedSize(100,180,0,100,0,0,128),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,180,75,25,0,0,128),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,180,50,50,0,0,128),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,180,25,75,0,0,128),
             /* 3.1.2 1KB*/
            // No replication
            new NoReplicationSize(100, 180,100,1024),
            // Sequenced Grain. All Global Reads
            new SequencedSize(100,180,100,0,0,0,1024),
            // Sequenced Grain, All Local Reads
            new SequencedSize(100,180,0,100,0,0,1024),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,180,75,25,0,0,1024),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,360,50,50,0,0,1024),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,360,25,75,0,0,1024),
         /* 3.1.3 1MB*/
            // No replication
            new NoReplicationSize(100, 360,100, 1048576),
            // Sequenced Grain. All Global Reads
            new SequencedSize(100,360,100,0,0,0, 1048576),
            // Sequenced Grain, All Local Reads
            new SequencedSize(100,360,0,100,0,0, 1048576),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,360,75,25,0,0, 1048576),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,360,50,50,0,0, 1048576),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,360,25,75,0,0, 1048576),
        
            /* 3.2 Write-Only Benchmarks */
             /* 3.2.1 128B*/
            // No replication
            new NoReplicationSize(100, 360,0,128),
            // Sequenced Grain. All Global writes
            new SequencedSize(100,360,0,0,100,0,128),
            // Sequenced Grain, All Local writes
            new SequencedSize(100,360,0,0,0,100,128),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,360,0,0,75,25,128),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,360,0,0,50,50,128),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,360,0,0,25,75,128),
             /* 3.2.2 1KB*/
            new NoReplicationSize(100, 360,0,1024),
            // Sequenced Grain. All Global writes
            new SequencedSize(100,360,0,0,100,0,1024),
            // Sequenced Grain, All Local writes
            new SequencedSize(100,360,0,0,0,100,1024),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,360,0,0,75,25,1024),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,360,0,0,50,50,1024),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,360,0,0,25,75,1024),
             /* 3.2.3 1MB*/
            new NoReplicationSize(100, 360,0, 1048576),
            // Sequenced Grain. All Global writes
            new SequencedSize(100,360,0,0,100,0, 1048576),
            // Sequenced Grain, All Local writes
            new SequencedSize(100,360,0,0,0,100, 1048576),
            // Sequenced Grain. 75 Global / 25 Local
            new SequencedSize(100,360,0,0,75,25, 1048576),
              // Sequenced Grain. 50 Global / 50 Local
            new SequencedSize(100,360,0,0,50,50, 1048576),
              // Sequenced Grain. 25 Global / 75 Local
            new SequencedSize(100,360,0,0,25,75, 1048576),

            /* 3.3 Read-Write Benchmarks */
            /* 3.3.4 Read mostly (ratio rw: 90/10) */

              // no replication
              new NoReplicationSize(100, 360,90,128),
              // Sequenced Grain. All Global ops
             new SequencedSize(100,360,90,0,10,0,128),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,360,0,90,0,10,128),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,360,45,45,5,5,128),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,360,0,90,10,0,128),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,360,90,0,0,10,128),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,360,45,45,0,10,128),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,360,45,45,10,0,128),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,90,0,5,5,128),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,0,90,5,5,128),

                 // no replication
              new NoReplicationSize(100, 360,90, 1024),
              // Sequenced Grain. All Global ops
             new SequencedSize(100,360,90,0,10,0, 1024),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,360,0,90,0,10, 1024),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,360,45,45,5,5, 1024),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,360,0,90,10,0, 1024),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,360,90,0,0,10, 1024),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,360,45,45,0,10, 1024),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,360,45,45,10,0, 1024),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,90,0,5,5, 1024),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,0,90,5,5, 1024),

                 // no replication
              new NoReplicationSize(100, 360,90, 1048576),
              // Sequenced Grain. All Global ops
             new SequencedSize(100,360,90,0,10,0, 1048576),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,360,0,90,0,10, 1048576),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,360,45,45,5,5, 1048576),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,360,0,90,10,0, 1048576),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,360,90,0,0,10, 1048576),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,360,45,45,0,10, 1048576),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,360,45,45,10,0, 1048576),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,90,0,5,5, 1048576),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,360,0,90,5,5, 1048576),

            /* 3.3.5 Write heavy (ratio rw: 70/30) */
            new NoReplicationSize(100, 360,70, 128),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,360,70,0,30,0, 128),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,360,0,70,0,30, 128),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,360,35,35,15,15, 128),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,360,0,70,30,0, 128),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,360,70,0,0,30, 128),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,360,35,35,0,30, 128),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,360,35,35,30,0, 128),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,70,0,15,15, 128),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,70,15,15, 128),

                  new NoReplicationSize(100, 180,70, 1024),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,180,70,0,30,0, 1024),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,180,0,70,0,30, 1024),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,180,35,35,15,15, 1024),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,180,0,70,30,0, 1024),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,180,70,0,0,30, 1024),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,180,35,35,0,30, 1024),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,180,35,35,30,0, 1024),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,70,0,15,15, 1024),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,70,15,15, 1024),

                  new NoReplicationSize(100, 180,70, 1048576),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,180,70,0,30,0, 1048576),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,180,0,70,0,30, 1048576),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,180,35,35,15,15, 1048576),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,180,0,70,30,0, 1048576),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,180,70,0,0,30, 1048576),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,180,35,35,0,30, 1048576),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,180,35,35,30,0, 1048576),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,70,0,15,15, 1048576),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,70,15,15, 1048576),


            /* 2.3.6 Read/Write (ratio rw: 50/50) */
            new NoReplicationSize(100, 180,50, 128),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,180,50,0,50,0, 128),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,180,0,50,0,50, 128),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,180,25,25,25,25, 128),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,180,0,50,50,0, 128),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,180,50,0,0,50, 128),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,180,25,25,0,50, 128),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,180,25,25,50,0, 128),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,50,0,25,25, 128),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,50,25,25, 128),

             new NoReplicationSize(100, 180,50, 1024),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,180,50,0,50,0, 1024),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,180,0,50,0,50, 1024),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,180,25,25,25,25, 1024),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,180,0,50,50,0, 1024),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,180,50,0,0,50, 1024),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,180,25,25,0,50, 1024),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,180,25,25,50,0, 1024),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,50,0,25,25, 1024),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,50,25,25, 1024),

              new NoReplicationSize(100, 180,50, 1048576),
                // Sequenced Grain. All Global ops
             new SequencedSize(100,180,50,0,50,0, 1048576),
              // Sequenced Grain. All Local ops
             new SequencedSize(100,180,0,50,0,50, 1048576),
              // Sequenced Grain. 50/50 Global/Local
             new SequencedSize(100,180,25,25,25,25, 1048576),
             // Sequenced Grain Local Reads, Global Writes
             new SequencedSize(100,180,0,50,50,0, 1048576),
             // Sequenced Grain Global Reads, Local Writes
             new SequencedSize(100,180,50,0,0,50, 1048576),
             // Sequenced Grain 50/50 Local/Global Reads, Local Writes
             new SequencedSize(100,180,25,25,0,50, 1048576),
              // Sequenced Grain 50/50 Local/Global Reads, Global Writes
             new SequencedSize(100,180,25,25,50,0, 1048576),
             // Sequenced Grain Global Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,50,0,25,25, 1048576),
             // Sequenced Grain Local Reads, 50/50 Local/Global Writes
             new SequencedSize(100,180,0,50,25,25, 1048576),



           
        };

        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {

            if (verb == "WS" && string.Join("/", urlpath) == "size")
            {
                throw new NotImplementedException();
            }

            if (verb == "GET" && string.Join("/", urlpath) == "size")
            {

                if (int.Parse(arguments["rep"]) == 0)
                {
                    Console.Write("{0}", arguments);
                    SizeRequestT requestType = (SizeRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestSize request = null;
                    if (requestType == SizeRequestT.READ_SYNC)
                    {
                        // READ type
                        request = new HttpRequestSize(numReq);
                    }
                    else if (requestType == SizeRequestT.WRITE_SYNC)
                    {
                        // WRITE type
                        Util.Assert(false, "Should be of POST type");
                    }

                    return request;
                }

                else
                {
                    Console.Write("{0}", arguments);
                    SizeRequestT requestType = (SizeRequestT)int.Parse(arguments["reqtype"]);
                    int numReq = int.Parse(arguments["numreq"]);


                    HttpRequestSequencedSize request = null;
                    if (requestType == SizeRequestT.READ_SYNC)
                    {
                        // ReadCurrent type
                        request = new HttpRequestSequencedSize(numReq, false);
                    }
                    else if (requestType == SizeRequestT.READ_ASYNC)
                    {

                        // ReadLater type
                        request = new HttpRequestSequencedSize(numReq, true);

                    }

                    return request;
                }
            }
            else if (verb == "POST" && string.Join("/", urlpath) == "size")
            {
                Console.Write("{0}", arguments);
                SizeRequestT requestType = (SizeRequestT)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);


                if (int.Parse(arguments["rep"]) == 0)
                {

                    HttpRequestSize request = null;
                    if (requestType == SizeRequestT.WRITE_SYNC)
                    {
                        request = new HttpRequestSize(numReq, Encoding.ASCII.GetBytes(body));
                    }
                    else
                    {
                        Util.Assert(false, "Incorrect message type");
                    }

                    return request;
                }
                else
                {
                    HttpRequestSequencedSize request = null;

                    if (requestType == SizeRequestT.WRITE_SYNC)
                    {
                        // Write Now Type
                        request = new HttpRequestSequencedSize(numReq, Encoding.ASCII.GetBytes(body), false);
                    }
                    else if (requestType == SizeRequestT.WRITE_ASYNC)
                    {
                        // Write Later Type
                        request = new HttpRequestSequencedSize(numReq, Encoding.ASCII.GetBytes(body), true);
                    }
                    return request;
                }
            }

            return null; // URL not recognized
        }

    }


}
