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

                }
            }

            return null; // URL not recognized
        }

    }


}
