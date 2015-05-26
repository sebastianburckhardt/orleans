using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common;
using Leaderboard.Interfaces; 

#pragma warning disable 1998

namespace Leaderboard.Benchmark
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "leaderboard"; } }

        // list of scenarios for this benchmark
        public IEnumerable<IScenario> Scenarios { get { return scenarios; } }

        private IScenario[] scenarios = new IScenario[] 
        {
            new NoReplicationLeaderboard(1,1000,100),
            new NoReplicationLeaderboard(1,1000,0),
            new NoReplicationLeaderboard(1,1000,50),
            new SequencedLeaderboard(1,1000,100,0,0,0),
            new SequencedLeaderboard(1,1000,0,100,0,0),
            new SequencedLeaderboard(1,1000,0,0,100,0),
            new SequencedLeaderboard(1,1000,0,0,0,100),
            new SequencedLeaderboard(1,1000,50,50,0,0),
            new SequencedLeaderboard(1,1000,0,0,50,50),
            new SequencedLeaderboard(1,1000,25,25,25,25),


            // todo

            // 1) write-only
            // no replication
            // updating global state only
            // updating local state only
            // 25-75 local/global
            // 50-50 global/local
            // 75-25 local/global

            // 2) read-only 
            // no replication
            // reading local only
            // reading global only
            // 25-75 local/global
            // 50-50 global/local
            // 75-25 local/global

            // 3) mixed
        };

        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {

            if (verb == "WS" && string.Join("/", urlpath) == "leaderboard")
            {
                throw new NotImplementedException();
                /*LeaderboardRequestT requestType = (LeaderboardRequestT) int.Parse(arguments["reqtype"]) ;
                int numReq =  int.Parse(arguments["numreq"]);
                SocketRequest request=null;
                if (requestType == LeaderboardRequestT.GET) { 
                    // GET type
                    request = new SocketRequest(numReq);
                } else {
                    Util.Assert(requestType == LeaderboardRequestT.POST);
                    //TODO unnecessary conversion to "SCORE" type, keep as string?
                    request = new SocketRequest(numReq, Score.fromString(arguments["score"]));
                }
                return request; */
            }

            if (verb == "GET" && string.Join("/", urlpath) == "leaderboard")
            {
                Console.Write("{0}", arguments);
                LeaderboardRequestT requestType = (LeaderboardRequestT)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);


                HttpRequestLeaderboard request = null;
                if (requestType == LeaderboardRequestT.GET_SYNC)
                {
                    // GetTop10 type
                    request = new HttpRequestLeaderboard(numReq);
                }
                else if (requestType == LeaderboardRequestT.POST_SYNC)
                {
                    // New score type
                    Util.Assert(requestType == LeaderboardRequestT.POST_SYNC);
                    request = new HttpRequestLeaderboard(numReq, Score.fromString(arguments["score"]));
                }
                
                return request;
            }

            if (verb == "GET" && string.Join("/", urlpath) == "seqleaderboard")
            {
                Console.Write("{0}", arguments);
                LeaderboardRequestT requestType = (LeaderboardRequestT)int.Parse(arguments["reqtype"]);
                int numReq = int.Parse(arguments["numreq"]);


                HttpRequestSequencedLeaderboard request = null;
                if (requestType == LeaderboardRequestT.GET_SYNC)
                {
                    // GetCurrentTop10 type
                    request = new HttpRequestSequencedLeaderboard(numReq,false);
                }
                else if (requestType == LeaderboardRequestT.GET_ASYNC)
                {

                        // GetApproxTop10 type
                        request = new HttpRequestSequencedLeaderboard(numReq, true);
   
                }
                else if (requestType == LeaderboardRequestT.POST_SYNC)
                {
                    // Post Now Type
                    request = new HttpRequestSequencedLeaderboard(numReq, Score.fromString(arguments["score"]), false);
                }
                else if (requestType == LeaderboardRequestT.POST_ASYNC)
                {
                    // Post Later Type
                    request = new HttpRequestSequencedLeaderboard(numReq, Score.fromString(arguments["score"]), true);
                }
               
                return request;
            }


            return null; // URL not recognized
        }

    }


}
