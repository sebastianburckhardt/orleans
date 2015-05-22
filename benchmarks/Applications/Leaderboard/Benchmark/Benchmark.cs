using Common;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

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
            new NoReplicationLeaderboard(1,1,100),

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
            if (verb == "GET" && string.Join("/", urlpath) == "leaderboard")
                return new GetRequest(int.Parse(arguments["nr"]));

            if (verb == "WS" && string.Join("/", urlpath) == "leaderboard")
                return new SocketRequest(int.Parse(arguments["numreqs"]));

            return null; // URL not recognized
        }

    }
}
