using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Common; 

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

        public class GetRequest : IHttpRequest
        {
            public GetRequest(int nr)
            {
                this.nr = nr;
            }

            private int nr;

            public string Signature
            {
                get { return string.Format("GET hello?nr={0}", nr); }
            }

            public string Body
            {
                get { return null; }
            }

            public async Task<string> ProcessRequestOnServer()
            {
                return "Hello #" + nr;
            }

            public async Task ProcessResponseOnClient(string response)
            {
                Util.Assert(response == "Hello #" + nr, "incorrect response");
            }

            public async Task ProcessErrorResponseOnClient(int statuscode, string response)
            {
                Util.Fail("Unexpected error message");
            }
        }

        public class SocketRequest : ISocketRequest
        {
            public SocketRequest(int numreqs)
            {
                this.numreqs = numreqs;
            }

            private int numreqs;

            // server/client state
            private int count;

            public string Signature
            {
                get { return "WS hello?numreqs=" + numreqs; }
            }

            public async Task ProcessConnectionOnServer(ISocket socket)
            {
                Util.Assert(count == 0);
            }

            public async Task ProcessMessageOnServer(ISocket socket, string message)
            {
                Util.Assert(message == "Hello #" + count++, "incorrect message from client");
                await socket.Send(message);
            }

            public async Task ProcessCloseOnServer(ISocket socket, string message)
            {
                Util.Assert(count == numreqs);
                Util.Assert(message == "completed");
                await socket.Close("ack");
            }

            public async Task ProcessConnectionOnClient(ISocket socket)
            {
                Util.Assert(count == 0);
                await socket.Send("Hello #" + count);
            }

            public async Task ProcessMessageOnClient(ISocket socket, string message)
            {
                Util.Assert(message == "Hello #" + count);
                if (++count < numreqs)
                    await socket.Send("Hello #" + count);
                else
                    await socket.Close("completed");
            }

            public async Task ProcessCloseOnClient(ISocket socket, string message)
            {
                Util.Fail("connection closed by server");
            }
        }


    }


}
