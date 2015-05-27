using Common;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Benchmark
{
    public class Benchmark : IBenchmark
    {
        // name of this benchmark
        public string Name { get { return "hello"; } }

        // list of scenarios for this benchmark
        public IEnumerable<IScenario> Scenarios { get { return scenarios; } }

        private IScenario[] scenarios = new IScenario[] 
        {
            new RobotHello(1,1),
            new RobotHello(4,4),
            new HttpHello(1,1),
            new HttpHello(4,10),
            new WebsocketHello(1,1),
            new WebsocketHello(4,10),
            new OrleansHello(1,1),
            new OrleansHello(4,10),
        };

        // parsing of http requests
        public IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body)
        {
            if (verb == "GET" && string.Join("/", urlpath) == "hello")
            {
                if (arguments["command"] == "http")
                {
                    return new GetRequest(int.Parse(arguments["nr"]));
                }
                else if (arguments["command"] == "orleans") 
                {
                    return new OrleansHelloRequest(int.Parse(arguments["nr"]));
                }
            }

            if (verb == "WS" && string.Join("/", urlpath) == "hello")
                return new SocketRequest(int.Parse(arguments["numreqs"]));

            return null; // URL not recognized
        }

    }
}
