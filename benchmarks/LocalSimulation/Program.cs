//*********************************************************//
//    Copyright (c) Microsoft. All rights reserved.
//    
//    Apache 2.0 License
//    
//    You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//    
//    Unless required by applicable law or agreed to in writing, software 
//    distributed under the License is distributed on an "AS IS" BASIS, 
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
//    implied. See the License for the specific language governing 
//    permissions and limitations under the License.
//
//*********************************************************

// Unset this to run external local silo
// http://dotnet.github.io/orleans/Step-by-step-Tutorials/Running-in-a-Stand-alone-Silo
#define USE_INPROC_SILO

using System;
using Orleans;
using Common;


namespace LocalSimulation
{
    /// <summary>
    /// Orleans test silo host
    /// </summary>
    public class Program
    {
        static void Main(string[] args)
        {

#if USE_INPROC_SILO
            // The Orleans silo environment is initialized in its own app domain in order to more
            // closely emulate the distributed situation, when the client and the server cannot
            // pass data via shared memory.
            AppDomain hostDomain = AppDomain.CreateDomain("OrleansHost", null, new AppDomainSetup
            {
                AppDomainInitializer = InitSilo,
                AppDomainInitializerArguments = args,
            });
#endif
            GrainClient.Initialize("DevTestClientConfiguration.xml");

            // initialize servers

            var numservers = 2;
            var deployment = "local-simulation-on-" + System.Environment.MachineName;
            var servers = new Benchmarks.Server[numservers];
            var urlpath = new string[numservers];

            for (int i = 0; i < numservers; i++)
            {
                string servername = "server" + i;
                servers[i] = new Benchmarks.Server(
                    "localhost on " + System.Environment.MachineName,
                   servername,
                    false,
                    false,
                    (s) => Console.WriteLine("[{0}] {1}", servername, s),
                    (s) => Console.WriteLine("[{0}] !!!!!!!!!!!!!!! {1}", servername, s));

                var endpoint = string.Format("http://+:843/simserver{0}/", i.ToString());
                Console.WriteLine("Launching server at " + endpoint);
                servers[i].Start(endpoint);
                urlpath[i] = string.Format("localhost:843/simserver{0}", i.ToString());
            }

            // start conductor console 

            var benchmarkconsole = new Benchmarks.Console(
                   (string s) => Console.WriteLine(s),
                   () => { Console.Write("> "); return Console.ReadLine(); }
                );

            benchmarkconsole.Welcome();

            while (true)
            {
                var scenarios = benchmarkconsole.SelectScenario();

                if (scenarios == null || !scenarios.HasValue)
                {
                    break;
                }

                foreach (var x in scenarios.Value.Value)
                {
                    var simulator = new Simulator(urlpath, x.NumRobots, scenarios.Value.Key, x);

                    var result = simulator.Run().Result;

                    Console.WriteLine(result);

                    Console.WriteLine();

                    Console.WriteLine(Util.PrintStats(simulator.Stats));

                }
            }
#if USE_INPROC_SILO
            hostDomain.DoCallBack(ShutdownSilo);
#endif
        }

#if USE_INPROC_SILO
        static void InitSilo(string[] args)
        {
            hostWrapper = new OrleansHostWrapper(args);

            if (!hostWrapper.Run())
            {
                Console.Error.WriteLine("Failed to initialize Orleans silo");
            }
        }

        static void ShutdownSilo()
        {
            if (hostWrapper != null)
            {
                hostWrapper.Dispose();
                GC.SuppressFinalize(hostWrapper);
            }
        }

        private static OrleansHostWrapper hostWrapper;
#endif
    }
}
