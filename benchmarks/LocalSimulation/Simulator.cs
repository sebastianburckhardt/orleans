
using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LocalSimulation
{
    class Simulator : IConductorContext
    {

        public Simulator(IEnumerable<string> servers, int numrobots, IBenchmark benchmark, IScenario scenario)
        {
            this.benchmark = benchmark;
            this.scenario = scenario;
            this.testname = string.Format("{0:o}.{1}.{2}", DateTime.UtcNow, benchmark.Name, scenario.Name);

            // assign servers to robots round-robin
            int robotnum = 0;
            while (robotnum < numrobots)
                foreach (var server in servers)
                {
                    robotclients.Add(new Benchmarks.Client(server, testname, robotnum++, Trace));
                    if (robotnum == numrobots)
                        break;
                }
        }

        public async Task<string> Run()
        {
            try
            {
                Console.WriteLine("----------------- START benchmark scenario {0}.{1} ---------------------------", benchmark.Name, scenario.Name);

                return await scenario.ConductorScript(this);
            }
            catch (Exception e)
            {
                return "ERROR: exception " + e.ToString();
            }
            finally
            {
                Console.WriteLine("----------------- END benchmark scenario {0}.{1} ---------------------------", benchmark.Name, scenario.Name);
            }
        }

        List<Benchmarks.Client> robotclients = new List<Benchmarks.Client>();
        public Dictionary<string, LatencyDistribution> Stats = new Dictionary<string, LatencyDistribution>();
        IBenchmark benchmark;
        IScenario scenario;
        string testname;

        public string TestName { get { return testname; } }

        public int NumRobots
        {
            get { return robotclients.Count(); }
        }

        public async Task<string> RunRobot(int robotnumber, string parameters)
        {
            // run the scenario
            string retval;
            try
            {
                retval = await scenario.RobotScript(robotclients[robotnumber], robotnumber, parameters);
            }
            catch (Exception e)
            {
                retval = e.ToString();
                robotclients[robotnumber].Trace(retval).Wait();
            }

            // collect stats from this robot
            foreach (var kvp in robotclients[robotnumber].Stats)
            {
                if (!Stats.ContainsKey(kvp.Key))
                    Stats.Add(kvp.Key, new LatencyDistribution());
                Stats[kvp.Key].MergeDistribution(kvp.Value);
            }

            return retval;
        }


 


        public async Task Trace(string info)
        {
            Console.WriteLine(info);
        }
    }
}
