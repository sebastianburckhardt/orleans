
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
            // assign servers to robots round-robin
            while (numrobots > 0)
                foreach (var server in servers)
                {
                    if (numrobots-- == 0)
                        break;
                    robotclients.Add(new Benchmarks.Client(server));
                }

            this.benchmark = benchmark;
            this.scenario = scenario;
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
        IBenchmark benchmark;
        IScenario scenario;

        public int NumRobots
        {
            get { return robotclients.Count(); }
        }

        public Task<string> RunRobot(int robotnumber, string parameters)
        {
            return scenario.RobotScript(robotclients[robotnumber], robotnumber, parameters);
        }


    }
}
