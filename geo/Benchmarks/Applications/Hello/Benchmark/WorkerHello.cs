using GeoOrleans.Benchmarks.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Hello.Benchmark
{

    public class RobotHello : IScenario
    {

        // scenario parameters
        public RobotHello(int numrobots, int numreqs)
        {
            this.numrobots = numrobots;
            this.numreqs = numreqs;
        }
        private int numrobots;
        private int numreqs;

        public string Name { get { return string.Format("robots{0}x{1}", numrobots, numreqs); } }

        public int NumRobots { get { return numrobots; } }

        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numrobots];

            // repeat numreqs times
            for (int k = 0; k < numreqs; k++)
            {
                // start each robot
                for (int i = 0; i < numrobots; i++)
                    robotrequests[i] = context.RunRobot(i, k.ToString());

                // wait for all robots
                await Task.WhenAll(robotrequests);

                // check robot responses
                for (int i = 0; i < numrobots; i++)
                    GeoOrleans.Runtime.Common.Util.Assert(robotrequests[i].Result == k.ToString());
            }

            return "ok";
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {

            // trace
            await context.Trace("Robot " + robotnumber + " says hello");

            // echo
            return parameters;
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetDefaultService();
        }
    }
}
