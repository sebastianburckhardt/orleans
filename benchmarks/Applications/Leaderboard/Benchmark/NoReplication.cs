using Common;
using Leaderboard.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#pragma warning disable 1998

namespace Leaderboard.Benchmark
{

    public class NoReplicationLeaderboard : IScenario
    {

        // scenario parameters
        // read operations = get top 10
        // write operations = post 
        public NoReplicationLeaderboard(int numrobots, int numreqs, int percentread)
        {
            this.numrobots = numrobots;
            this.numreqs = numreqs;
            this.percentread = percentread;
            this.percentwrite = 100 - percentread;
        }

        private int numrobots;
        private int numreqs;
        private int percentread;
        private int percentwrite;

        public string Name { get { return string.Format("robots{0}x{1}x{2}", numrobots, numreqs, percentread); } }

        public int NumRobots { get { return numrobots; } }

        public int PercentRead { get { return percentread; } }

        public int PercentWrite { get { return percentwrite;  } }
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
                    Util.Assert(robotrequests[i].Result == k.ToString());
            }

            return "ok";
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);
           // var leaderboard = LeaderBoardGrainFactory.GetGrain(0);
           // Score[] scores = await leaderboard.GetTopTen();
           // PrintPosts(scores);

           // Score s = new Score() {Name = "John" , Points = robotnumber};
           // await leaderboard.Post(s);


           //s = new Score() { Name = "Jack", Points = robotnumber };
           // await leaderboard.Post(s);


            return parameters;
        }


        void PrintPosts(Score[] s)
        {
            Console.WriteLine("Posts are: ");
            s.ToList().ForEach(i => Console.WriteLine(i.ToString()));
        }

    }


}
