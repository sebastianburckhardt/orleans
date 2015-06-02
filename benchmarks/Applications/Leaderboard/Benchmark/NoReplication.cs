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
        public NoReplicationLeaderboard(int pNumRobots, int pRunTime, int pPercentRead)
        {
            this.numRobots = pNumRobots;
            this.runTime = pRunTime;
            this.percentRead = pPercentRead;
            this.percentWrite = 100 - percentRead;
        }

        private int numRobots;
        private int runTime;
        private int percentRead;
        private int percentWrite;
        private Random rnd;

        enum OperationType
        {
            READ_SYNC,
            READ_ASYNC,
            WRITE_SYNC,
            WRITE_ASYNC
        };

        public string RobotServiceEndpoint(int workernumber)
        {

            return Endpoints.GetDefaultService();
        }

        public string Name { get { return string.Format("norep-robots{0}xnr{1}xsreads{2}", numRobots, runTime, percentRead); } }

        public int NumRobots { get { return numRobots; } }

        public int PercentRead { get { return percentRead; } }

        public int PercentWrite { get { return percentWrite; } }
        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numRobots];

            // start each robot
            for (int i = 0; i < numRobots; i++)
                robotrequests[i] = context.RunRobot(i, "");

            // wait for all robots
            await Task.WhenAll(robotrequests);

            int totalOps = 0;
            double throughput = 0.0;
            // check robot responses
            for (int i = 0; i < numRobots; i++)
            {
                string response = robotrequests[i].Result;
                string[] res = response.Split('-');
                totalOps += int.Parse(res[0]);
            }
            throughput = totalOps / runTime;
            return throughput.ToString();
        }

        private OperationType generateOperationType()
        {
            OperationType retType = OperationType.READ_ASYNC;
            int nextInt;

            nextInt = rnd.Next(1, 100);

            if (nextInt <= percentRead)
            {
                retType = OperationType.READ_SYNC;
            }
            else 
            {
                retType = OperationType.WRITE_SYNC;
            }
            return retType;
        }


        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {

            int reads;
            int writes;
            Random rnd;
            string[] names;
            int nameLength;
            OperationType nextOp;
            Score nextScore;

            /* Debug */
            int totReads;
            int totWrites;
            int totOps;

            rnd = new Random();
            names = new string[] { "Jack", "John", "Jim", "Ted", "Tid", "Tad" };
            nameLength = names.Length;
            reads = percentRead;
            writes = percentWrite;
            nextOp = OperationType.READ_SYNC;

            /* Debug */
            totReads = 0;
            totWrites = 0;
            totOps = 0;

            var begin = DateTime.Now;
            var end = DateTime.Now;


            //TODO: refactor
            while (true)
            {

                end = DateTime.Now;
                if ((end - begin).TotalSeconds > runTime) break;
                nextOp = generateOperationType();
                switch (nextOp)
                {
                    case OperationType.READ_SYNC:
                        string listSize = await context.ServiceRequest(new HttpRequestLeaderboard(totOps * robotnumber));
                        totReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_SYNC:
                        nextScore = new Score
                      {
                          Name = names[rnd.Next(0, nameLength - 1)],
                          Points = totOps * robotnumber
                      };
                        await context.ServiceRequest(new HttpRequestLeaderboard(totOps * robotnumber, nextScore));
                        totWrites++;
                        totOps++;
                        break;
                    case OperationType.READ_ASYNC:
                        throw new NotImplementedException();
                    case OperationType.WRITE_ASYNC:
                        throw new NotImplementedException();
                } // end switch

            }
                Util.Assert(totReads == (percentRead * totOps / 100), "Incorrect Number Reads " + totReads);
                Util.Assert(totWrites == (percentWrite * totOps / 100), "Incorrect Number Writes " + totWrites);

                Console.Write("Executed {0} reads, {1} writes \n", totReads, totWrites);
                return totOps.ToString() + "-" + begin + "-" + end;
        }
    }


    public class HttpRequestLeaderboard : IHttpRequest
    {

        /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestLeaderboard(int pNumReq)
        {
            this.requestType = LeaderboardRequestT.GET_SYNC;
            this.numReq = pNumReq;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequestLeaderboard(int pNumReq, Score pScore)
        {
            this.requestType = LeaderboardRequestT.POST_SYNC;
            this.score = pScore;
            this.numReq = pNumReq;
        }

        // Request number
        private int numReq;
        // Request type, get or post
        private LeaderboardRequestT requestType;
        // Score to post if requestType = post
        private Score score;


        public string Signature
        {
            get
            {
                if (requestType == LeaderboardRequestT.GET_SYNC)
                {
                    return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=0";
                }
                else
                {
                    return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&score=" + score.ToString() + "&rep=0";
                }
            }
        }


        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {

            var leaderboard = LeaderboardGrainFactory.GetGrain(0);
            string posts;
            Score[] scores;

            if (requestType == LeaderboardRequestT.GET_SYNC)
            {
                Console.Write("Get \n");
                scores = leaderboard.GetTopTen("hello").Result;
                posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                Console.Write("{0}\n", posts);
                return posts;
            }
            else
            {
                Util.Assert(requestType == LeaderboardRequestT.POST_SYNC);
                Console.Write("Post{0} \n ", score.ToString());
                await leaderboard.Post(score);
                return "ok";
            }


        }

        public Task<string> ProcessResponseOnClient(string response)
        {
            return Task.FromResult(response);
        }


        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }

    }


    /*

/// <summary>
/// Socket Request class for the Leaderboard benchmark
/// Socket Requests can be of 2 types:
/// 1) GetTop10 posts
/// 2) Posts
/// 
/// </summary>
    public class SocketRequest : ISocketRequest
    {
        /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public SocketRequest(int pNumReq)
        {
            this.requestType = LeaderboardRequestT.GET;
            this.numReq = pNumReq;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public SocketRequest(int pNumReq,Score pScore)
        {
            this.requestType = LeaderboardRequestT.POST;
            this.score = pScore;
            this.numReq = pNumReq;
        }

        // Request number
        private int numReq;
        // Request type, get or post
        private LeaderboardRequestT requestType;
        // Score to post if requestType = post
        private Score score;
   

        // server/client state
        private int count;

        public string Signature
        {
            get {
                if (requestType == LeaderboardRequestT.GET) {
                    return "WS leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq="+ numReq;
                }
                else
                {
                    return "WS leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&score=" + score.ToString();
                }
            }
        }

        public async Task ProcessConnectionOnServer(ISocket socket)
        {
            Util.Assert(count == 0);
        }

        public async Task ProcessMessageOnServer(ISocket socket, string message)
        {
            Console.Write("Message is {0} ", message);

            //      var leaderboard = LeaderBoardGrainFactory.GetGrain(0);
            // Score[] scores = await leaderboard.GetTopTen();
            // PrintPosts(scores);

            // Score s = new Score() {Name = "John" , Points = robotnumber};
            // await leaderboard.Post(s);


            //s = new Score() { Name = "Jack", Points = robotnumber };
            // await leaderboard.Post(s);

            

            // THIS IS WHERE STUFF HAPPENS
            count++;
            await socket.Send(message);
        }

        public async Task ProcessCloseOnServer(ISocket socket, string message)
        {
            Console.Write("{0} {1} \n ", count, numReq);
            Util.Assert(count == numReq);
            Util.Assert(message == "completed");
            await socket.Close("ack");
        }

        public async Task ProcessConnectionOnClient(ISocket socket)
        {
            Util.Assert(count == 0);
            await socket.Send("Leaderboards #" + count);
        }

        public async Task ProcessMessageOnClient(ISocket socket, string message)
        {
            Util.Assert(message == "Leaderboard #" + count);
            if (++count < numReq)
                await socket.Send("Leaderboard #" + count);
            else
                await socket.Close("completed");
        }

        public async Task ProcessCloseOnClient(ISocket socket, string message)
        {
            Util.Fail("connection closed by server");
        }
    } */



