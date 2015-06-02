using Common;
using Leaderboard.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;


#pragma warning disable 1998

namespace Leaderboard.Benchmark
{

    public class SequencedLeaderboard : IScenario
    {

        // scenario parameters
        // sync read operations = get exact top 10
        // async read operations = get approx top 10
        // sync write operations = post now
        // async write operations = postlater
        public SequencedLeaderboard(int pNumRobots, int pRunTime, int pPercentSyncReads, int pPercentAsyncReads, int pPercentSyncWrites, int pPercentAsyncWrites, int pDummy)
        {
            this.numRobots = pNumRobots;
            this.runTime = pRunTime;
            this.percentSyncRead = pPercentSyncReads;
            this.percentAsyncRead = pPercentAsyncReads;
            this.percentSyncWrite = pPercentSyncWrites;
            this.percentAsyncWrite = pPercentAsyncWrites;
            this.dummyGrain = pDummy;
        }

        private int numRobots;
        private int runTime;

        private int percentSyncRead;
        private int percentAsyncRead;
        private int percentSyncWrite;
        private int percentAsyncWrite;
        private int dummyGrain = 0;
        private Random rnd = new Random();

        enum OperationType
        {
            READ_SYNC,
            READ_ASYNC,
            WRITE_SYNC,
            WRITE_ASYNC
        };

        public String RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetDefaultService();

        }

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xsreads{2}xasreads{3}xswrites{4}xaswrites{5}xdummy{6}", numRobots, runTime, percentSyncRead, percentAsyncRead, percentSyncWrite, percentAsyncWrite, dummyGrain); } }

        public int NumRobots { get { return numRobots; } }

        public int PercentSyncRead { get { return percentSyncRead; } }

        public int PercentAsyncRead { get { return percentAsyncRead; } }

        public int PercentSyncWrite { get { return percentSyncWrite; } }
        public int PercentAsyncWrite { get { return percentAsyncWrite; } }

        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numRobots];

            // start each robot
            for (int i = 0; i < numRobots; i++)
                robotrequests[i] = context.RunRobot(i,"");

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

            if (nextInt <= percentSyncRead)
            {
                retType = OperationType.READ_SYNC;
            }
            else if (nextInt <= (percentSyncRead + percentAsyncRead))
            {
                retType = OperationType.READ_ASYNC;
            }
            else if (nextInt <= (percentSyncRead + percentAsyncRead + percentSyncWrite))
            {
                retType = OperationType.WRITE_SYNC;
            }
            else if (nextInt <= (percentSyncRead + percentAsyncRead + percentSyncWrite + percentAsyncWrite))
            {
                retType = OperationType.WRITE_ASYNC;
            }
            return retType;
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {

 

            Util.Assert(percentSyncRead + percentAsyncRead + percentSyncWrite + percentAsyncWrite == 100,
                "Incorrect percentage breakdown " + percentSyncRead + " " + percentAsyncRead + " " +
                percentSyncWrite + " " + percentAsyncWrite);

            int syncReads;
            int asyncReads;
            int syncWrites;
            int asyncWrites;

            Random rnd;
            string[] names;
            int nameLength;
            OperationType nextOp;
            Score nextScore;

            /* Debug */
            int totSyncReads;
            int totAsyncReads;
            int totSyncWrites;
            int totAsyncWrites;
            int totOps;
            
            

            rnd = new Random();
            names = new string[] { "Jack", "John", "Jim", "Ted", "Tid", "Tad" };
            nameLength = names.Length;
            syncReads = percentSyncRead;
            asyncReads = percentAsyncRead;
            syncWrites = percentSyncWrite;
            asyncWrites = percentAsyncWrite;
            nextOp = OperationType.READ_SYNC;

            /* Debug */
            totSyncReads = 0;
            totAsyncReads = 0;
            totSyncWrites = 0;
            totAsyncWrites = 0;
            totOps = 0;

         
            Stopwatch s = new Stopwatch();
            s.Start();
            while (true)
            {
                s.Stop();

                if (s.ElapsedMilliseconds > runTime * 1000) break;

                s.Start();

                nextOp = generateOperationType();
              

                switch (nextOp)
                {
                    case OperationType.READ_SYNC:
                        await context.ServiceRequest(new HttpRequestSequencedLeaderboard(runTime * robotnumber + totOps, false, dummyGrain));
                        totSyncReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_SYNC:
                        nextScore = new Score
                      {
                          Name = names[rnd.Next(0, nameLength - 1)],
                          Points = runTime * robotnumber + totOps
                      };
                        await context.ServiceRequest(new HttpRequestSequencedLeaderboard(runTime * robotnumber +  totOps, nextScore, false, dummyGrain));
                        totSyncWrites++;
                        totOps++;
                        break;
                    case OperationType.READ_ASYNC:
                        await context.ServiceRequest(new HttpRequestSequencedLeaderboard(runTime * robotnumber + totOps, true, dummyGrain));
                        totAsyncReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_ASYNC:
                        nextScore = new Score
                     {
                         Name = names[rnd.Next(0, nameLength - 1)],
                         Points = runTime * robotnumber + totOps
                     };
                        await context.ServiceRequest(new HttpRequestSequencedLeaderboard(runTime * robotnumber + totOps, nextScore, true, dummyGrain));
                        totAsyncWrites++;
                        totOps++;
                        break;
                } // end switch

            } // end for loop

            Util.Assert(totAsyncReads == (percentAsyncRead * totOps / 100), "Incorrect Number Async Reads " + totAsyncReads);
            Util.Assert(totAsyncWrites == (percentAsyncWrite * totOps / 100), "Incorrect Number Sync Writes " + totAsyncWrites);
            Util.Assert(totSyncReads == (percentSyncRead * totOps / 100), "Incorrect Number Sync Reads " + totSyncReads);
            Util.Assert(totSyncWrites == (percentSyncWrite * totOps / 100), "Incorrect Number Sync Writes " + totSyncWrites);


            Console.Write("Executed {0} sync reads, {1} sync writes, {2} async reads, {3} async writes \n", totSyncReads, totSyncWrites, totAsyncReads, totAsyncWrites);
            return totOps.ToString() + "-" + s.ElapsedMilliseconds;
        }




    }


    public class HttpRequestSequencedLeaderboard : IHttpRequest
    {

        /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedLeaderboard(int pNumReq, bool async)
        {
            if (async)
            {
                this.requestType = LeaderboardRequestT.GET_ASYNC;
            }
            else
            {
                this.requestType = LeaderboardRequestT.GET_SYNC;
            }
            this.numReq = pNumReq;
        }


        /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedLeaderboard(int pNumReq, bool async, int pDummyGrain)
        {
            if (async)
            {
                this.requestType = LeaderboardRequestT.GET_ASYNC;
            }
            else
            {
                this.requestType = LeaderboardRequestT.GET_SYNC;
            }
            this.numReq = pNumReq;
            this.dummyGrain = pDummyGrain;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedLeaderboard(int pNumReq, Score pScore, bool async)
        {
            if (async)
            {
                this.requestType = LeaderboardRequestT.POST_ASYNC;
            }
            else
            {
                this.requestType = LeaderboardRequestT.POST_SYNC;
            }
            this.score = pScore;
            this.numReq = pNumReq;
            this.dummyGrain = 0;
        }


        /// <summary>
        /// Dummy Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedLeaderboard(int pNumReq, Score pScore, bool async, int pDummyGrain)
        {
            if (async)
            {
                this.requestType = LeaderboardRequestT.POST_ASYNC;
            }
            else
            {
                this.requestType = LeaderboardRequestT.POST_SYNC;
            }
            this.score = pScore;
            this.numReq = pNumReq;
            this.dummyGrain = pDummyGrain;
        }

        // Request number
        private int numReq;
        // Request type, get or post
        private LeaderboardRequestT requestType;
        // Score to post if requestType = post
        private Score score;
        private int dummyGrain;

        public string Signature
        {
            get
            {
                if (requestType == LeaderboardRequestT.GET_SYNC || requestType == LeaderboardRequestT.GET_ASYNC)
                {
                    return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=1" + "&dummy=" + dummyGrain;
                }
                else
                {
                    return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&score=" + score.ToString() + "&rep=1" + "&dummy=" + dummyGrain;
                }
            }
        }


        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            string posts;
            Score[] scores;

            if (dummyGrain == 1)
            {
                // Use dummy grain;
                IDummySequencedLeaderboardGrain leaderboard = null;
                using (new TraceInterval("Leaderboard FE - dummy - getgrain", 0))
                {
                    leaderboard = DummySequencedLeaderboardGrainFactory.GetGrain(0);
                }
                if (requestType == LeaderboardRequestT.GET_SYNC)
                {
                    //              Console.Write("Get Cuurent \n");

                    using (new TraceInterval("Leaderboard FE - dummy - get - sync", 1))
                    {
                        scores = leaderboard.GetExactTopTen("hello").Result;
                        posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                    }
                    //            Console.Write("{0}\n", posts);
                    return posts;
                }
                else if (requestType == LeaderboardRequestT.GET_ASYNC)
                {
                    //            Console.Write("Get Approx \n");
                    using (new TraceInterval("Leaderboard FE - dummy - get - Async", 2))
                    {
                        scores = leaderboard.GetApproxTopTen("hello").Result;
                        posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                    }
                    //              Console.Write("{0}\n", posts);
                    return posts;
                }
                else if (requestType == LeaderboardRequestT.POST_SYNC)
                {
                    using (new TraceInterval("Leaderboard FE - dummy - post - sync", 3))
                    {
                        await leaderboard.PostNow(score);
                    }
                    return "ok";
                }
                else
                {
                    // POST_ASYNC
                    //            Console.Write("Post Later {0} \n ", score.ToString());
                    using (new TraceInterval("Leaderboard FE - dummy - post - async", 4))
                    {
                        await leaderboard.PostLater(score);
                    }
                    return "ok";
                }
            }
            else
            {
                ISequencedLeaderboardGrain leaderboard = null;
                using (new TraceInterval("Leaderboard FE - getgrain", 0))
                {
                    leaderboard = SequencedLeaderboardGrainFactory.GetGrain(0);
                }
                if (requestType == LeaderboardRequestT.GET_SYNC)
                {
                    //              Console.Write("Get Cuurent \n");
                    using (new TraceInterval("Leaderboard FE - get - sync", 1))
                    {
                        scores = leaderboard.GetExactTopTen("hello").Result;
                        posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                    }
                    //            Console.Write("{0}\n", posts);
                    return posts;
                }
                else if (requestType == LeaderboardRequestT.GET_ASYNC)
                {
                    //            Console.Write("Get Approx \n");
                    using (new TraceInterval("Leaderboard FE - get - Async", 2))
                    {
                        scores = leaderboard.GetApproxTopTen("hello").Result;
                        posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                    }
                    //              Console.Write("{0}\n", posts);
                    return posts;
                }
                else if (requestType == LeaderboardRequestT.POST_SYNC)
                {
                    using (new TraceInterval("Leaderboard FE - post - sync", 3))
                    {
                        await leaderboard.PostNow(score);
                    }
                    return "ok";
                }
                else
                {
                    // POST_ASYNC
                    //            Console.Write("Post Later {0} \n ", score.ToString());
                    using (new TraceInterval("Leaderboard FE - post - async", 4))
                    {
                        await leaderboard.PostLater(score);
                    }
                    return "ok";
                }
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
}


