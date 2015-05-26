using Orleans;
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
        public NoReplicationLeaderboard(int pNumRobots, int pNumReqs, int pPercentRead)
        {
            this.numRobots = pNumRobots;
            this.numReqs = pNumReqs;
            this.percentRead = pPercentRead;
            this.percentWrite = 100 - percentRead;
        }

        private int numRobots;
        private int numReqs;
        private int percentRead;
        private int percentWrite;

        enum OperationType
        {
            READ_SYNC,
            READ_ASYNC,
            WRITE_SYNC,
            WRITE_ASYNC
        };

        public string Name { get { return string.Format("norep-robots{0}xnr{1}xsreads{2}", numRobots, numReqs, percentRead); } }

        public int NumRobots { get { return numRobots; } }

        public int PercentRead { get { return percentRead; } }

        public int PercentWrite { get { return percentWrite; } }
        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numRobots];

                // start each robot
                for (int i = 0; i < numRobots; i++)
                    robotrequests[i] = context.RunRobot(i, numReqs.ToString()+"-"+percentRead);

                // wait for all robots
                await Task.WhenAll(robotrequests);

                // check robot responses
                for (int i = 0; i < numRobots; i++) {
                    Console.Write("Finished: {0} \n", robotrequests[i].Result );
                }
         
            return "ok";
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);
     
            int reads;
            int writes;
            Random rnd;
            string[] names;
            int nameLength;
            int nextRandom;
            OperationType nextOp;
            Score nextScore;
            Boolean executed;

            /* Debug */
            int totReads;
            int totWrites;

            rnd = new Random();
            names = new string[] { "Jack","John","Jim","Ted","Tid","Tad"};
            nameLength = names.Length;
            reads = percentRead;
            writes = percentWrite;
            executed = false;
            nextOp = OperationType.READ_SYNC;

            /* Debug */
            totReads = 0;
            totWrites = 0; 
           
           /*
            nextScore = new Score
            {
                Name = names[rnd.Next(0, nameLength - 1)],
                Points = numreqs * robotnumber + 1
            };
            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber, nextScore));
            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber));

            nextScore = new Score
            {
                Name = names[rnd.Next(0, nameLength - 1)],
                Points = numreqs * robotnumber + 1
            };

            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber, nextScore));
            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber));
             */
           
            //TODO: refactor
            for (int i = 0; i < numReqs; i++)
            {
                while (!executed)
                {
                    nextRandom = rnd.Next(0, 1);
                    if (nextRandom == 0)
                    {
                        if (reads > 0)
                        {
                            nextOp = OperationType.READ_SYNC;
                            reads--;
                            executed = true;
                        }
                        else if (writes > 0)
                        {
                            nextOp = OperationType.WRITE_SYNC;
                            writes--;
                            executed = true;
                        }
                    }
                    else if (writes > 0)
                    {
                        nextOp = OperationType.WRITE_SYNC;
                        writes--;
                        executed = true;
                    }
                    else if (reads > 0)
                    {
                        nextOp = OperationType.READ_SYNC;
                        reads--;
                        executed = true;
                    }

                    if (!executed)
                    {
                        // all reads and writes have been executed, reinitialise
                        reads = percentRead;
                        writes = percentWrite;
                    }

                } // !executed 

                switch (nextOp) {
                    case OperationType.READ_SYNC:
                        await context.ServiceRequest(new HttpRequestLeaderboard(numReqs * robotnumber + i));
                        totReads++;
                        break;
                    case OperationType.WRITE_SYNC:
                          nextScore = new Score
                        {
                            Name = names[rnd.Next(0, nameLength - 1)],
                            Points = numReqs * robotnumber + i
                        };
                        await context.ServiceRequest(new HttpRequestLeaderboard(numReqs * robotnumber + i, nextScore));
                        totWrites++;
                        break;
                    case OperationType.READ_ASYNC:
                        throw new NotImplementedException();
                    case OperationType.WRITE_ASYNC:
                        throw new NotImplementedException();
                } // end switch
                executed = false;

            } // end for loop

            Util.Assert(totReads == (percentRead * numReqs / 100), "Incorrect Number Reads "+ totReads);
            Util.Assert(totWrites == (percentWrite * numReqs / 100), "Incorrect Number Writes " + totWrites);

            Console.Write("Executed {0} reads, {1} writes \n", totReads, totWrites);
            return parameters;
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
        public HttpRequestLeaderboard(int pNumReq,Score pScore)
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
                   return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq;
                }
                else
                {
                 return "GET leaderboard?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&score=" + score.ToString();
                }
            }
        }


        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            var leaderboard = LeaderboardGrainFactory.GetGrain(0);
            string posts;
            Score[] scores;

           if (requestType == LeaderboardRequestT.GET_SYNC)
            {
                Console.Write("Get \n");
                scores =  leaderboard.GetTopTen("hello").Result;
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

        public async Task ProcessResponseOnClient(string response)
        {
            Console.Write("{0} Req # {1} \n ", response, numReq );
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


