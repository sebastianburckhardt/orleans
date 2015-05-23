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

        public int PercentWrite { get { return percentwrite; } }
        // 
        public async Task<string> ConductorScript(IConductorContext context)
        {
            var robotrequests = new Task<string>[numrobots];

                // start each robot
                for (int i = 0; i < numrobots; i++)
                    robotrequests[i] = context.RunRobot(i, numreqs.ToString()+"-"+percentread);

                // wait for all robots
                await Task.WhenAll(robotrequests);

                // check robot responses
                for (int i = 0; i < numrobots; i++) {
                    Console.Write("Finished: {0} \n", robotrequests[i].Result );
  //                  Util.Assert(robotrequests[i].Result == "ok", "Incorrect reply");
                }
         
            return "ok";
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);
     
            int percentread = Convert.ToInt32(parameters.Split('-')[1]);
            int percentwrite = 100 - percentread;
            int reads;
            int writes;
            Random rnd;
            string[] names;
            int nameLength;
            int nextRandom;
            bool executed;
            Score nextScore;

            rnd = new Random();
            names = new string[] { "Jack","John","Jim","Ted","Tid","Tad"};
            nameLength = names.Length;
            reads = percentread;
            writes = percentwrite;
            executed = false;

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


            /* 
            //TODO: refactor
            for (int i = 0; i < numreqs; i++)
            {
                nextRandom = rnd.Next(0, 1);
                if (nextRandom == 0)
                {
                    if (reads > 0)
                    {
                        // do read
                        await context.ServiceRequest(new HttpRequest(numreqs * robotnumber + i));
                        reads--;
                        executed = true;
                    }
                    else
                    {
                        if (writes > 0)
                        {
                            //do write    
                            nextScore = new Score { Name = names[rnd.Next(0, nameLength - 1)], 
                                                    Points = numreqs * robotnumber + 1 
                                                  };
                            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber + i, nextScore));
                            writes--;
                            executed = true;
                        }
                    }
                }
                else
                {
                    if (writes > 0)
                    {
                        //do write 
                        nextScore = new Score
                        {
                            Name = names[rnd.Next(0, nameLength - 1)],
                            Points = numreqs * robotnumber + 1
                        };
                        await context.ServiceRequest(new HttpRequest(numreqs * robotnumber + i, nextScore));
                        writes--;
                        executed = true;
                    }
                    else
                    {
                        if (reads > 0)
                        {
                            // do read
                            await context.ServiceRequest(new HttpRequest(numreqs * robotnumber + i));
                            reads--;
                            executed = true;
                        }
                    }
                }

                if (!executed)
                {
                    // all reads and writes have been execute, reinitialise
                    reads = percentread;
                    writes = percentwrite;
                }
            }
                
            */
            return parameters;
        }


   

    }

    enum LeaderboardRequestT
    {
        GET,
        POST
    }


    public class HttpRequest : IHttpRequest
    {

          /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequest(int pNumReq)
        {
            this.requestType = LeaderboardRequestT.GET;
            this.numReq = pNumReq;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequest(int pNumReq,Score pScore)
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


        public string Signature
        {
            get
            {
                if (requestType == LeaderboardRequestT.GET)
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

            var leaderboard = LeaderBoardGrainFactory.GetGrain(0);
            string posts;
            Score[] scores;

           if (requestType == LeaderboardRequestT.GET)
            {
                Console.Write("Get \n");
                // string posts = leaderboard.SayHello("Hello").Result;
                scores =  leaderboard.GetTopTen("hello").Result;
                 posts = Leaderboard.Interfaces.Score.PrintScores(scores);
                Console.Write("{0}\n", posts);
                return posts;
            }
            else
            {
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
    }
}


