using Common;
using Computation.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;


#pragma warning disable 1998

namespace Computation.Benchmark
{

    public class NoReplicationComputation : IScenario
    {

        // scenario parameters
        // read operations = get top 10
        // write operations = post 
        public NoReplicationComputation(int pNumRobots, int pRunTime, int pPercentRead, int pTimeUpdate)
        {
            this.numRobots = pNumRobots;
            this.runTime = pRunTime;
            this.percentRead = pPercentRead;
            this.percentWrite = 100 - percentRead;
            this.timeUpdate = pTimeUpdate;
        }

        private int numRobots;
        private int percentRead;
        private int percentWrite;
        private int timeUpdate;
        private int runTime;

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

        public string Name { get { return string.Format("norep-robots{0}xnr{1}xsreads{2}xcomp{3}", numRobots, runTime, percentRead, timeUpdate); } }

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
                string response = "";
                try
                {
                    response = robotrequests[i].Result;
                    string[] res = response.Split('-');
                    totalOps += int.Parse(res[0]);
                }
                catch (Exception e)
                {
                    throw new Exception("Robot failed to return totOps value " + response + " " + e.ToString());
                }
            }
            throughput = totalOps / runTime;
            return throughput.ToString();
        }

        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);

            int reads;
            int writes;
            Random rnd;
  
            OperationType nextOp;
            byte[] nextWrite;
            
            /* Stats */
            int totReads;
            int totWrites;
            int totOps;

            rnd = new Random();
            reads = percentRead;
            writes = percentWrite;
     
            nextOp = OperationType.READ_SYNC;
            nextWrite = new byte[100];

            /* Stats */
            totReads = 0;
            totWrites = 0;
            totOps = 0;

            rnd.NextBytes(nextWrite);


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
                        await context.ServiceRequest(new HttpRequestComputation(totOps * robotnumber));
                        totReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_SYNC:
                        rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestComputation(totOps * robotnumber, nextWrite, timeUpdate));
                        totWrites++;
                        totOps++;
                        break;
                    case OperationType.READ_ASYNC:
                        throw new NotImplementedException();
                    case OperationType.WRITE_ASYNC:
                        throw new NotImplementedException();
                } // end switch
   
            } // end for loop

            Util.Assert(totReads == (percentRead * totOps / 100), "Incorrect Number Reads " + totReads);
            Util.Assert(totWrites == (percentWrite * totOps / 100), "Incorrect Number Writes " + totWrites);

            Console.Write("Executed {0} reads, {1} writes \n", totReads, totWrites);

            return totOps.ToString();
        }

        private OperationType generateOperationType()
        {
            OperationType retType;
            int nextInt;

            nextInt = rnd.Next(1, 100);

            if (nextInt <= percentRead)
            {
                retType =OperationType.READ_SYNC;
            }
            else
            {
                retType = OperationType.WRITE_SYNC;
            }
            return retType;
        }


    }


    public class HttpRequestComputation : IHttpRequest
    {

        /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestComputation(int pNumReq)
        {
            this.requestType = ComputationRequestT.READ_SYNC;
            this.numReq = pNumReq;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequestComputation(int pNumReq, byte[] pPayload, int pTimeUpdate)
        {
            this.requestType = ComputationRequestT.WRITE_SYNC;
            this.payload = pPayload;
            this.numReq = pNumReq;
            this.payload = pPayload;
            this.timeUpdate = pTimeUpdate;
        }

        // Request number
        private int numReq;
        // Request type, get or post
        private ComputationRequestT requestType;
        // Score to post if requestType = post
        private byte[] payload;
        private int timeUpdate;


        public string Signature
        {
            get
            {
                if (requestType == ComputationRequestT.READ_SYNC)
                {
                    return "GET computation?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=0";
                }
                else
                {
                    Util.Assert(payload != null, "Cannot have WRITE type and null payload");
                    return "POST computation?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=0" + "&time=" + timeUpdate;
                }
            }
        }


        public string Body
        {
            get { if (payload == null) return null; else return Encoding.ASCII.GetString(payload); }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            var grain = ComputationGrainFactory.GetGrain(0);

            byte[] readData;

            if (requestType == ComputationRequestT.READ_SYNC)
            {
                Console.Write("READ \n");
                readData = grain.Read("Hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
            }
            else
            {
                Util.Assert(requestType == ComputationRequestT.WRITE_SYNC);
                await grain.Write(timeUpdate);
                return "ok";
            }


        }

        public Task<string> ProcessResponseOnClient(string response)
        {
            Console.Write("{0} Req # {1} \n ", response, numReq);
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


