using GeoOrleans.Runtime.Common;
using GeoOrleans.Benchmarks.Common;
using GeoOrleans.Benchmarks.Computation.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;


#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Computation.Benchmark
{

    public class SequencedComputation : IScenario
    {

        // scenario parameters
        // sync read operations = get exact top 10
        // async read operations = get approx top 10
        // sync write operations = post now
        // async write operations = postlater
        public SequencedComputation(int pNumRobots, int pRunTime, int pPercentSyncReads, int pPercentAsyncReads, int pPercentSyncWrites, int pPercentAsyncWrites, int pTimeUpdate)
        {
            this.numRobots = pNumRobots;
            this.runTime = pRunTime;
            this.percentSyncRead = pPercentSyncReads;
            this.percentAsyncRead = pPercentAsyncReads;
            this.percentSyncWrite = pPercentSyncWrites;
            this.percentAsyncWrite = pPercentAsyncWrites;
            this.timeUpdate = pTimeUpdate;
            this.rnd = new Random();
        }

        private int numRobots;
        private int runTime;

        private int percentSyncRead;
        private int percentAsyncRead;
        private int percentSyncWrite;
        private int percentAsyncWrite;
        private int timeUpdate;

        private Random rnd;

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

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xsreads{2}xasreads{3}xswrites{4}xaswrites{5}xcomp{6}", numRobots, runTime, percentSyncRead, percentAsyncRead, percentSyncWrite, percentAsyncWrite, timeUpdate); } }

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
            } else if  (nextInt <= (percentSyncRead + percentAsyncRead + percentSyncWrite + percentAsyncWrite)) { 
                retType =  OperationType.WRITE_ASYNC;
            }
            return retType;
        }


        // each robot simply echoes the parameters
        public async Task<string> RobotScript(IRobotContext context, int robotnumber, string parameters)
        {
            Console.Write("PARAMETERS {0} \n", parameters);

            string[] param = parameters.Split('-');

            GeoOrleans.Runtime.Common.Util.Assert(percentSyncRead + percentAsyncRead + percentSyncWrite + percentAsyncWrite == 100,
                "Incorrect percentage breakdown " + percentSyncRead + " " + percentAsyncRead + " " +
                percentSyncWrite + " " + percentAsyncWrite);

            int syncReads;
            int asyncReads;
            int syncWrites;
            int asyncWrites;

            Random rnd;

            OperationType nextOp;
            byte[] nextWrite;
            
            /* stats */
            int totSyncReads;
            int totAsyncReads;
            int totSyncWrites;
            int totAsyncWrites;
            int totOps;
            

            rnd = new Random();
            nextWrite = new byte[100];
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
                        await context.ServiceRequest(new HttpRequestSequencedComputation(totOps * robotnumber, false));
                        totSyncReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_SYNC:
                        rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestSequencedComputation(totOps * robotnumber + totOps, nextWrite, timeUpdate, false));
                        totSyncWrites++;
                        totOps++;
                        break;
                    case OperationType.READ_ASYNC:
                        await context.ServiceRequest(new HttpRequestSequencedComputation(totOps * robotnumber + totOps, true));
                        totAsyncReads++;
                        totOps++;
                        break;
                    case OperationType.WRITE_ASYNC:
                        rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestSequencedComputation(totOps * robotnumber + totOps, nextWrite, timeUpdate, true));
                        totAsyncWrites++;
                        totOps++;
                        break;
                } // end switch                

            } // end for loop



            Console.Write("Executed {0} sync reads, {1} sync writes, {2} async reads, {3} async writes Throughput {4}\n", totSyncReads, totSyncWrites, totAsyncReads, totAsyncWrites);
            return totOps.ToString() + "-" + s.ElapsedMilliseconds;
        }




    }


    public class HttpRequestSequencedComputation : IHttpRequest
    {

        /// <summary>
        /// Constructor for READ calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedComputation(int pNumReq, bool async)
        {
            if (async)
            {
                this.requestType = ComputationRequestT.READ_ASYNC;
            }
            else
            {
                this.requestType = ComputationRequestT.READ_SYNC;
            }
            this.numReq = pNumReq;
        }

        public HttpRequestSequencedComputation(int pNumReq, byte[] pPayload, int pTime, bool async)
        {
            if (async)
            {
                this.requestType = ComputationRequestT.WRITE_ASYNC;
            }
            else
            {
                this.requestType = ComputationRequestT.WRITE_SYNC;
            }
            this.numReq = pNumReq;
            this.payload = pPayload;
            this.timeUpdate = pTime;

        }



        // Request number
        private int numReq;
        // Request type, get or post
        private ComputationRequestT requestType;
        // Dummy Payload
        private byte[] payload;
        // Time to spin
        private int timeUpdate;


        public string Signature
        {
            get
            {
                if (requestType == ComputationRequestT.READ_SYNC || requestType == ComputationRequestT.READ_ASYNC)
                {
                    return "GET computation?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=1";
                }
                else
                {
                    return "POST computation?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=1" + "&time=" + timeUpdate;
                }
            }
        }


        public string Body
        {
            get
            {
                if (payload == null) return null;
                else return Encoding.ASCII.GetString(payload);
            }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            var grain = SequencedComputationGrainFactory.GetGrain(0);
            Byte[] readData;


            if (requestType == ComputationRequestT.READ_SYNC)
            {
                Console.Write("Read Current \n");
                readData = grain.ReadCurrent("hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
            }
            else if (requestType == ComputationRequestT.READ_ASYNC)
            {
                Console.Write("Read Approx \n");
                readData = grain.ReadApprox("hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
            }
            else if (requestType == ComputationRequestT.WRITE_SYNC)
            {
                Console.Write("Write Now \n");
                await grain.WriteNow(timeUpdate);
                return "ok";
            }
            else
            {
                // POST_ASYNC
                Console.Write("Write Later \n ");
                await grain.WriteLater(timeUpdate);
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
            GeoOrleans.Runtime.Common.Util.Fail("Unexpected error message");
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
            GeoOrleans.Runtime.Common.Util.Assert(count == 0);
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
            GeoOrleans.Runtime.Common.Util.Assert(count == numReq);
            GeoOrleans.Runtime.Common.Util.Assert(message == "completed");
            await socket.Close("ack");
        }

        public async Task ProcessConnectionOnClient(ISocket socket)
        {
            GeoOrleans.Runtime.Common.Util.Assert(count == 0);
            await socket.Send("Leaderboards #" + count);
        }

        public async Task ProcessMessageOnClient(ISocket socket, string message)
        {
            GeoOrleans.Runtime.Common.Util.Assert(message == "Leaderboard #" + count);
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


