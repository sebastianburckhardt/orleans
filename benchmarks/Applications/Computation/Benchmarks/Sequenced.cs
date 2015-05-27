using Common;
using Computation.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


#pragma warning disable 1998

namespace Computation.Benchmark
{

    public class SequencedComputation : IScenario
    {

        // scenario parameters
        // sync read operations = get exact top 10
        // async read operations = get approx top 10
        // sync write operations = post now
        // async write operations = postlater
        public SequencedComputation(int pNumRobots, int pNumReqs, int pPercentSyncReads, int pPercentAsyncReads, int pPercentSyncWrites, int pPercentAsyncWrites, int pTimeUpdate)
        {
            this.numRobots = pNumRobots;
            this.numReqs = pNumReqs;
            this.percentSyncRead = pPercentSyncReads;
            this.percentAsyncRead = pPercentAsyncReads;
            this.percentSyncWrite = pPercentSyncWrites;
            this.percentAsyncWrite = pPercentAsyncWrites;
            this.timeUpdate = pTimeUpdate;
        }

        private int numRobots;
        private int numReqs;

        private int percentSyncRead;
        private int percentAsyncRead;
        private int percentSyncWrite;
        private int percentAsyncWrite;
        private int timeUpdate;

        enum OperationType
        {
            READ_SYNC,
            READ_ASYNC,
            WRITE_SYNC,
            WRITE_ASYNC
        };

        public String RobotServiceEndpoint(int workernumber)
        {
            throw new NotImplementedException();
        }

        public string Name { get { return string.Format("rep-robots{0}xnr{1}xsreads{2}xasreads{3}xswrites{4}xaswrites{5}xsize{6}", numRobots, numReqs, percentSyncRead,percentAsyncRead, percentSyncWrite,percentAsyncWrite,timeUpdate); } }

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
                    robotrequests[i] = context.RunRobot(i, numReqs.ToString());

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

            string[] param = parameters.Split('-');

            Util.Assert(percentSyncRead + percentAsyncRead + percentSyncWrite + percentAsyncWrite == 100,
                "Incorrect percentage breakdown " + percentSyncRead + " " + percentAsyncRead + " " +
                percentSyncWrite + " " + percentAsyncWrite);

            int syncReads;
            int asyncReads;
            int syncWrites;
            int asyncWrites;

            Random rnd;

            int nextRandom;
            OperationType nextOp;
            byte[] nextWrite;
            Boolean executed;

            /* Debug */
            int totSyncReads;
            int totAsyncReads;
            int totSyncWrites;
            int totAsyncWrites;


            rnd = new Random();
            nextWrite = new byte[100];
            syncReads = percentSyncRead;
            asyncReads = percentAsyncRead;
            syncWrites = percentSyncWrite;
            asyncWrites = percentAsyncWrite;
            executed = false;
            nextOp = OperationType.READ_SYNC;

            /* Debug */
            totSyncReads = 0;
            totAsyncReads = 0;
            totSyncWrites = 0;
            totAsyncWrites = 0;


            rnd.NextBytes(nextWrite);


            //TODO: refactor
       
            for (int i = 0; i < numReqs; i++)
            {
                if (asyncReads == 0 && asyncWrites == 0
                    && syncReads == 0 && syncWrites == 0)
                {
                    syncReads = percentSyncRead;
                    asyncReads = percentAsyncRead;
                    syncWrites = percentSyncWrite;
                    asyncWrites = percentAsyncWrite;
                }


                nextRandom = rnd.Next(0, 3);
                if (nextRandom == 0)
                {
                    if (syncReads > 0)
                    {
                        nextOp = OperationType.READ_SYNC;
                        syncReads--;
                        executed = true;
                    }
                    else if (asyncReads > 0)
                    {
                        nextOp = OperationType.READ_ASYNC;
                        asyncReads--;
                        executed = true;
                    }
                    else if (syncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_SYNC;
                        syncWrites--;
                        executed = true;
                    }
                    else if (asyncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_ASYNC;
                        asyncWrites--;
                        executed = true;
                    }
                }
                else if (nextRandom == 1)
                {

                    if (asyncReads > 0)
                    {
                        nextOp = OperationType.READ_ASYNC;
                        asyncReads--;
                        executed = true;
                    }
                    else if (syncReads > 0)
                    {
                        nextOp = OperationType.READ_SYNC;
                        syncReads--;
                        executed = true;
                    }
                    else if (syncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_SYNC;
                        syncWrites--;
                        executed = true;
                    }
                    else if (asyncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_ASYNC;
                        asyncWrites--;
                        executed = true;
                    }
                }
                else if (nextRandom == 2)
                {
                    if (syncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_SYNC;
                        syncWrites--;
                        executed = true;
                    }
                    else if (asyncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_ASYNC;
                        asyncWrites--;
                        executed = true;
                    }
                    else if (asyncReads > 0)
                    {
                        nextOp = OperationType.READ_ASYNC;
                        asyncReads--;
                        executed = true;
                    }
                    else if (syncReads > 0)
                    {
                        nextOp = OperationType.READ_SYNC;
                        syncReads--;
                        executed = true;
                    }

                }

                else if (nextRandom == 3)
                {
                    if (asyncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_ASYNC;
                        asyncWrites--;
                        executed = true;
                    }
                    else if (syncWrites > 0)
                    {
                        nextOp = OperationType.WRITE_SYNC;
                        syncWrites--;
                        executed = true;
                    }

                    else if (asyncReads > 0)
                    {
                        nextOp = OperationType.READ_ASYNC;
                        asyncReads--;
                        executed = true;
                    }
                    else if (syncReads > 0)
                    {
                        nextOp = OperationType.READ_SYNC;
                        syncReads--;
                        executed = true;
                    }

                }

                Util.Assert(executed == true, "Incorrect If logic \n");

                switch (nextOp)
                {
                    case OperationType.READ_SYNC:
                        await context.ServiceRequest(new HttpRequestSequencedSize(numReqs * robotnumber + i, false));
                        totSyncReads++;
                        break;
                    case OperationType.WRITE_SYNC:
                        rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestSequencedSize(numReqs * robotnumber + i, nextWrite, timeUpdate, false));
                        totSyncWrites++;
                        break;
                    case OperationType.READ_ASYNC:
                        await context.ServiceRequest(new HttpRequestSequencedSize(numReqs * robotnumber + i, true));
                        totAsyncReads++;
                        break;
                    case OperationType.WRITE_ASYNC:
                        rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestSequencedSize(numReqs * robotnumber + i, nextWrite, timeUpdate, true));
                        totAsyncWrites++;
                        break;
                } // end switch
                executed = false;

            } // end for loop

            Util.Assert(totAsyncReads == (percentAsyncRead * numReqs / 100), "Incorrect Number Async Reads " + totAsyncReads);
            Util.Assert(totAsyncWrites == (percentAsyncWrite * numReqs / 100), "Incorrect Number Sync Writes " + totAsyncWrites);
            Util.Assert(totSyncReads == (percentSyncRead * numReqs / 100), "Incorrect Number Sync Reads " + totSyncReads);
            Util.Assert(totSyncWrites == (percentSyncWrite * numReqs / 100), "Incorrect Number Sync Writes " + totSyncWrites);


            Console.Write("Executed {0} sync reads, {1} sync writes, {2} async reads, {3} async writes \n", totSyncReads, totSyncWrites, totAsyncReads, totAsyncWrites);
            return parameters;
        }


   

    }


    public class HttpRequestSequencedSize : IHttpRequest
    {

          /// <summary>
        /// Constructor for READ calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestSequencedSize(int pNumReq, bool async)
        {
            if (async)
            {
                this.requestType = SizeRequestT.READ_ASYNC;
            }
            else
            {
                this.requestType = SizeRequestT.READ_SYNC;
            }
            this.numReq = pNumReq;
        }

        public HttpRequestSequencedSize(int pNumReq, byte[] pPayload, int pTime, bool async)
        {
            if (async)
            {
                this.requestType = SizeRequestT.WRITE_ASYNC;
            }
            else
            {
                this.requestType = SizeRequestT.WRITE_SYNC;
            }
            this.numReq = pNumReq;
            this.payload = pPayload;
            this.timeUpdate = pTime;
                
        }

        

        // Request number
        private int numReq;
        // Request type, get or post
        private SizeRequestT requestType;
        // Dummy Payload
        private byte[] payload;
        // Time to spin
        private int timeUpdate;


        public string Signature
        {
            get
            {
                if (requestType == SizeRequestT.READ_SYNC || requestType == SizeRequestT.READ_ASYNC)
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
            get { if (payload == null) return null;
                else return Encoding.ASCII.GetString(payload); }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            Console.Write("ProcessRequestOnServer {0}  {1} ", numReq, requestType);

            var grain = SequencedComputationGrainFactory.GetGrain(0);
            Byte[] readData;


           if (requestType == SizeRequestT.READ_SYNC)
            {
                Console.Write("Read Current \n");
                readData = grain.ReadCurrent("hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
            }
           else if (requestType == SizeRequestT.READ_ASYNC)
           {
                Console.Write("Read Approx \n");
                readData = grain.ReadApprox("hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
           }
           else if (requestType == SizeRequestT.WRITE_SYNC)  {
                Console.Write("Write Now \n");
                await grain.WriteNow(timeUpdate);
                return "ok";
            }   else { 
               // POST_ASYNC
                Console.Write("Write Later \n ");
                await grain.WriteLater(timeUpdate);
                return "ok";
           }
        }

        public Task<string> ProcessResponseOnClient(string response)
        {
            Console.Write("{0} Req # {1} \n ", response, numReq );
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


