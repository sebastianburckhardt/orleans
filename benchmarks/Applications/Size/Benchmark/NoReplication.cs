using Common;
using Size.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


#pragma warning disable 1998

namespace Size.Benchmark
{

    public class NoReplicationSize : IScenario
    {

        // scenario parameters
        // read operations = get top 10
        // write operations = post 
        public NoReplicationSize(int pNumRobots, int pNumReqs, int pPercentRead, int pPayloadSize)
        {
            this.numRobots = pNumRobots;
            this.numReqs = pNumReqs;
            this.percentRead = pPercentRead;
            this.percentWrite = 100 - percentRead;
            this.payloadSize = pPayloadSize;
        }

        private int numRobots;
        private int numReqs;
        private int percentRead;
        private int percentWrite;
        private int payloadSize;

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

        public string Name { get { return string.Format("norep-robots{0}xnr{1}xsreads{2}xsize{3}", numRobots, numReqs, percentRead, payloadSize); } }

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
            int nextRandom;
            OperationType nextOp;
            byte[] nextWrite;
            Boolean executed;

            /* Debug */
            int totReads;
            int totWrites;

            rnd = new Random();
            reads = percentRead;
            writes = percentWrite;
            executed = false;
            nextOp = OperationType.READ_SYNC;
            nextWrite = new byte[payloadSize];

            /* Debug */
            totReads = 0;
            totWrites = 0;

            rnd.NextBytes(nextWrite);


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
                        await context.ServiceRequest(new HttpRequestSize(numReqs * robotnumber + i));
                        totReads++;
                        break;
                    case OperationType.WRITE_SYNC:
                          rnd.NextBytes(nextWrite);
                        await context.ServiceRequest(new HttpRequestSize(numReqs * robotnumber + i, nextWrite));
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


    public class HttpRequestSize : IHttpRequest
    {

          /// <summary>
        /// Constructor for GetTop10 calls
        /// </summary>
        /// <param name="pNumReq"></param>
        public HttpRequestSize(int pNumReq)
        {
            this.requestType = SizeRequestT.READ_SYNC;
            this.numReq = pNumReq;
        }

        /// <summary>
        /// Constructor for POST calls.
        /// </summary>
        /// <param name="pScore"></param>
        /// <param name="pNumReq"></param>
        public HttpRequestSize(int pNumReq,byte[] pPayload)
        {
            this.requestType = SizeRequestT.WRITE_SYNC;
            this.payload = pPayload;
            this.numReq = pNumReq;
            this.payload = pPayload;
        }

        // Request number
        private int numReq;
        // Request type, get or post
        private SizeRequestT requestType;
        // Score to post if requestType = post
        private byte[] payload ;


        public string Signature
        {
            get
            {
                if (requestType == SizeRequestT.READ_SYNC)
                {
                    return "GET size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=0";
                }
                else
                {
                    Util.Assert(payload != null, "Cannot have WRITE type and null payload");
                    return "POST size?reqtype=" + Convert.ToInt32(requestType) + "&" + "numreq=" + numReq + "&rep=0";
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

            var grain = SizeGrainFactory.GetGrain(0);

            byte[] readData;

           if (requestType == SizeRequestT.READ_SYNC)
            {
                Console.Write("READ \n");
                readData = grain.Read("Hello").Result;
                if (readData == null) return "";
                else return Encoding.ASCII.GetString(readData);
            }
            else
            {
                Util.Assert(requestType == SizeRequestT.WRITE_SYNC);
                await grain.Write(payload);
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


