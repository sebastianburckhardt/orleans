using Common;
using Hello.Interfaces;
using Orleans.Streams;
using ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Benchmark
{

    public class ReactiveOrleansHello : IScenario
    {
        public ReactiveOrleansHello(int numrobots, bool poll)
        {
            this.numrobots = numrobots;
            this.poll = poll;
        }
        private int numrobots;
        private bool poll;

        public string Name { get { return string.Format("reactive{0}x{1}", numrobots, poll ? "poll" : "stream"); } }

        public int NumRobots { get { return numrobots; } }

        public async Task<string> ConductorScript(IConductorContext context)
        {
            var workerrequests = new Task<string>[numrobots];
            for (int i = 0; i < numrobots; i++)
                workerrequests[i] = context.RunRobot(i, "");

            await Task.WhenAll(workerrequests);

            return string.Join(",", workerrequests.Select((t) => t.Result));
        }

        public async Task<string> RobotScript(IRobotContext context, int workernumber, string parameters)
        {
            Task<string>[] requests = new Task<string>[numrobots];

            var result = await context.ServiceConnection(new ReactiveOrleansSocketRequest(numrobots, context.RobotNumber, poll));

            return result;
        }


        public string RobotServiceEndpoint(int workernumber)
        {
            return Endpoints.GetService(workernumber);
        }
    }


    public class ReactiveOrleansSocketRequest : ISocketRequest 
    {
        public ReactiveOrleansSocketRequest(int numrobots, int robotnumber, bool poll)
        {
            this.numrobots = numrobots;
            this.robotnumber = robotnumber;
            this.poll = poll;
        }

        private int numrobots; 
        private int robotnumber;
        private bool poll;
 

        public string Signature
        {
            get { return "WS hello?command=reactive&numrobots=" + numrobots + "&robotnr=" + robotnumber + "&poll=" + poll; }
        }

        public async Task ProcessConnectionOnServer(ISocket socket)
        {
            var response = "ok";

            var replicatedGrain = ReplicatedHelloGrainFactory.GetGrain(0);

            if (robotnumber > 0)
            {
                // wait for msg by previous robot
                response = await (poll ? WaitForMsgByPolling(replicatedGrain) 
                    : WaitForMsgBySubscription(replicatedGrain));
            }

            if (response == "ok" && robotnumber < numrobots)
            {
                await replicatedGrain.Hello(robotnumber.ToString());
            }
          
            await socket.Close(response);
        }

        private async Task<string> WaitForMsgByPolling(IReplicatedHelloGrain replicatedGrain)
        {
            while (true)
            {
                var msgs = (await replicatedGrain.GetTopMessagesAsync(false));

                if (msgs[msgs.Length - 1] == (robotnumber - 1).ToString())
                    break;

                await Task.Delay(50);
            }
            return "ok";
        }

        private async Task<string> WaitForMsgBySubscription(IReplicatedHelloGrain replicatedGrain)
        {  
            var promise = new ObserverAdapter()
            {
                replicatedGrain = replicatedGrain,
                robotnumber = robotnumber
            };

           // TODO
       //      stream = await replicatedGrain.GetTopMessagesStreamAsync();

         //   await stream.SubscribeAsync(promise);

            return await promise.Task;
        }

    

        public class ObserverAdapter : TaskCompletionSource<string>, IAsyncObserver<String[]>
        {
            public int robotnumber;
            public IReplicatedHelloGrain replicatedGrain;

            public async Task OnCompletedAsync()
            {
                TrySetResult("stream ended unexpectedly");
            }

            public async Task OnErrorAsync(Exception ex)
            {
               TrySetResult("error in stream: " + ex);
            }

            public async Task OnNextAsync(String[] msgs, StreamSequenceToken token = null)
            {
                if (msgs[msgs.Length - 1] == (robotnumber - 1).ToString())
                    TrySetResult("ok");
            }
        }


        public async Task ProcessMessageOnServer(ISocket socket, string message)
        {
            await socket.Close("error: no messages expected from client");
        }

        public async Task ProcessCloseOnServer(ISocket socket, string message)
        {
            await socket.Close("error: should have been closed by server");
        }

        public async Task<string> ProcessConnectionOnClient(ISocket socket)
        {
            return "connected";
        }

        public async Task<string> ProcessMessageOnClient(ISocket socket, string message)
        {
            return "error: expect no messages from server";
        }

        public async Task<string> ProcessCloseOnClient(ISocket socket, string message)
        {
            await socket.Close("ack");
            return message;
        }
    }

 

}
