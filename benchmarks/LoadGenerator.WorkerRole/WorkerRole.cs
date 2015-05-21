using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using System.Net.WebSockets;
using System.Text;

namespace LoadGenerator.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);

        public override void Run()
        {
            Trace.TraceInformation("LoadGenerator.WorkerRole is running");

            try
            {
                this.RunAsync(this.cancellationTokenSource.Token).Wait();
            }
            finally
            {
                this.runCompleteEvent.Set();
            }
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 300;

            // For information on handling configuration changes
            // see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

            bool result = base.OnStart();

            Trace.TraceInformation("LoadGenerator.WorkerRole has been started");

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("LoadGenerator.WorkerRole is stopping");

            this.cancellationTokenSource.Cancel();
            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("LoadGenerator.WorkerRole has stopped");
        }

        private byte[] receiveBuffer = new byte[512];

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            var deployment = RoleEnvironment.DeploymentId;
            var instance = RoleEnvironment.CurrentRoleInstance.Id;
            int connectioncount = 0;

            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {

                var uri = new Uri("ws://localhost:20473/api/robots");


                using (var ws = new ClientWebSocket())
                {

                    try
                    {
                        Trace.TraceInformation(string.Format("Connecting to {0}...", uri));

                        await ws.ConnectAsync(uri, cancellationToken);

                        if (ws.State == WebSocketState.Open)
                        {
                            Trace.TraceInformation("Connected.");

                            //  LoadGenerator ->  Conductor   :  READY instanceid
                            var message = "READY " + instance + "." + connectioncount++.ToString();
                            ArraySegment<byte> outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
                            await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, cancellationToken);

                            Trace.TraceInformation(string.Format("Sent {0}", message));
                        }  

                        // receive loop
                        while (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseSent)
                        {
                            WebSocketReceiveResult receiveResult = null;

                            int bufsize = receiveBuffer.Length;
                            receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), cancellationToken);

                            if (receiveResult.MessageType == WebSocketMessageType.Close)
                            {
                                Trace.TraceInformation("Received Close.");

                                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "close ack", cancellationToken);
                            }
                            else if (receiveResult.MessageType != WebSocketMessageType.Text)
                            {
                                Trace.TraceInformation("Received wrong message type.");

                                await ws.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Cannot accept binary frame", cancellationToken);
                            }
                            else
                            {
                                 

                                int count = receiveResult.Count;

                                while (receiveResult.EndOfMessage == false)
                                {
                                    if (count >= bufsize)
                                    {
                                        // enlarge buffer
                                        bufsize = bufsize * 2;
                                        var newbuf = new byte[bufsize * 2];
                                        receiveBuffer.CopyTo(newbuf, 0);
                                        receiveBuffer = newbuf;
                                    }

                                    receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer, count, bufsize - count), cancellationToken);

                                    if (receiveResult.MessageType != WebSocketMessageType.Text)
                                        await ws.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "expected text frame", cancellationToken);

                                    count += receiveResult.Count;
                                }

                                var content = Encoding.UTF8.GetString(receiveBuffer, 0, count);

                                Trace.TraceInformation("Received " + content);

                                //  Conductor -> LoadGenerator : START robotnr args

                                content = content.Substring(content.IndexOf(' ') + 1);
                                var pos = content.IndexOf(' ');
                                var robotnr = int.Parse(content.Substring(0, pos));
                                var args = content.Substring(pos + 1);

                                //  LoadGenerator -> Conductor : DONE robotnr result

                                var message = "DONE " + robotnr.ToString() + " " + args;

                                var outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
                                await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, cancellationToken);
                                Trace.TraceInformation("Sent " + message);
                            }
                        }
                    }
                        catch(Exception e)
                    {
                        Trace.TraceInformation(string.Format("Exception caught: {0}", e));
                    }
                    finally
                    {
                        // close websocket if it is still open
                        if (ws.State == WebSocketState.Open)
                            Task.WaitAny(
                                ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None),
                                Task.Delay(10000));
                    }

                    await Task.Delay(10000); // retry in 10 sec

                }
            }
        }
    }
}
