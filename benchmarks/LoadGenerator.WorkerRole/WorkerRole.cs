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
using Benchmarks;
using Common;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
namespace LoadGenerator.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        private readonly BenchmarkList benchmarks = new BenchmarkList();

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

        private async Task StartRobotAsync(string content, WebSocket ws, Func<string, Task> tracer, CancellationToken cancellationToken)
        {
            JObject testdata = JObject.Parse(content);
            string testname = (string)testdata["testname"];
            int robotnr = int.Parse((string)testdata["robotnr"]);
            string args = (string)testdata["args"];


            var scenarioStartPos = testname.LastIndexOf("."); //assuming senario name doesnt have "."
            var benchmarkStartPos = testname.LastIndexOf(".", scenarioStartPos - 1); //searches backward

            var scenarioName = testname.Substring(scenarioStartPos + 1).Trim(); //why is there extra space?
            var benchmarkName = testname.Substring(benchmarkStartPos + 1, scenarioStartPos - benchmarkStartPos - 1);

            var benchmark = benchmarks.ByName(benchmarkName);
            var scenarios = benchmark.Scenarios.Where(s => s.Name.Equals(scenarioName, StringComparison.CurrentCultureIgnoreCase));

            if (scenarios.Count() != 1) //i.e. no scenorio or more than one scenario with same name.
            {
                //TODO: Ask sebastian about error handling.
                await tracer("No such scenario found");
                Trace.TraceInformation("No such scenario found");
                return;
            }

            var scenario = scenarios.First();
            string serviceEndpoint = scenario.RobotServiceEndpoint(robotnr);
            String retval;
            var client = new Benchmarks.Client(serviceEndpoint, testname, robotnr, tracer);

            await tracer("Starting robot: " + robotnr + DateTime.Now.ToString());
            //Should catch exceptions from the robot script and notify the conductor of the failure. Which may decide to retry if required.
            //no need to disconnect from the conductor since this is the FE error.
            bool success = true;
            try
            {
                retval = await scenario.RobotScript(client, robotnr, args);
            }
            catch (Exception ex)
            {
                success = false;
                retval = ex.Message + " " + ex.StackTrace;
            }

            //  LoadGenerator -> Conductor : DONE robotnr stats retval

            var stats = client.Stats;
            BinaryFormatter bf = new BinaryFormatter();
            string statsBase64 = null;
            using (MemoryStream ms = new MemoryStream())
            {
                bf.Serialize(ms, stats);
                ms.Flush();
                statsBase64 = System.Convert.ToBase64String(ms.ToArray());
                byte[] converted = System.Convert.FromBase64String(statsBase64);
                Array.Equals(converted, ms.ToArray());
            }

            string messagetype;
            if (success)
            {
                messagetype = "DONE";
            }
            else
            {
                messagetype = "EXCEPTION";
            }

            //var statsBase64 = System.Convert.ToBase64String();
            //var message = "DONE " + robotnr.ToString() + " " + statsBase64 + " " + retval;
            JObject message = JObject.FromObject(new
            {
                type = messagetype,
                robotnr = robotnr.ToString(),
                stats = statsBase64,
                retval = retval
            });
            var outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message.ToString()));

            await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, cancellationToken);

            Trace.TraceInformation("Sent " + message);
        }


        private async Task RunAsync(CancellationToken cancellationToken)
        {
            var deployment = RoleEnvironment.DeploymentId;
            var instance = RoleEnvironment.CurrentRoleInstance.Id;
            int connectioncount = 0;

            // TODO: Replace the following with your own logic.
            while (!cancellationToken.IsCancellationRequested)
            {

                //var uri = new Uri("ws://localhost:20473/api/robots");
                //var uri = new Uri("ws://orleansgeoconductor.cloudapp.net/api/robots");
                var uri = new Uri("ws://" + Endpoints.GetConductor() + "/api/robots");

                Func<string, Task> tracer = null;

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
                            //var message = "READY " + instance + "." + connectioncount++.ToString();
                            JObject message = JObject.FromObject(new
                                {
                                    type = "READY",
                                    loadgenerator = deployment + "." + instance + "." + connectioncount++.ToString()
                                });
                            ArraySegment<byte> outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message.ToString()));
                            await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, cancellationToken);

                            Trace.TraceInformation(string.Format("Sent {0}", message));
                        }

                        // define trace function
                        tracer = async (string s) =>
                        {
                            if (ws.State == WebSocketState.Open)
                            {
                                //var message = "TRACE " + s;
                                JObject message = JObject.FromObject(new
                                {
                                    type = "TRACE",
                                    message = s
                                });
                                var outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message.ToString()));
                                await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, cancellationToken);
                                Trace.TraceInformation("Sent " + message);
                            }
                        };


                        // receive loop
                        while (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseSent)
                        {
                            WebSocketReceiveResult receiveResult = null;

                            int bufsize = receiveBuffer.Length;
                            try
                            {
                                //receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), cancellationToken);
                                receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), cancellationToken);
                            }
                            catch (Exception ex)
                            {
                                Trace.TraceInformation("Connection to conductor possibly timedout. Trying again");
                                continue;
                            }

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

                                //NOTE: Do not await
                                //Task.Run(() => StartRobotAsync(content, ws, tracer, cancellationToken));
                                StartRobotAsync(content, ws, tracer, cancellationToken);

                                //  Conductor -> LoadGenerator : START testname robotnr args

                                /*content = content.Substring(content.IndexOf(' ') + 1);
                                var pos1 = content.IndexOf(' ');
                                var pos2 = content.IndexOf(' ', pos1 + 1);
                                var testname = content.Substring(0, pos1 + 1);
                                var robotnr = int.Parse(content.Substring(pos1 + 1, pos2 - pos1));
                                var args = content.Substring(pos2 + 1);*/

                            }
                        }
                    }
                    catch (Exception e) //todo: use this handler only for websocket level exception. "server exceptions should be handled within the loop and the connection should be reused.
                    {
                        Trace.TraceInformation(string.Format("Exception caught: {0}", e));

                        // send exception to conductor if WS is open
                        if (tracer != null)
                            tracer(string.Format("Exception in load generator {0}: {1}", instance, e)).Wait();
                    }
                    finally
                    {
                        // close websocket if it is still open
                        if (ws.State == WebSocketState.Open)
                            Task.WaitAny(
                                ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", cancellationToken),
                                Task.Delay(10000));
                    }

                    await Task.Delay(10000); // retry in 10 sec

                }
            }
        }
    }
}
