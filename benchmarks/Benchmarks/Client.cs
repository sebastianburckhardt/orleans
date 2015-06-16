using Common;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Benchmarks
{
    /// <summary>
    /// A wrapper for Http Client functionality
    /// </summary>
    public class Client : IRobotContext
    {
        public Client(string urlpath, string testname, int robotnumber, Func<string, Task> tracer)
        {
            this.urlpath = urlpath;
            this.testname = testname;
            this.robotnumber = robotnumber;
            this.tracer = tracer;
        }

        string urlpath;
        string testname;
        int robotnumber;
        Func<string,Task> tracer;
        public Dictionary<string, LatencyDistribution> Stats = new Dictionary<string, LatencyDistribution>();

        public string TestName { get { return testname; } }

        public int RobotNumber { get { return robotnumber;  } }

        public async Task<string> ServiceRequest(IHttpRequest request)
        {
            var sig = request.Signature.Split(' ');
            Util.Assert(sig.Length == 2);
            Util.Assert(sig[0] == "GET" || sig[0] == "PUT" || sig[0] == "POST");
            var urlparams = (sig[1].Length == 0 ? "?" : (sig[1] + "&")) + "testname=" + testname;

            var req = (HttpWebRequest)WebRequest.Create("http://" + urlpath + "/" + urlparams);
            //var req = (HttpWebRequest)WebRequest.Create("http://localhost:843/simserver/test");
            req.Method = sig[0];

            string result = null;

            if (request.Body != null)
            {
                req.ContentType = "application/text";
                byte[] bytes = System.Text.Encoding.ASCII.GetBytes(request.Body);
                req.ContentLength = bytes.Length;
                System.IO.Stream os = await req.GetRequestStreamAsync();
                await os.WriteAsync(bytes, 0, bytes.Length);
                os.Close();
            }

            var sw = new Stopwatch();
            string responsecategory = "none";

            try
            {
                sw.Start();
                var resp = (HttpWebResponse)await req.GetResponseAsync();
                sw.Stop();

                if (resp != null)
                {
                    responsecategory = ((int)resp.StatusCode).ToString() + " " + resp.StatusCode.ToString();
                    System.IO.StreamReader sr = new System.IO.StreamReader(resp.GetResponseStream());
                    result = await request.ProcessResponseOnClient(await sr.ReadToEndAsync());
                }
            }
            catch (WebException we) //catch only web exceptions, let other exceptions be handled by parent handlers.
            {
                HttpWebResponse re = we.Response as HttpWebResponse;
                Exception ex;

                if (re != null && re.StatusCode == HttpStatusCode.InternalServerError)
                {
                    //THis exception will occur when there is a server exception and an error code (say 500 is sent).
                    //This is the scenario where the server (i.e. the front end has caught some unhandled exception and generated and sent a 500 error with details in the body)

                    string responseStr = null;
                    if (re != null)
                    {
                        using (Stream data = re.GetResponseStream())
                        {
                            using (var reader = new StreamReader(data))
                            {
                                responseStr = reader.ReadToEnd();
                            }
                        }
                    }
                    else
                    {
                        responseStr = we.Message == null ? we.Message : "No Error message";
                    }

                    if (re.StatusCode == HttpStatusCode.InternalServerError)
                    {
                        //This is the scenario where the server (i.e. the front end has caught some unhandled exception and generated and sent a 500 error with details in the body)

                        Exception clientex;
                        string message;
                        if (re.ContentType == "application/json")
                        {
                            message = responseStr;
                            //todo: do this recursively for inner exceptions.
                            /*JObject serverex = JObject.Parse(responseStr);
                            JObject exception = JObject.Parse((string)serverex["exception"]);
                            clientex.StackTrace = (string) exception["stacktrace"];*/
                            clientex = new Exception(String.Format("Following exception occured on frontend or orleans silo: {0}", message));
                        }
                        else
                        {
                            clientex = new Exception(responseStr);
                        }
                        ex = clientex;
                    }
                    else
                    {
                        ex = we;
                    }
                    throw ex;
                }
                else
                {
                    throw we;
                }
            }
            finally
            {
                sw.Stop();

                // request category is signature minus parameters
                var requestcategory = request.Signature;
                var pos = requestcategory.IndexOf('?');
                if (pos != -1)
                    requestcategory = requestcategory.Substring(0, pos);

                // statistics are collected per request and response category
                var key = requestcategory + " (" + responsecategory + ")";
                LatencyDistribution distribution;
                lock (Stats)
                {
                    if (!Stats.TryGetValue(key, out distribution))
                        distribution = Stats[key] = new LatencyDistribution();
                }
                try
                {
                    distribution.AddDataPoint(sw.ElapsedMilliseconds);
                }
                catch (Exception e)
                {
                    System.Console.Write("Latency should not be negative " + e.ToString());
                    //TODO HANDLE BETTER
                }                
            }
            return await Task.FromResult(result);
        }

        public class SocketWrapper : ISocket
        {
            public SocketWrapper(ClientWebSocket ws)
            {
                this.ws = ws;
            }
            ClientWebSocket ws;

            public async Task Send(string message)
            {
                ArraySegment<byte> outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));
                await ws.SendAsync(outputBuffer, WebSocketMessageType.Text, true, CancellationToken.None);
            }

            public async Task Close(string message)
            {
                await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, message, CancellationToken.None);
            }
        }

        private byte[] receiveBuffer = new byte[512];

        public async Task<string> ServiceConnection(ISocketRequest request)
        {
            var sig = request.Signature.Split(' ');
            Util.Assert(sig.Length == 2);
            Util.Assert(sig[0] == "WS");
            var urlparams = (sig[1].Length == 0 ? "?" : (sig[1] + "&")) + "testname=" + testname;
            var uri = new Uri("ws://" + urlpath + "/" + urlparams);
            string result = null;
            using (var ws = new ClientWebSocket())
            {
                try
                {
                    await ws.ConnectAsync(uri, CancellationToken.None);

                    var socketwrapper = new SocketWrapper(ws);
                    result = await request.ProcessConnectionOnClient(socketwrapper);

                    // receive loop
                    while (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseSent)  
                    {
                        WebSocketReceiveResult receiveResult = null;

                        int bufsize = receiveBuffer.Length;

                        receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None);

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            result = await request.ProcessCloseOnClient(socketwrapper, receiveResult.CloseStatusDescription);
                        }
                        else if (receiveResult.MessageType != WebSocketMessageType.Text)
                        {
                            await ws.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Cannot accept binary frame", CancellationToken.None);
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

                                receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer, count, bufsize - count), CancellationToken.None);

                                if (receiveResult.MessageType != WebSocketMessageType.Text)
                                    await ws.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "expected text frame", CancellationToken.None);

                                count += receiveResult.Count;
                            }

                            var content = Encoding.UTF8.GetString(receiveBuffer, 0, count);

                            result = await request.ProcessMessageOnClient(socketwrapper, content);
                        }
                    }
                }
                finally
                {
                    // close websocket if it is still open
                    if (ws.State == WebSocketState.Open)
                        Task.WaitAny(
                            ws.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "", CancellationToken.None),
                            Task.Delay(10000));
                }
                return await Task.FromResult(result);
            }
        }


        public IBenchmark Benchmark
        {
            get { return Benchmark; }
        }


        public Task Trace(string info)
        {
            return tracer(string.Format("[robot{0}] {1}", this.RobotNumber, info));
        }
    }
}
