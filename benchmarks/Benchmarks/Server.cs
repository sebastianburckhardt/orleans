using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net.WebSockets;
using System.Diagnostics;
using System.Web;
using Benchmarks;
using Newtonsoft.Json.Linq;
using System.Collections.Specialized;
using Common;

namespace Benchmarks
{
    /// <summary>
    /// An HttpListener for receiving requests (both regular http requests and websocket connections).
    /// Used both for running locally during simulation, or in a worker role in Azure for the Orleans front end.
    /// </summary>
    public class Server
    {
        private int connectioncount = 0;

        public Server(string deployment, string instanceid, bool runningincloud,
            bool securehttp, Action<string> tracer, Action<string> diag)
        {
            this.deployment = deployment;
            this.instanceid = instanceid;
            this.runningincloud = runningincloud;
            this.securehttp = securehttp;
            this.tracer = tracer;
            this.diag = diag;
        }

        internal string deployment;
        internal string instanceid;
        internal bool runningincloud;
        internal bool securehttp;
        internal Action<string> tracer;
        internal Action<string> diag;

        internal HttpListener listener;
        internal BenchmarkList benchmarks;

       
        public void Start(string listenerPrefix)
        {

            listener = new HttpListener();
            listener.Prefixes.Add(listenerPrefix);
            listener.AuthenticationSchemes = AuthenticationSchemes.Anonymous; 

            listener.Start();

            benchmarks = new BenchmarkList();

            servertask = Serve();
        }

        private Task servertask;

        public async Task Serve()
        {
            try
            {
                while (true)
                {
                    HttpListenerContext listenerContext = await listener.GetContextAsync();

                    var request = listenerContext.Request;

                    var url = new Uri("http://" + request.UserHostName + request.RawUrl);
                    var verb = request.IsWebSocketRequest ? "WS" : request.HttpMethod;

                    string body = null;
                    if (request.HasEntityBody)
                    {
                        System.IO.Stream bodystr = request.InputStream;
                        System.IO.StreamReader reader = new System.IO.StreamReader(bodystr, request.ContentEncoding);
                        body = await reader.ReadToEndAsync();
                    }

                    //Potential bug. Ask Sebastian 
                    //var urlpath = url.AbsolutePath.Split('/').Select(s => HttpUtility.UrlDecode(s)).Skip(2).ToArray(); 
                    var urlpath = new String[] { url.AbsolutePath.Split('/').Last() };
                    var arguments = HttpUtility.ParseQueryString(url.Query);

                    var testname = arguments["testname"];
                    arguments.Remove("testname");

                    tracer(string.Format("---> {0} {1}", verb, request.RawUrl));

                    var handler = benchmarks.ParseRequest(verb, urlpath, arguments, body);
                    System.Console.Write("Body {0} ", body);

                        if (handler == null)
                        {
                            var response = listenerContext.Response;
                            response.StatusCode = (int) HttpStatusCode.BadRequest;
                            response.StatusDescription = "no registered handler for this request";
                            response.Close();
                            tracer("<- Error: Unsupported request");
                            continue;
                        }
            
                    var bgtask = request.IsWebSocketRequest ?
                      ProcessWebsocketRequest((ISocketRequest) handler, urlpath, arguments, testname, listenerContext)
                      : ProcessHttpRequest((IHttpRequest) handler, urlpath, arguments, testname, verb, body, listenerContext);
                }
            }
            catch (HttpListenerException e)
            {
                diag("HttpListenerException caught: " + e.Message);
            }
        }


        public string IsDown()
        {
            if (servertask != null && servertask.IsCompleted)
                return "request listener is stopped";

            return null;
        }

        public void Stop(string reason, bool stoplisteners)
        {
            if (stoplisteners && listener != null)
            {
                try
                {
                    diag("Stopping Listener...");
                    listener.Stop();
                    diag("OK.");

                }
                catch (Exception e)
                {
                    diag("Exception caught while stopping listener:" + e);
                }
            }
        }

        public string GetIdentity()
        {
            return instanceid;
        }

        private async Task ProcessHttpRequest(IHttpRequest handler,
                IEnumerable<string> urlpath, NameValueCollection arguments, string testname, string verb, string body,
                HttpListenerContext listenerContext)
        {

            var request = listenerContext.Request;
            var response = listenerContext.Response;


            try
            {
                var responsestring = await handler.ProcessRequestOnServer();

                // prevent caching of responses
                response.Headers.Add("Cache-Control", "no-cache");

                if (responsestring != null)
                    EncodeJsonResponse(response, responsestring);

                response.Close();

                tracer("<- HttpResponse " + (responsestring.Length > 100 ? ("(" + responsestring.Length + " characters)") : responsestring));
            }
            catch (Exception ee)
            {
                while (ee is AggregateException)
                    ee = ee.InnerException;

                if (ee is HttpException)
                {
                    var he = (HttpException)ee;
                    response.StatusCode = he.GetHttpCode();
                    response.StatusDescription = he.Message;
                }
                else
                {
                    response.StatusCode = (int)HttpStatusCode.InternalServerError;
                    response.StatusDescription = "Server Error";
                }

                // send Json with detailed error description to Client
                var json = new JObject();
                json["code"] = response.StatusCode;
                json["error"] = response.StatusDescription;
                var exc = new JObject();
                exc["type"] = ee.GetType().Name;
                exc["message"] = ee.Message;
                exc["stacktrace"] = ee.StackTrace;
                json["exception"] = exc;
                EncodeJsonResponse(response, json.ToString());
                response.Close();

                tracer("<- HttpErrorResponse: " + ee.GetType().Name + ": " + ee.Message + "\n" + ee.StackTrace);
            }
        }

        private static void EncodeJsonResponse(HttpListenerResponse response, string responsestring)
        {
            System.Text.Encoding encoding = response.ContentEncoding;
            if (encoding == null)
            {
                encoding = System.Text.Encoding.UTF8;
                response.ContentEncoding = encoding;
            }
            byte[] buffer = encoding.GetBytes(responsestring);
            // Get a response stream and write the response to it.
            response.ContentType = "application/json";
            response.ContentLength64 = buffer.Length;
            System.IO.Stream output = response.OutputStream;
            output.Write(buffer, 0, buffer.Length);
            // Send the response
            output.Close();
        }



        [ThreadStatic]
        private static System.Security.Cryptography.RandomNumberGenerator rng;


         private async Task ProcessWebsocketRequest(ISocketRequest handler, IEnumerable<string> urlpath, NameValueCollection arguments, string testname, HttpListenerContext listenerContext)
        {

            WebSocketContext webSocketContext = null;

            try
            {
                var request = listenerContext.Request;

                // When calling `AcceptWebSocketAsync` the negotiated subprotocol must be specified. This sample assumes that no subprotocol 
                // was requested. 
                webSocketContext = await listenerContext.AcceptWebSocketAsync(subProtocol: null);
            }
            catch (Exception e)
            {
                // The upgrade process failed somehow. For simplicity lets assume it was a failure on the part of the server and indicate this using 500.
                listenerContext.Response.StatusCode = 500;
                listenerContext.Response.Close();
                tracer("Connection failed. Exception: " + e);
                return;
            }

            // socket is now established.
    
            if (rng == null)
                rng = new System.Security.Cryptography.RNGCryptoServiceProvider();
            var data = new byte[16];
            lock (rng)
                rng.GetBytes(data);
            var guid = new Guid(data);

            Interlocked.Increment(ref connectioncount);

            try
            {
                tracer("Connection succeeded. Count: " + connectioncount);

                var wswrapper = new WebSocketWrapper(webSocketContext.WebSocket, guid, handler, tracer);

                await wswrapper.ReceiveLoop();
            }
            catch (Exception e)
            {
                diag("Exception in Connection handler " + e);
            }
            finally
            {
                Interlocked.Decrement(ref connectioncount);

                tracer("Connection closed. Count: " + connectioncount);
            }
        }
    }


    public class WebSocketWrapper : ISocket
    {

        internal WebSocket webSocket;
        internal Guid guid;
        internal string guidprefix;
        internal ISocketRequest sr;
        internal Action<string> tracer;

        public WebSocketWrapper(WebSocket ws, Guid guid, ISocketRequest sr, Action<string> tracer)
        {
            this.webSocket = ws;
            this.guid = guid;
            this.guidprefix = "[" + guid.ToString().Substring(0, 5) + "]";
            this.sr = sr;
            this.tracer = tracer;
        }

        public async Task Send(string message)
        {
            ArraySegment<byte> outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));

            tracer("<- " + this.guidprefix + " " + (message.Length > 1024 ? message.Substring(0, 1024) : message));

            // Now send the data using `SendAsync` using `WebSocketMessageType.Text` as the message type.
            await webSocket.SendAsync(outputBuffer, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public async Task Close(string message)
        {
            tracer("<- " + this.guidprefix + " CLOSE " + (message == null ? "" :(message.Length > 1024 ? message.Substring(0, 1024) : message)));

            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, message, CancellationToken.None);
        }

        private byte[] receiveBuffer = new byte[512];


        public async Task ReceiveLoop()
        {
            try
            {

                while (webSocket.State == WebSocketState.Open || webSocket.State == WebSocketState.CloseSent)
                {
                    WebSocketReceiveResult receiveResult = null;

                    int bufsize = receiveBuffer.Length;

                    receiveResult = await webSocket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None);

                    if (receiveResult.MessageType == WebSocketMessageType.Close)
                    {
                        var message = receiveResult.CloseStatusDescription;

                        tracer("-> " + this.guidprefix + " CLOSE " + (message.Length > 1024 ? message.Substring(0, 1024) : message));

                        await sr.ProcessCloseOnServer(this, message);
                    }
                    else if (receiveResult.MessageType != WebSocketMessageType.Text)
                    {
                        await webSocket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Cannot accept binary frame", CancellationToken.None);
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

                            receiveResult = await webSocket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer, count, bufsize - count), CancellationToken.None);

                            if (receiveResult.MessageType != WebSocketMessageType.Text)
                                await webSocket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "expected text frame", CancellationToken.None);

                            count += receiveResult.Count;
                        }

                        var content = Encoding.UTF8.GetString(receiveBuffer, 0, count);

                        tracer("-> " + this.guidprefix + " " + (content.Length > 400 ? (content.Substring(0, 300) + "(...)" + content.Substring(content.Length - 96, 95)) : content));

                        await sr.ProcessMessageOnServer(this, content);
                    }
                }
            }
            catch (Exception e)
            {
                if (webSocket != null)
                {
                    tracer("<- " + this.guidprefix + " CLOSE due to exception " + e.ToString());
                        Task.WaitAny(
                            webSocket.CloseOutputAsync(WebSocketCloseStatus.InternalServerError, "server error: " + e.Message, CancellationToken.None),
                            Task.Delay(10000)
                            );
                }
            }
            finally
            {
                if (webSocket != null)
                {
                    webSocket.Abort();
                    webSocket.Dispose();
                }
            }


        }
    }
}
