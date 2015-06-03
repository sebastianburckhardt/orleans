using Common;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Hubs;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using System.Web.Http;
using System.Web.WebSockets;

namespace Conductor.Webrole.Controllers
{
    public class RobotsController : ApiController
    {

        // GET api/robots
        public HttpResponseMessage Get()
        {
            if (HttpContext.Current.IsWebSocketRequest)
            {
                HttpContext.Current.AcceptWebSocketRequest(ProcessRobotConnection);
            }
            return new HttpResponseMessage(HttpStatusCode.SwitchingProtocols);
        }

        /*
        private async Task ProcessRobotWS(AspNetWebSocketContext context)
        {
            WebSocket socket = context.WebSocket;
            while (true)
            {
                ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024]);
                WebSocketReceiveResult result = await socket.ReceiveAsync(
                    buffer, CancellationToken.None);
                if (socket.State == WebSocketState.Open)
                {
                    string userMessage = Encoding.UTF8.GetString(
                        buffer.Array, 0, result.Count);
                    userMessage = "You sent: " + userMessage + " at " +
                        DateTime.Now.ToLongTimeString();
                    buffer = new ArraySegment<byte>(
                        Encoding.UTF8.GetBytes(userMessage));
                    await socket.SendAsync(
                        buffer, WebSocketMessageType.Text, true, CancellationToken.None);
                }
                else
                {
                    break;
                }
            }
        }
        */

        private async Task ProcessRobotConnection(AspNetWebSocketContext context)
        {
            WebSocket socket = context.WebSocket;
            string instance = null;

            //DefaultHubManager hd = new DefaultHubManager(GlobalHost.DependencyResolver);
            //var hub = hd.ResolveHub("CommandHub") as CommandHub;

            var conductor = Conductor.Instance;
            if (conductor.Hub == null)
                await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "No console connected", CancellationToken.None);

            try
            {
                while (socket.State == WebSocketState.Open || socket.State == WebSocketState.CloseSent)
                {
                    int bufsize = 1024;
                    var receiveBuffer = new byte[bufsize];
                    WebSocketReceiveResult receiveResult = await socket.ReceiveAsync(
                        new ArraySegment<byte>(receiveBuffer), CancellationToken.None);

                    if (receiveResult.MessageType == WebSocketMessageType.Close)
                    {
                        if (instance != null)
                            conductor.OnDisconnect(instance, receiveResult.CloseStatusDescription);
                        await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "close ack", CancellationToken.None);
                    }
                    else if (receiveResult.MessageType != WebSocketMessageType.Text)
                    {
                        await socket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Cannot accept binary frame", CancellationToken.None);
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

                            receiveResult = await socket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer, count, bufsize - count), CancellationToken.None);

                            //if (receiveResult.MessageType != WebSocketMessageType.Text)
                            //    await ws.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "expected text frame", CancellationToken.None);

                            count += receiveResult.Count;
                        }
                        
                        string userMessageJson = Encoding.UTF8.GetString(receiveBuffer, 0, count);
                        JObject message = JObject.Parse(userMessageJson);
                        string messageType = (string)message["type"];
                        if (messageType.StartsWith("READY"))
                        {
                            //instance = userMessage.Substring(userMessage.IndexOf(' ') + 1);
                            instance = (string)message["loadgenerator"];
                            conductor.OnConnect(instance, socket);
                        }
                        else if (messageType.StartsWith("TRACE"))
                        {
                            //userMessage = userMessage.Substring(userMessage.IndexOf(' ') + 1);
                            var traceMessage = (string)message["message"];
                            await conductor.Trace(traceMessage);
                        }
                        else if (messageType.StartsWith("DONE") || 
                            messageType.StartsWith("EXCEPTION"))
                        {

                            /*userMessage = userMessage.Substring(userMessage.IndexOf(' ') + 1);
                            var pos = userMessage.IndexOf(' ');
                            var robotnr = int.Parse(userMessage.Substring(0, pos));
                            var rvalPos = userMessage.IndexOf(' ', pos + 1);
                            var statsBase64 = userMessage.Substring(pos + 1, rvalPos - pos);
                            var rval = userMessage.Substring(rvalPos + 1);*/
                            var robotnr = int.Parse((string)message["robotnr"]);
                            var statsBase64 = (string)message["stats"];
                            var rval = (string)message["retval"];

                            Dictionary<string, LatencyDistribution> stats;

                            byte[] statsBinary = null;

                            statsBinary = System.Convert.FromBase64String(statsBase64);
                            BinaryFormatter bf = new BinaryFormatter();
                            using (MemoryStream ms = new MemoryStream(statsBinary))
                            {
                                stats = (Dictionary<string, LatencyDistribution>)bf.Deserialize(ms);
                            }

                            if (messageType.StartsWith("EXCEPTION"))
                            {
                                rval = "Exception occurred on FrontEnd: (skipping the scenario):" + rval;
                            }
                            conductor.OnRobotMessage(robotnr, rval, stats);
                        }
                    }
                }
            } 
            catch(Exception e)
            {
                conductor.WriteLine("Exception: " + e.Message + e.StackTrace);
            }
        }


        /*
         * 
        // GET api/<controller>
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }

        // GET api/<controller>/5
        public string Get(int id)
        {
            return "value";
        }

        // POST api/<controller>
        public void Post([FromBody]string value)
        {
        }

        // PUT api/<controller>/5
        public void Put(int id, [FromBody]string value)
        {
        }

        // DELETE api/<controller>/5
        public void Delete(int id)
        {
        }
        
        */
    }
}