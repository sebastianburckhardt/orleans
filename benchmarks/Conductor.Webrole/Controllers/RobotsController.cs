using Common;
using Microsoft.AspNet.SignalR;
using Microsoft.AspNet.SignalR.Hubs;
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

            while (socket.State == WebSocketState.Open || socket.State == WebSocketState.CloseSent)
            {
                ArraySegment<byte> buffer = new ArraySegment<byte>(new byte[1024 * 4]);
                WebSocketReceiveResult result = await socket.ReceiveAsync(
                    buffer, CancellationToken.  None);

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    if (instance != null)
                        conductor.OnDisconnect(instance, result.CloseStatusDescription);
                    await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "close ack", CancellationToken.None);
                }
                else if (result.MessageType != WebSocketMessageType.Text)
                {
                    await socket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "Cannot accept binary frame", CancellationToken.None);
                }
                else
                {

                    string userMessage = Encoding.UTF8.GetString(buffer.Array, 0, result.Count);

                    if (userMessage.StartsWith("READY"))
                    {
                        instance = userMessage.Substring(userMessage.IndexOf(' ') + 1);
                        conductor.OnConnect(instance, socket);
                    }
                    else if (userMessage.StartsWith("DONE"))
                    {
                        userMessage = userMessage.Substring(userMessage.IndexOf(' ') + 1);
                        var pos = userMessage.IndexOf(' ');
                        var robotnr = int.Parse(userMessage.Substring(0, pos));
                        var statsPos = userMessage.IndexOf(' ', pos + 1);
                        var msg = userMessage.Substring(pos + 1, statsPos - pos) + "\n";
                        
                        var statsBase64 = userMessage.Substring(statsPos + 1);
                        byte[] statsBinary = null;
                        try
                        {
                            statsBinary = System.Convert.FromBase64String(statsBase64);
                        }
                        catch (Exception e)
                        {

                        }
                        BinaryFormatter bf = new BinaryFormatter();
                        using (MemoryStream ms = new MemoryStream(statsBinary))
                        {
                            var stats = (Dictionary<string, LatencyDistribution>)bf.Deserialize(ms);
                            msg += Util.PrintStats(stats);
                        }
                        
                        conductor.OnRobotMessage(robotnr, msg);
                    }
                }
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