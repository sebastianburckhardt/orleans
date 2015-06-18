using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.WebSockets;
using System.Threading;
using GeoOrleans.Runtime.Common;
using GeoOrleans.Benchmarks.Common;

namespace GeoOrleans.Benchmarks.Orleans.Frontend
{



    public class WebSocketWrapper : ISocket
    {

        internal WebSocket webSocket;
        internal Guid guid;
        internal ISocketRequest sr;
        internal Action<string> tracer;

        public WebSocketWrapper(WebSocket ws, Guid guid, ISocketRequest sr, Action<string> tracer)
        {
            this.webSocket = ws;
            this.guid = guid;
            this.sr = sr;
            this.tracer = tracer;
        }

        public async Task Send(string message)
        {
            ArraySegment<byte> outputBuffer = new ArraySegment<byte>(Encoding.UTF8.GetBytes(message));

            //tracer(member + "<-" + (content.Length > 1024 ? content.Substring(0, 1024) : content));

            // Now send the data using `SendAsync` using `WebSocketMessageType.Text` as the message type.
            await webSocket.SendAsync(outputBuffer, WebSocketMessageType.Text, true, CancellationToken.None);
        }

        public async Task Close(string message)
        {
            await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, message, CancellationToken.None);
        }

        private byte[] receiveBuffer = new byte[512];


        public async Task ReceiveLoop()
        {
            try
            {

                while (true)
                {
                    WebSocketReceiveResult receiveResult = null;

                    int bufsize = receiveBuffer.Length;

                    receiveResult = await webSocket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None);
                    
                    //tracer("received " + receiveResult.Count + " bytes, eom=" + receiveResult.EndOfMessage);


                    if (receiveResult.MessageType == WebSocketMessageType.Close)
                    {
                        await sr.ProcessCloseOnServer(this, receiveResult.CloseStatusDescription);
                        break;
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
                            //tracer(member + "-> (cont)");

                            if (count >= bufsize)
                            {
                                // enlarge buffer
                                bufsize = bufsize * 2;
                                var newbuf = new byte[bufsize * 2];
                                receiveBuffer.CopyTo(newbuf, 0);
                                receiveBuffer = newbuf;
                            }

                            receiveResult = await webSocket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer, count, bufsize - count), CancellationToken.None);
                            //tracer("received " + receiveResult.Count + " more bytes, eom=" + receiveResult.EndOfMessage);

                            if (receiveResult.MessageType != WebSocketMessageType.Text)
                                await webSocket.CloseAsync(WebSocketCloseStatus.InvalidMessageType, "expected text frame", CancellationToken.None);

                            count += receiveResult.Count;
                        }

                        var content = Encoding.UTF8.GetString(receiveBuffer, 0, count);

                        //tracer(member + "->" + (content.Length > 400 ? (content.Substring(0, 300) + "(...)" + content.Substring(content.Length-96, 95)) : content));

                        await sr.ProcessMessageOnServer(this, content);
                    }
                }
            }

            finally
            {
                if (webSocket != null)
                    webSocket.Dispose();
            }


        }
    }

}
