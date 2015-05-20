using Common;
using System;
using System.Collections.Generic;
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
        public Client(string urlpath)
        {
            this.urlpath = urlpath;
        }

        string urlpath;

        public async Task ServiceRequest(IHttpRequest request)
        {
            var sig = request.Signature.Split(' ');
            Util.Assert(sig.Length == 2);
            Util.Assert(sig[0] == "GET" || sig[0] == "PUT" || sig[0] == "POST");

            var req = (HttpWebRequest) WebRequest.Create("http://" + urlpath + sig[1]);
            //var req = (HttpWebRequest)WebRequest.Create("http://localhost:843/simserver/test");
            req.Method = sig[0];
        

            if (request.Body != null)
            {
                req.ContentType = "application/text";
                byte [] bytes = System.Text.Encoding.ASCII.GetBytes(request.Body);
                req.ContentLength = bytes.Length;
                System.IO.Stream os = await req.GetRequestStreamAsync ();
                await os.WriteAsync (bytes, 0, bytes.Length); 
                os.Close();
            }

            var resp = await req.GetResponseAsync();
            if (resp != null)
            {
                System.IO.StreamReader sr = new System.IO.StreamReader(resp.GetResponseStream());
                await request.ProcessResponseOnClient(await sr.ReadToEndAsync());
            }  
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

        public async Task ServiceConnection(ISocketRequest request)
        {
            var sig = request.Signature.Split(' ');
            Util.Assert(sig.Length == 2);
            Util.Assert(sig[0] == "WS");
            var uri = new Uri("ws://" + urlpath + sig[1]);

            using (var ws = new ClientWebSocket())
            {
                try
                {
                    await ws.ConnectAsync(uri, CancellationToken.None);

                    var socketwrapper = new SocketWrapper(ws);
                    await request.ProcessConnectionOnClient(socketwrapper);

                    // receive loop
                    while (ws.State == WebSocketState.Open || ws.State == WebSocketState.CloseSent)  
                    {
                        WebSocketReceiveResult receiveResult = null;

                        int bufsize = receiveBuffer.Length;

                        receiveResult = await ws.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), CancellationToken.None);

                        if (receiveResult.MessageType == WebSocketMessageType.Close)
                        {
                            await request.ProcessCloseOnServer(socketwrapper, receiveResult.CloseStatusDescription);
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

                            await request.ProcessMessageOnClient(socketwrapper, content);
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
            }
        }


        public IBenchmark Benchmark
        {
            get { return Benchmark; }
        }
    }
}
