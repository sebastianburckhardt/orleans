using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Counters;

namespace Orleans.Messaging
{
    /// <summary>
    /// The Receiver class is used by the GatewayConnection to receive messages. It runs its own thread, but it performs all i/o operations synchronously.
    /// </summary>
    internal class GatewayClientReceiver : AsynchAgent
    {
        private readonly GatewayConnection gatewayConnection;

        internal GatewayClientReceiver(GatewayConnection gw)
        {
            gatewayConnection = gw;
            onFault = FaultBehavior.RestartOnFault;
        }

        protected override void Run()
        {
            if (gatewayConnection.MsgCenter.MessagingConfiguration.UseMessageBatching)
            {
                RunBatch();
            }
            else
            {
                RunNonBatch();
            }
        }

        protected void RunNonBatch()
        {
            try
            {
                var lengths = new byte[Message.LengthHeaderSize];
                var lengthSegments = new List<ArraySegment<byte>>() { new ArraySegment<byte>(lengths) };

                while (!cts.IsCancellationRequested)
                {
                    if (!FillBuffer(lengthSegments, Message.LengthHeaderSize))
                    {
                        continue;
                    }

                    var headerLength = BitConverter.ToInt32(lengths, 0);
                    var header = BufferPool.GlobalPool.GetMultiBuffer(headerLength);
                    var bodyLength = BitConverter.ToInt32(lengths, 4);
                    var body = BufferPool.GlobalPool.GetMultiBuffer(bodyLength);

                    if (!FillBuffer(header, headerLength))
                    {
                        continue;
                    }
                    if (!FillBuffer(body, bodyLength))
                    {
                        continue;
                    }
                    var msg = new Message(header, body);
                    //MessagingStatisticsGroup.OnMessageReceive(gatewayConnection.Address, headerLength, bodyLength);
                    MessagingStatisticsGroup.OnMessageReceive(msg, headerLength, bodyLength);

                    if (log.IsVerbose3) log.Verbose3("Received a message from gateway {0}: {1}", gatewayConnection.Address, msg);
                    gatewayConnection.MsgCenter.QueueIncomingMessage(msg);
                }
            }
            catch (Exception ex)
            {
                //gatewayConnection.MarkAsDisconnected();
                log.Warn(ErrorCode.ProxyClientUnhandledExceptionWhileReceiving, String.Format("Unexpected/unhandled exception while receiving: {0}. Restarting gateway receiver for {1}.",
                    ex, gatewayConnection.Address), ex);
                throw;
            }
        }

        private void RunBatch()
        {
            try
            {
                var MetaHeader = new byte[Message.LengthMetaHeader];
                var MetaHeaderSegments = new List<ArraySegment<byte>>() { new ArraySegment<byte>(MetaHeader) };

                while (!cts.IsCancellationRequested)
                {
                    if (!FillBuffer(MetaHeaderSegments, Message.LengthMetaHeader))
                    {
                        continue;
                    }

                    var numberOfMessages = BitConverter.ToInt32(MetaHeader, 0);
                    var lengths = new byte[Message.LengthHeaderSize * numberOfMessages];
                    var lengthSegments = new List<ArraySegment<byte>>() { new ArraySegment<byte>(lengths) };

                    if (!FillBuffer(lengthSegments, Message.LengthHeaderSize * numberOfMessages))
                    {
                        continue;
                    }

                    var headerLengths = new int[numberOfMessages];
                    var bodyLengths = new int[numberOfMessages];
                    var headerbodiesLength = 0;
                    var headerbodies = new List<ArraySegment<byte>>();

                    for (int i = 0; i < numberOfMessages; i++)
                    {
                        headerLengths[i] = BitConverter.ToInt32(lengths, i * 8);
                        bodyLengths[i] = BitConverter.ToInt32(lengths, i * 8 + 4);
                        headerbodiesLength += (headerLengths[i] + bodyLengths[i]);

                        // We need to set the boundary of ArraySegment<byte>s to the same as the header/body boundary
                        headerbodies.AddRange(BufferPool.GlobalPool.GetMultiBuffer(headerLengths[i]));
                        headerbodies.AddRange(BufferPool.GlobalPool.GetMultiBuffer(bodyLengths[i]));
                    }

                    if (!FillBuffer(headerbodies, headerbodiesLength))
                    {
                        continue;
                    }

                    var lengthSoFar = 0;
                    for (int i = 0; i < numberOfMessages; i++)
                    {
                        var header = ByteArrayBuilder.BuildSegmentListWithLengthLimit(headerbodies, lengthSoFar, headerLengths[i]);
                        var body = ByteArrayBuilder.BuildSegmentListWithLengthLimit(headerbodies, lengthSoFar + headerLengths[i], bodyLengths[i]);
                        lengthSoFar += (headerLengths[i] + bodyLengths[i]);

                        var msg = new Message(header, body);
                        //MessagingStatisticsGroup.OnMessageReceive(gatewayConnection.Address, headerLength, bodyLength);
                        MessagingStatisticsGroup.OnMessageReceive(msg, headerLengths[i], bodyLengths[i]);

                        if (log.IsVerbose3) log.Verbose3("Received a message from gateway {0}: {1}", gatewayConnection.Address, msg);
                        gatewayConnection.MsgCenter.QueueIncomingMessage(msg);
                    }
                    MessagingStatisticsGroup.OnMessageBatchReceive(SocketDirection.ClientToGW, numberOfMessages, lengthSoFar);
                }
            }
            catch (Exception ex)
            {
                //gatewayConnection.MarkAsDisconnected();
                log.Warn(ErrorCode.ProxyClientUnhandledExceptionWhileReceiving, String.Format("Unexpected/unhandled exception while receiving: {0}. Restarting gateway receiver for {1}.",
                    ex, gatewayConnection.Address), ex);
                throw;
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private bool FillBuffer(List<ArraySegment<byte>> buffer, int length)
        {
            var offset = 0;

            while (offset < length)
            {
                Socket socketCapture = null;
                if (cts.IsCancellationRequested)
                {
                    return false;
                }

                try
                {
                    if (gatewayConnection.Sock == null || !gatewayConnection.Sock.Connected)
                    {
                        gatewayConnection.Connect();
                    }
                    socketCapture = gatewayConnection.Sock;
                    if (socketCapture != null && socketCapture.Connected)
                    {
                        var bytesRead = socketCapture.Receive(ByteArrayBuilder.BuildSegmentList(buffer, offset));
                        if (bytesRead == 0)
                        {
                            throw new EndOfStreamException("Socket closed");
                        }
                        offset += bytesRead;
                    }
                }
                catch (Exception ex)
                {
                    // Only try to reconnect if we're not shutting down
                    if (!cts.IsCancellationRequested)
                    {
                        if (ex is SocketException)
                        {
                            log.Warn(ErrorCode.Runtime_Error_100158, "Exception receiving from gateway " + gatewayConnection.Address, ex);
                        }
                        // GK - was: gatewayConnection.MarkAsDead();
                        gatewayConnection.MarkAsDisconnected(socketCapture);
                    }
                    return false;
                }
            }
            return true;
        }
    }
}
