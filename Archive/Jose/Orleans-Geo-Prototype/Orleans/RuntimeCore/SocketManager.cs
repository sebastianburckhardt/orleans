using System;
using System.Net;
using System.Net.Sockets;
using Orleans.Counters;

namespace Orleans
{
    internal class SocketManager
    {
        private readonly LRU<IPEndPoint, Socket> cache;

        private const int MAX_SOCKETS = 200;

        public static readonly Guid SiloDirectConnectionId = new Guid("11111111-1111-1111-1111-111111111111");

        internal SocketManager(IMessagingConfiguration config)
        {
            cache = new LRU<IPEndPoint, Socket>(MAX_SOCKETS, config.MaxSocketAge, SendingSocketCreator);
            cache.RaiseFlushEvent += FlushHandler;
        }

        /// <summary>
        /// Creates a socket bound to an address for use accepting connections.
        /// This is for use by client gateways and other acceptors.
        /// </summary>
        /// <param name="address">The address to bind to.</param>
        /// <returns>The new socket, appropriately bound.</returns>
        internal static Socket GetAcceptingSocketForEndpoint(IPEndPoint address)
        {
            var s = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                // Prep the socket so it will reset on close
                s.LingerState = new LingerOption(true, 0);
                s.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
                // And bind it to the address
                s.Bind(address);
            }
            catch (Exception)
            {
                CloseSocket(s);
                throw;
            }
            return s;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1811:AvoidUncalledPrivateCode")]
        internal bool CheckSendingSocket(IPEndPoint target)
        {
            return cache.ContainsKey(target);
        }

        internal Socket GetSendingSocket(IPEndPoint target)
        {
            return cache.Get(target);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private Socket SendingSocketCreator(IPEndPoint target)
        {
            var s = new Socket(target.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                s.Connect(target);
                // Prep the socket so it will reset on close and won't Nagle
                s.LingerState = new LingerOption(true, 0);
                s.NoDelay = true;
                s.Send(SiloDirectConnectionId.ToByteArray()); // Identifies this client as a direct silo-to-silo socket
                // Hang an asynch receive off of the socket to detect closure
                var foo = new byte[4];
                s.BeginReceive(foo, 0, 1, SocketFlags.None, ReceiveCallback,
                    new Tuple<Socket, IPEndPoint, SocketManager>(s, target, this));
                NetworkingStatisticsGroup.OnOpenedSendingSocket();
            }
            catch (Exception)
            {
                try
                {
                    s.Close();
                }
                catch (Exception)
                {
                    // ignore
                }
                throw;
            }
            return s;
        }

        // We hang an asynch receive, with this callback, off of every send socket.
        // Since we should never see data coming in on these sockets, having the receive complete means that
        // the socket is in an unknown state and we should close it and try again.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void ReceiveCallback(IAsyncResult result)
        {
            try{
                var t = result.AsyncState as Tuple<Socket, IPEndPoint, SocketManager>;
                if (t != null)
                {
                    if (t.Item3.cache.ContainsKey(t.Item2))
                    {
                        try
                        {
                            t.Item1.EndReceive(result);
                        }
                        catch (Exception)
                        {
                            // ignore
                        }
                        finally
                        {
                            // TODO: resolve potential race condition with this cache entry being updated since ContainsKey was called. TFS 180717.
                            t.Item3.InvalidateEntry(t.Item2);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.GetLogger("SocketManager", Logger.LoggerType.Runtime).Error(ErrorCode.Messaging_Socket_ReceiveError, String.Format("ReceiveCallback: {0}",result), ex);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic"), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "s")]
        internal void ReturnSendingSocket(Socket s)
        {
            // Do nothing -- the socket will get cleaned up when it gets flushed from the cache
        }

        private static void FlushHandler(Object sender, LRU<IPEndPoint, Socket>.FlushEventArgs args)
        {
            if (args.Value != null)
            {
                CloseSocket(args.Value);
                NetworkingStatisticsGroup.OnClosedSendingSocket();
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal void InvalidateEntry(IPEndPoint target)
        {
            Socket socket;
            if (cache.RemoveKey(target, out socket))
            {
                CloseSocket(socket);
                NetworkingStatisticsGroup.OnClosedSendingSocket();
            }
            //Console.WriteLine("Invalidated entry for " + target.ToString());
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        // Note that this method assumes that there are no other threads accessing this object while this method runs.
        // Since this is true for the MessageCenter's use of this object, we don't lock around all calls to avoid the overhead.
        internal void Stop()
        {
            // Clear() on an LRU<> calls the flush handler on every item, so no need to manually close the sockets.
            cache.Clear();
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static void CloseSocket(Socket s)
        {
            if (s == null)
            {
                return;
            }

            try
            {
                s.Shutdown(SocketShutdown.Both);
            }
            catch (ObjectDisposedException)
            {
                // Socket is already closed -- we're done here
                return;
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Disconnect(false);
            }
            catch (Exception)
            {
                // Ignore
            }

            try
            {
                s.Close();
            }
            catch (Exception)
            {
                // Ignore
            }
        }
    }
}
