using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GossipNetwork
{
     class GossipOracleData
    {
        private volatile GossipData gossipdata;  // immutable, can read without lock
         
        private readonly List<IGossipListener> statusListeners;
        private readonly TraceLogger logger;

        internal GossipData Current { get { return gossipdata; } }

        internal GossipOracleData(TraceLogger log)
        {
            logger = log;
            gossipdata = new GossipData();
            statusListeners = new List<IGossipListener>();
        }

        internal bool SubscribeToGossipEvents(IGossipListener observer)
        {
            lock (statusListeners)
            {
                if (statusListeners.Contains(observer))
                    return false;

                statusListeners.Add(observer);
                return true;
            }
        }

        internal bool UnSubscribeFromGossipEvents(IGossipListener observer)
        {
            lock (statusListeners)
            {
                return statusListeners.Contains(observer) && statusListeners.Remove(observer);
            }
        }

        public void ApplyGossipDataAndNotify(GossipData data)
        {
            lock (statusListeners) // process and notify one change at a time
            {
                GossipData delta;
                gossipdata = gossipdata.Merge(data, out delta);
                if (!delta.IsEmpty)
                {
                    if (logger.IsVerbose2) logger.Verbose2("-NotifyLocalSubscribers: nonempty delta");
                    foreach (var listener in statusListeners)
                        try
                        {
                            listener.GossipNotification(delta);
                        }
                        catch (Exception exc)
                        {
                            logger.Error(ErrorCode.MembershipLocalSubscriberException,
                                String.Format("Local ISiloStatusListener {0} has thrown an exception when was notified about SiloStatusChangeNotification",
                                listener.GetType().FullName), exc);
                        }
                }
            }
        }

    
    }

}
