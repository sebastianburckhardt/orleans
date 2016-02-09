using Orleans.MultiCluster;
using System;
using System.Collections.Generic;

namespace Orleans.Runtime.MultiClusterNetwork
{
    class MultiClusterOracleData : IMultiClusterGossipData
    {
        private volatile MultiClusterData localData;  // immutable, can read without lock

        private readonly HashSet<GrainReference> confListeners;

        private readonly TraceLogger logger;

        internal MultiClusterData Current { get { return localData; } }

        internal MultiClusterOracleData(TraceLogger log)
        {
            logger = log;
            localData = new MultiClusterData();
            confListeners = new HashSet<GrainReference>();
        }

        internal bool SubscribeToMultiClusterConfigurationEvents(GrainReference observer)
        {
            if (logger.IsVerbose2)
                logger.Verbose2("SubscribeToMultiClusterConfigurationEvents: {0}", observer);

            if (confListeners.Contains(observer))
                return false;

            confListeners.Add(observer);
            return true;
        }

        internal bool UnSubscribeFromMultiClusterConfigurationEvents(GrainReference observer)
        {
            if (logger.IsVerbose3)
                logger.Verbose3("UnSubscribeFromMultiClusterConfigurationEvents: {0}", observer);

            return confListeners.Remove(observer);
        }


        public MultiClusterData ApplyDataAndNotify(MultiClusterData data)
        {
            if (data.IsEmpty)
                return data;

            MultiClusterData delta;
            MultiClusterData prev = localData;

            localData = prev.Merge(data, out delta);

            if (logger.IsVerbose2)
                logger.Verbose2("ApplyDataAndNotify: delta {0}", delta);

            if (delta.IsEmpty)
                return delta;

            if (delta.Configuration != null)
            {
                // notify configuration listeners of change
                foreach (var listener in confListeners)
                {
                    try
                    {
                        if (logger.IsVerbose2)
                            logger.Verbose2("-NotificationWork: notify IProtocolParticipant {0} of configuration {1}", listener, delta.Configuration);

                        // enqueue conf change event as grain call
                        var g = InsideRuntimeClient.Current.InternalGrainFactory.Cast<IProtocolParticipant>(listener);
                        g.OnMultiClusterConfigurationChange(delta.Configuration).Ignore();
                    }
                    catch (Exception exc)
                    {
                        logger.Error(ErrorCode.MultiClusterNetwork_LocalSubscriberException,
                            String.Format("IProtocolParticipant {0} threw exception processing configuration {1}",
                            listener, delta.Configuration), exc);
                    }
                }
            }

            return delta;
        }
    }
}
