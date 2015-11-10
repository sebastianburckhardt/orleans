using Orleans.MultiCluster;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.MultiClusterNetwork
{
    class MultiClusterOracleData : IMultiClusterGossipData
    {
        private volatile MultiClusterData localdata;  // immutable, can read without lock

        private readonly HashSet<GrainReference> confListeners;

        private readonly TraceLogger logger;
        private BackgroundWorker notificationworker;
        private List<MultiClusterConfiguration> conf_changes;

        internal MultiClusterData Current { get { return localdata; } }

        internal MultiClusterOracleData(TraceLogger log)
        {
            logger = log;
            localdata = new MultiClusterData();
            confListeners = new HashSet<GrainReference>();
            conf_changes = new List<MultiClusterConfiguration>();

            notificationworker = new BackgroundWorker(() => NotificationWork());
        }

        private async Task NotificationWork()
        {
            if (conf_changes.Count == 0)
                return; // nothing to do

            // take the list of deltas we need to send to the listeners
            var conflist = conf_changes;
            conf_changes = new List<MultiClusterConfiguration>();

            if (logger.IsVerbose2)
                logger.Verbose2("Configuration Notifications: {0} updates", conflist.Count);

            // do the listener notifications on threadpool so as to not starve multicluster oracle when there are many
            await Task.Run(() =>
            {
                lock (confListeners) // must not race with add/remove of status listeners
                {
                    foreach (var listener in confListeners)
                        foreach (var conf in conflist)
                        {
                            try
                            {
                                if (logger.IsVerbose3)
                                    logger.Verbose3("-NotificationWork: notify IProtocolParticipant {0} of configuration {1}", listener, conf);

                                // enqueue event as grain call
                                var g = InsideRuntimeClient.Current.InternalGrainFactory.Cast<IProtocolParticipant>(listener);
                                g.OnMultiClusterConfigurationChange(conf).Ignore();
                                //TODO advertise completion of all notifications in multicluster network
                            }
                            catch (Exception exc)
                            {
                                logger.Error(ErrorCode.MultiClusterNetwork_LocalSubscriberException,
                                    String.Format("IProtocolParticipant {0} threw exception processing configuration {1}",
                                    listener, conf), exc);
                            }
                        }
                }
            });
        }


        internal bool SubscribeToMultiClusterConfigurationEvents(GrainReference observer)
        {
            if (logger.IsVerbose3)
                logger.Verbose3("SubscribeToMultiClusterConfigurationEvents: {0}", observer);

            lock (confListeners)
            {
                if (confListeners.Contains(observer))
                    return false;

                confListeners.Add(observer);
                return true;
            }
        }

        internal bool UnSubscribeFromMultiClusterConfigurationEvents(GrainReference observer)
        {
            if (logger.IsVerbose3)
                logger.Verbose3("UnSubscribeFromMultiClusterConfigurationEvents: {0}", observer);

            lock (confListeners)
            {
                return confListeners.Remove(observer);
            }
        }


        public MultiClusterData ApplyDataAndNotify(MultiClusterData data)
        {
            if (data.IsEmpty)
                return data;

            MultiClusterData delta;
            MultiClusterData prev = localdata;

            localdata = prev.Merge(data, out delta);

            if (logger.IsVerbose2)
                logger.Verbose2("ApplyDataAndNotify: delta {0}", delta);

            if (delta.IsEmpty)
                return delta;

            if (delta.Configuration != null)
                conf_changes.Add(delta.Configuration);

            notificationworker.Notify();

            return delta;
        }



    }

}
