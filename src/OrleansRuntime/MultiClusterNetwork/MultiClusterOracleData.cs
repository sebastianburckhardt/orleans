using Orleans.MultiCluster;
using Orleans.Replication;
using Orleans.Runtime.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.MultiClusterNetwork
{
    class MultiClusterOracleData
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
                logger.Verbose2("-NotificationWork: {0} nonempty deltas", conflist.Count);

            // do the listener notifications on threadpool so as to not starve multicluster oracle when there are many
            await Task.Run(() =>
            {
                lock (confListeners) // must not race with add/remove of status listeners
                {
                    foreach (var listener in confListeners)
                        try
                        {
                            foreach (var conf in conflist)
                            {
                                if (logger.IsVerbose3)
                                    logger.Verbose3("-NotificationWork: notify {0}", listener.GetType().FullName);

                                // enqueue event as grain call
                                var g = InsideRuntimeClient.Current.InternalGrainFactory.Cast<IReplicationProtocolParticipant>(listener);
                                g.OnMultiClusterConfigurationChange(conf).Ignore();
                            }
                        }
                        catch (Exception exc)
                        {
                            logger.Error(ErrorCode.MultiClusterNetwork_LocalSubscriberException,
                                String.Format("Local IReplicationProtocolParticipant {0} has thrown an exception",
                                listener.GetType().FullName), exc);
                        }
                }
            });
        }


        internal bool SubscribeToMultiClusterConfigurationEvents(GrainReference observer)
        {
            if (logger.IsVerbose3)
                logger.Verbose3("-SubscribeToMultiClusterConfigurationEvents: {0}", observer.GetType().FullName);

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
                logger.Verbose3("-UnSubscribeFromMultiClusterConfigurationEvents: {0}", observer.GetType().FullName);

            lock (confListeners)
            {
                return confListeners.Remove(observer);
            }
        }


        public bool ApplyIncomingDataAndNotify(MultiClusterData data)
        {
            if (data.IsEmpty)
                return false;

            MultiClusterData delta;
            MultiClusterData prev = localdata;

            localdata = prev.Merge(data, out delta);

            if (logger.IsVerbose2)
                logger.Verbose2("-ApplyIncomingDataAndNotify: delta {0}", delta.ToString());

            if (delta.IsEmpty)
                return false;

            if (delta.Configuration != null)
                conf_changes.Add(delta.Configuration);

            notificationworker.Notify();

            return true;
        }



    }

}
