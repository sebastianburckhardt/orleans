using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans.Runtime.MembershipService
{
    internal class ConsistentRingProviderForGrains : IConsistentRingProviderForGrains, IRingRangeListener
    {
        private readonly IConsistentRingProvider ringProvider;
        private readonly List<IGrainRingRangeListener> grainStatusListeners;
        private readonly Logger logger;

        internal ConsistentRingProviderForGrains(IConsistentRingProvider ring)
        {
            ringProvider = ring;
            grainStatusListeners = new List<IGrainRingRangeListener>();
            ringProvider.SubscribeToRangeChangeEvents(this);
            logger = Logger.GetLogger("ConsistentRingProviderForGrains");
        }

        public IRingRange GetMyRange()
        {
            return ringProvider.GetMyRange();
        }

        public bool SubscribeToRangeChangeEvents(IGrainRingRangeListener observer)
        {
            lock (grainStatusListeners)
            {
                if (grainStatusListeners.Contains(observer))
                {
                    return false;
                }
                grainStatusListeners.Add(observer);
                return true;
            }
        }

        public bool UnSubscribeFromRangeChangeEvents(IGrainRingRangeListener observer)
        {
            lock (grainStatusListeners)
            {
                if (grainStatusListeners.Contains(observer))
                {
                    return grainStatusListeners.Remove(observer);
                }
                return false;
            }
        }

        public void RangeChangeNotification(IRingRange old, IRingRange now, bool increased)
        {
            logger.Info("-NotifyLocal GrainRangeSubscribers about old {0} new {1} increased? {2}", old, now, increased);
            List<IGrainRingRangeListener> copy;
            lock (grainStatusListeners)
            {
                copy = grainStatusListeners.ToList();
            }
            foreach (IGrainRingRangeListener listener in copy)
            {
                try
                {
                    Task task = listener.RangeChangeNotification(old, now, increased);
                    task.Ignore();
                }
                catch (Exception exc)
                {
                    logger.Error(ErrorCode.CRP_ForGrains_Local_Subscriber_Exception,
                        String.Format("Local IGrainRingRangeListener {0} has thrown an exception when was notified about RangeChangeNotification about old {1} new {2} increased? {3}",
                        listener.GetType().FullName, old, now, increased), exc);
                }
            }
        }
    }
}


