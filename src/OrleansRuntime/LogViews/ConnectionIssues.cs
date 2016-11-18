using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.LogViews;

namespace Orleans.Runtime.LogViews
{


    [Serializable]
    public class NotificationFailed : ConnectionIssue
    {
        public string RemoteCluster { get; set; }
        public Exception Exception { get; set; }

        public override TimeSpan ComputeRetryDelay(TimeSpan? previous)
        {
            if (NumberOfConsecutiveFailures < 3) return TimeSpan.FromMilliseconds(1);
            else if (NumberOfConsecutiveFailures < 1000) return TimeSpan.FromSeconds(30);
            else return TimeSpan.FromMinutes(1);
        }
    }

    [Serializable]
    public class PrimaryOperationFailed : ConnectionIssue
    {
        public Exception Exception { get; set; }

        public override TimeSpan ComputeRetryDelay(TimeSpan? previous)
        {
            // after first fail do not backoff yet... keep it at zero
            if (previous == null)
            {
                return TimeSpan.Zero;
            }

            var backoff = previous.Value.TotalMilliseconds;

            if (random == null)
                random = new Random();

            // grows exponentially up to slowpoll interval
            if (previous.Value.TotalMilliseconds < slowpollinterval)
                backoff = (int)((backoff + random.Next(5, 15)) * 1.5);

            // during slowpoll, slightly randomize
            if (backoff > slowpollinterval)
                backoff = slowpollinterval + random.Next(1, 200);

            return TimeSpan.FromMilliseconds(backoff);
        }


        [ThreadStatic]
        static Random random;

        private const int slowpollinterval = 10000;
    }








}
