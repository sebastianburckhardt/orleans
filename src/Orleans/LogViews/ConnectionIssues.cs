using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
 
    [Serializable]
    public abstract class ConnectionIssue
    {
        public DateTime TimeStamp { get; set; }

        public DateTime TimeOfFirstFailure { get; set; }

        public int NumberOfConsecutiveFailures { get; set; }

        public TimeSpan RetryDelay { get; set; }

        public abstract TimeSpan ComputeRetryDelay(TimeSpan? previous);
    }


  
    [Serializable]
    public abstract class NotificationFailed : ConnectionIssue
    {
        public string RemoteClusterId { get; set; }

        public Exception Exception { get; set; }
    }



}