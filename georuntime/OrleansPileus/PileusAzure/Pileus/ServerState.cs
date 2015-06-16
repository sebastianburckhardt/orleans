using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Records information about a server that holds object replicas.
    /// Note that replicas of different containers will need different instances of server state, 
    /// even if they are stored on the same server/site, since the high timestamps may differ.
    /// </summary>
    [Serializable()]
    public class ServerState
    {
        private string server;      // name of server
        private bool isPrimary;     // whether this is the primary
        private int rank;           // relative value of this server compared to others
        private DateTimeOffset highTime;  // timestamp of last update received from primary
        private DateTimeOffset lowTime;   // timestamp of last discarded version
        private DateTimeOffset updated;   // when this state was recorded
        private LatencyDistribution rttDist;  // a set of latency values for this server

        /// <summary>
        /// Constructs a new server record.
        /// </summary>
        /// <param name="server">Name of the server</param>
        /// <param name="isPrimary">Whether this is the primary server</param>
        /// <param name="priority">Number used to order this server relative to others (where lower rank is better)</param>
        public ServerState(string server, bool isPrimary, int rank)
        {
            this.server = server;
            this.isPrimary = isPrimary;
            this.rank = rank;
            if (isPrimary == false)
            {
                this.highTime = new DateTime(0);
                this.updated = DateTimeOffset.Now;
            }
            else
            {
                // Primary is always up to date to serve any data item
                this.highTime = DateTimeOffset.Now;
                this.updated = this.highTime;
            }
            this.lowTime = new DateTime(0);
            this.rttDist = new LatencyDistribution(0, 10000, 50);
        }

        /// <summary>
        /// Get the name of the server.
        /// </summary>
        public string Name
        {
            get { return server; }
        }

        /// <summary>
        /// Get whether this server is the primary.
        /// This is only a hint; the true set of primaries should be obtained from the configuration.
        /// </summary>
        public bool IsPrimary
        {
            get { return isPrimary; }
            set { isPrimary = value; }
        }

        /// <summary>
        /// Get and set the server's rank relative to other servers.
        /// Ranks are arbitrary numbers used to order the servers.
        /// For example, servers could be assigned ranks from 1 to N, 
        /// or ranks could based on the servers' relative access latencies.
        /// Lower ranks are better, i.e. a server with rank 1 is preferred.
        /// </summary>
        public int Rank
        {
            get { return rank; }
            set { rank = value; updated = DateTimeOffset.Now; }
        }

        /// <summary>
        /// Get and set the server's high timestamp, i.e. how up-to-date the server is.
        /// The assumption is that servers receive updates in timestamp order,
        /// and so a single timestamp is sufficient to record the set of updates it has received.
        /// </summary>
        public DateTimeOffset HighTime
        {
            get { return highTime; }
            set { highTime = value; updated = DateTimeOffset.Now; }
        }

        /// <summary>
        /// Get and set the server's low timestamp.
        /// This is the highest timestamp of any version that the server has discarded.
        /// The assumption is that all versions with lower timestamps have also been discarded
        /// unless such versions are the latest version of some object.
        /// </summary>
        public DateTimeOffset LowTime
        {
            get { return lowTime; }
            set { lowTime = value; updated = DateTimeOffset.Now; }
        }

        /// <summary>
        /// Gets the time when this record was created or updated.
        /// This could be used, for instance, to determine how long ago the server's high time was recorded.
        /// </summary>
        public DateTimeOffset Updated
        {
            get { return updated; }
        }

        /// <summary>
        /// Returns the distribution of round-trip times to this server.
        /// </summary>
        public LatencyDistribution RTTs
        {
            get { return rttDist;  }
        }

        public double AverageRTT
        {
            get { return this.rttDist.FindAverage(); }
        }

        public void AddRtt(long t)
        {
            rttDist.Add(t);
            updated = DateTimeOffset.Now;
        }
        
        public float FindProbabilityOfRttLessThan(long t, bool optimistic=true)
        {
            float ret = rttDist.ProbabilityOfFindingValueLessThanGiven(t, optimistic);
            return ret;
        }

        /// <summary>
        /// Return false if the server has not been contacted more than 5 times or if it has not been contacted recently. 
        /// </summary>
        /// <returns></returns>
        public bool IsContacted()
        {
            bool result = rttDist.GetTotalEntries() > 5;
            
            // consider the server uncontacted if its state has not been updated in the past 3 minutes
            if (updated + TimeSpan.FromMinutes(3) < DateTimeOffset.Now)
            {
                result = false;
            }
            
            return result;
        }
    }
}
