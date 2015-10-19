using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.MultiCluster
{
    /// <summary>
    /// Multicluster configuration, as injected by user, and stored/transmitted in the multicluster network.
    /// </summary>
    [Serializable]
    public class MultiClusterConfiguration : IEquatable<MultiClusterConfiguration>
    {
        /// <summary>
        /// The UTC timestamp of this configuration. 
        /// New configurations are injected by administrator.
        /// Newer configurations automatically replace older ones in the multicluster network.
        /// </summary>
        public DateTime AdminTimestamp { get; private set; }

        /// <summary>
        /// List of clusters that are joined to the multicluster.
        /// </summary>
        public IReadOnlyList<string> Clusters { get; private set; }

        /// <summary>
        /// A comment included by the administrator.
        /// </summary>
        public string Comment { get; private set; }


        public MultiClusterConfiguration(DateTime Timestamp, IReadOnlyList<string> Clusters, string Comment = "")
        {
            System.Diagnostics.Debug.Assert(Clusters != null);
            this.AdminTimestamp = Timestamp;
            this.Clusters = Clusters;
            this.Comment = Comment;
        }

        public override string ToString()
        {
            return string.Format("{0} [{1}] {2}",
                AdminTimestamp, string.Join(",", Clusters), Comment
            );
        }

        public static bool OlderThan(MultiClusterConfiguration a, MultiClusterConfiguration b)
        {
            if (a == null)
                return b != null;
            else
                return b != null && a.AdminTimestamp < b.AdminTimestamp;
        }

        public bool Equals(MultiClusterConfiguration other)
        {
            if (!AdminTimestamp.Equals(other.AdminTimestamp)
                || Clusters.Count != other.Clusters.Count)
                return false;

            for (int i = 0; i < Clusters.Count; i++)
                if (Clusters[i] != other.Clusters[i])
                    return false;

            if (Comment != other.Comment)
                return false;

            return true;
        }

    }
}