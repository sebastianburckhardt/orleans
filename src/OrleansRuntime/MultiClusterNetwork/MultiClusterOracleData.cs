using Orleans.MultiCluster;
using System;
using System.Collections.Generic;

namespace Orleans.Runtime.MultiClusterNetwork
{
    class MultiClusterOracleData 
    {
        private volatile MultiClusterData localData;  // immutable, can read without lock

        private readonly TraceLogger logger;

        internal MultiClusterData Current { get { return localData; } }

        internal MultiClusterOracleData(TraceLogger log)
        {
            logger = log;
            localData = new MultiClusterData();
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
                // code will be added in separate PR
            }

            return delta;
        }
    }
}
