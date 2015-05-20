using System;
using System.Collections.Generic;

namespace Orleans
{
    [Serializable]
    internal class GraphPartitionPlacement : PlacementStrategy
    {
        internal static GraphPartitionPlacement Singleton { get; private set; }

        internal static void InitializeClass()
        {
            Singleton = new GraphPartitionPlacement();
        }

        private GraphPartitionPlacement()
        { }

        public override bool Equals(object obj)
        {
            return obj is GraphPartitionPlacement;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }
    }
}
