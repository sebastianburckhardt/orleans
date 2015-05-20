using System;


namespace Orleans
{
    /// <summary>
    /// Abstract marker class for configuration information associated with a cell
    /// </summary>
    [Serializable]
    public class Strategy
    {
        /// <summary>
        /// Generates a human-readable string description of this strategy.
        /// </summary>
        /// <returns>A string containing the description. By default, this is the short name of the implementing type.</returns>
        public override string ToString()
        {
            return GetType().Name;
        }
    }

    /// <summary>
    /// Strategy that applies to an individual grain
    /// </summary>
    [Serializable]
    internal abstract class GrainStrategy : Strategy
    {
        /// <summary>
        /// Placement strategy that indicates that new activations of this grain type should be placed randomly,
        /// subject to the overall placement policy.
        /// </summary>
        public static PlacementStrategy RandomPlacement;
        /// <summary>
        /// Placement strategy that indicates that new activations of this grain type should be placed on a local silo.
        /// </summary>
        public static PlacementStrategy PreferLocalPlacement;
        /// <summary>
        /// Placement strategy that indicates that new activations of this grain type should be placed randomly,
        /// subject to the current load distribution across the deployment.
        /// </summary>
        public static PlacementStrategy LoadAwarePlacement;
        /// <summary>
        /// Use a local activation, create if not present
        /// </summary>
        public static PlacementStrategy LocalPlacement;
        /// <summary>
        /// Use a local activation, create if not present or all busy
        /// </summary>
        public static PlacementStrategy LocalPlacementAvailable;
        /// <summary>
        /// Use a graph partitioning algorithm
        /// </summary>
        internal static PlacementStrategy GraphPartitionPlacement;
        /// <summary>
        /// Use a local activation, ensure there is always at least one inactive to copy state
        /// </summary>
        public static PlacementStrategy LocalPlacementAvailableSpare;

        internal static void InitDefaultGrainStrategies(NodeConfiguration nodeConfig)
        {
            RandomPlacement = Orleans.RandomPlacement.Singleton;

            PreferLocalPlacement = Orleans.PreferLocalPlacement.Singleton;

            LoadAwarePlacement = Orleans.LoadAwarePlacement.Singleton;

            LocalPlacement = new LocalPlacement(-1);

            LocalPlacementAvailable = new LocalPlacement(0);

            LocalPlacementAvailableSpare = new LocalPlacement(1);

            GraphPartitionPlacement = Orleans.GraphPartitionPlacement.Singleton;
        }
    }
}
