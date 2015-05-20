using System;


namespace Orleans
{
    [Serializable]
    internal abstract class PlacementStrategy
    {
        internal static void Initialize()
        {
            RandomPlacement.InitializeClass();
            PreferLocalPlacement.InitializeClass();
            LoadAwarePlacement.InitializeClass();
            ExplicitPlacement.InitializeClass();
            SystemPlacement.InitializeClass();
            GraphPartitionPlacement.InitializeClass();
            LocalPlacement.InitializeClass(NodeConfiguration.DEFAULT_MAX_LOCAL_ACTIVATIONS);
        }


        internal static void Initialize(NodeConfiguration nodeConfig)
        {
            Initialize();
            GrainStrategy.InitDefaultGrainStrategies(nodeConfig);
        }

        internal static PlacementStrategy GetDefault()
        {
            return RandomPlacement.Singleton;
        }

        public override string ToString()
        {
            throw new NotImplementedException();
        }
    }
}