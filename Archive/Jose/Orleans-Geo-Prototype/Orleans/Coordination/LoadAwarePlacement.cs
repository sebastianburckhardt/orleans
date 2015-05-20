using System;

namespace Orleans
{
    [Serializable]
    internal class LoadAwarePlacement : PlacementStrategy
    {
        internal static LoadAwarePlacement Singleton { get; private set; }

        private LoadAwarePlacement()
        {}

        internal static void InitializeClass()
        {
            Singleton = new LoadAwarePlacement();
        }

        public override bool Equals(object obj)
        {
            return obj is LoadAwarePlacement;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }
    }
}
