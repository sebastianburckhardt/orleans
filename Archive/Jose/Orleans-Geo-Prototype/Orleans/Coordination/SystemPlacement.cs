using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    [Serializable]
    internal class SystemPlacement : PlacementStrategy
    {
        internal static SystemPlacement Singleton { get; private set; }

        internal static void InitializeClass()
        {
            Singleton = new SystemPlacement();
        }

        private SystemPlacement()
        {}

        public override bool Equals(object obj)
        {
            return obj is SystemPlacement;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }
    }
}
