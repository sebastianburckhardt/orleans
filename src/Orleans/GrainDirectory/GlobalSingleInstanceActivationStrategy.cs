using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    [Serializable]
    internal class GlobalSingleInstanceActivationStrategy : ActivationStrategy
    {

        private static GlobalSingleInstanceActivationStrategy singleton;

        internal static GlobalSingleInstanceActivationStrategy Singleton
        {
            get
            {
                if (singleton == null)
                {
                    Initialize();
                }
                return singleton;
            }
        }

        internal static void Initialize()
        {
            singleton = new GlobalSingleInstanceActivationStrategy();
        }

        private GlobalSingleInstanceActivationStrategy()
        { }

        public override bool Equals(object obj)
        {
            return obj is SingleInstanceActivationStrategy;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }

        internal override bool IsSingleInstance()
        {
            return true;
        }
    }
}
