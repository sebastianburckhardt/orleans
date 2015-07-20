using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    [Serializable]
    internal class SingleInstanceActivationStrategy : ActivationStrategy
    {
        private static SingleInstanceActivationStrategy singleton;

        internal static SingleInstanceActivationStrategy Singleton
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
            singleton = new SingleInstanceActivationStrategy();
        }

        private SingleInstanceActivationStrategy()
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
