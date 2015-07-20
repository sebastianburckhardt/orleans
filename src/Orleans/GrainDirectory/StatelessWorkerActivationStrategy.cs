using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    [Serializable]
    internal class StatelessWorkerActivationStrategy : ActivationStrategy
    {
        private static StatelessWorkerActivationStrategy singleton;

        internal static StatelessWorkerActivationStrategy Singleton
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
            singleton = new StatelessWorkerActivationStrategy();
        }

        private StatelessWorkerActivationStrategy()
        { }

        public override bool Equals(object obj)
        {
            return obj is StatelessWorkerActivationStrategy;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode();
        }

        internal override bool IsSingleInstance()
        {
            return false;
        }
    }
}
