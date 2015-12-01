using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory
{
    /// <summary>
    /// A multi-cluster registration strategy where each cluster has 
    /// its own independent directory. This is the default.
    /// </summary>
    [Serializable]
    internal class ClusterLocalRegistration : MultiClusterRegistrationStrategy
    {
        private static ClusterLocalRegistration singleton;

        internal static ClusterLocalRegistration Singleton
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
            singleton = new ClusterLocalRegistration();
        }

        private ClusterLocalRegistration()
        { }

        public override bool Equals(object obj)
        {
            return obj is ClusterLocalRegistration;
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
