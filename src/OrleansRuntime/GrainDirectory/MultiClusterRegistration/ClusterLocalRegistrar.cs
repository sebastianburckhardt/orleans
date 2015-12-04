using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory
{
    /// <summary>
    /// The registrar for the Cluster-Local Registration Strategy.
    /// </summary>
    internal class ClusterLocalRegistrar : IGrainRegistrar
    {
        public GrainDirectoryPartition DirectoryPartition { get; private set; }

        public ClusterLocalRegistrar(GrainDirectoryPartition partition)
        {
            DirectoryPartition = partition;
        }

        public virtual Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address, bool singleActivation)
        {
            if (singleActivation)
            {
                var returnedAddress = DirectoryPartition.AddSingleActivation(address.Grain, address.Activation, address.Silo);
                return Task.FromResult(returnedAddress);
            }
            else
            {
                var etag = DirectoryPartition.AddActivation(address.Grain, address.Activation, address.Silo);
                return Task.FromResult(new Tuple<ActivationAddress,int>(address, etag));
            }
        }
  
        public virtual Task UnregisterAsync(ActivationAddress address, bool force)
        {
            DirectoryPartition.RemoveActivation(address.Grain, address.Activation, force);
            return TaskDone.Done;
        }

        public virtual Task DeleteAsync(GrainId gid)
        {
            DirectoryPartition.RemoveGrain(gid);
            return TaskDone.Done;
        }
    }
}
