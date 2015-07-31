using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal abstract class GrainRegistrarBase : IGrainRegistrar
    {
        public GrainDirectoryPartition DirectoryPartition { get; private set; }

        protected GrainRegistrarBase(GrainDirectoryPartition partition)
        {
            DirectoryPartition = partition;
        }

        public abstract Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address);

        public virtual Task UnregisterAsync(ActivationAddress address, bool force)
        {
            // if I am the owner, remove the old activation locally
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
