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
        public LocalGrainDirectory Router { get; private set; }

        protected GrainRegistrarBase(LocalGrainDirectory router)
        {
            Router = router;
        }

        public abstract Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address);

        public virtual Task UnregisterAsync(ActivationAddress address, bool force)
        {
            // if I am the owner, remove the old activation locally
            Router.DirectoryPartition.RemoveActivation(address.Grain, address.Activation, force);
            return TaskDone.Done;
        }

        public Task DeleteAsync(GrainId gid)
        {
            Router.DirectoryPartition.RemoveGrain(gid);
            return TaskDone.Done;
        }
    }
}
