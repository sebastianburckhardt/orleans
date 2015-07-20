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

        public abstract Task<ActivationAddress> RegisterAsync(ActivationAddress address);
    }
}
