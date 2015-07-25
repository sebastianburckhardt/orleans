using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class GlobalSingleInstanceRegistrar : GrainRegistrarBase
    {
        public GlobalSingleInstanceRegistrar(GrainDirectoryPartition partition) : base(partition)
        {
        }

        public override Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address)
        {
            throw new NotImplementedException();            
        }
    }
}
