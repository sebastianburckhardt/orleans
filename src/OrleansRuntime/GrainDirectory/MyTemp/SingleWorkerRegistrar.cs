﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class SingleInstanceRegistrar : GrainRegistrarBase
    {
        public SingleInstanceRegistrar(GrainDirectoryPartition partition) : base(partition)
        { }

        public override async Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address)
        {
            //assume you are the owner and perform local operation.
            var returnedAddress = DirectoryPartition.AddSingleActivation(address.Grain, address.Activation, address.Silo);

            return returnedAddress;
        }
    }
}
