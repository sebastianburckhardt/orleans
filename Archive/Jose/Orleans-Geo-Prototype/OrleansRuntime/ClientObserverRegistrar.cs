using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;




namespace Orleans.Runtime
{
    internal class ClientObserverRegistrar : SystemTarget, IClientObserverRegistrar
    {
        private readonly ILocalGrainDirectory grainDirectory;
        private readonly ISiloMessageCenter localMessageCenter;
        private readonly SiloAddress myAddress;

        internal ClientObserverRegistrar(SiloAddress myAddr, ISiloMessageCenter mc, ILocalGrainDirectory dir)
            : base(Constants.ClientObserverRegistrarId, myAddr)
        {
            grainDirectory = dir;
            localMessageCenter = mc;
            myAddress = myAddr;
        }

        #region IClientGateway Members

        /// <summary>
        /// Registers a client object on this gateway.
        /// </summary>
        public async Task<ActivationAddress> RegisterClientObserver(GrainId id, Guid clientId)
        {
            localMessageCenter.RecordProxiedGrain(id, clientId);
            var location = myAddress;
            var addr = ActivationAddress.NewActivationAddress(location, id);
            await grainDirectory.RegisterAsync(addr);
            return addr;
        }

        public async Task UnregisterClientObserver(ActivationAddress target)
        {
            if (localMessageCenter.IsProxying)
            {
                localMessageCenter.RecordUnproxiedGrain(target.Grain);
            }
            await grainDirectory.UnregisterAsync(target);
        }

        /// <summary>
        /// Unregisters client object from all gateways.
        /// </summary>
        public async Task UnregisterClientObserver(GrainId target)
        {
            if (localMessageCenter.IsProxying)
            {
                localMessageCenter.RecordUnproxiedGrain(target);
            }
            await grainDirectory.DeleteGrain(target);
        }

        #endregion
    }
}


