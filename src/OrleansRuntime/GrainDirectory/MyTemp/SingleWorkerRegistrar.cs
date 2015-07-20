using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class SingleInstanceRegistrar : GrainRegistrarBase
    {
        public SingleInstanceRegistrar(LocalGrainDirectory router) : base(router)
        {
            
        }

        public override async Task<ActivationAddress> RegisterAsync(ActivationAddress address)
        {
            SiloAddress owner = Router.CalculateTargetSilo(address.Grain);
            if (owner == null)
            {
                // We don't know about any other silos, and we're stopping, so throw
                throw new InvalidOperationException("Grain directory is stopping");
            }
            if (owner.Equals(Router.MyAddress))
            {
                Router.RegistrationsSingleActLocal.Increment();
                // if I am the owner, store the new activation locally
                Tuple<ActivationAddress, int> returnedAddress = Router.DirectoryPartition.AddSingleActivation(address.Grain, address.Activation, address.Silo);
                return returnedAddress == null ? null : returnedAddress.Item1;
            }
            else
            {
                Router.RegistrationsSingleActRemoteSent.Increment();
                // otherwise, notify the owner
                Tuple<ActivationAddress, int> returnedAddress = await Router.GetDirectoryReference(owner).RegisterSingleActivation(address, LocalGrainDirectory.NUM_RETRIES);

                // Caching optimization: 
                // cache the result of a successfull RegisterSingleActivation call, only if it is not a duplicate activation.
                // this way next local lookup will find this ActivationAddress in the cache and we will save a full lookup!
                if (returnedAddress == null || returnedAddress.Item1 == null) return null;

                if (!address.Equals(returnedAddress.Item1) || !Router.IsValidSilo(address.Silo)) return returnedAddress.Item1;

                var cached = new List<Tuple<SiloAddress, ActivationId>>(new[] { Tuple.Create(address.Silo, address.Activation) });
                // update the cache so next local lookup will find this ActivationAddress in the cache and we will save full lookup.
                Router.DirectoryCache.AddOrUpdate(address.Grain, cached, returnedAddress.Item2);
                return returnedAddress.Item1;
            }
        }
    }
}
