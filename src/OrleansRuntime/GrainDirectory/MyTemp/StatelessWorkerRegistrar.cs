using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.GrainDirectory;

namespace Orleans.Runtime.GrainDirectory.MyTemp
{
    internal class StatelessWorkerRegistrar : GrainRegistrarBase
    {
        public StatelessWorkerRegistrar(LocalGrainDirectory router) : base(router)
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
                Router.RegistrationsLocal.Increment();
                // if I am the owner, store the new activation locally
                Router.DirectoryPartition.AddActivation(address.Grain, address.Activation, address.Silo);
            }
            else
            {
                Router.RegistrationsRemoteSent.Increment();
                // otherwise, notify the owner
                int eTag = await Router.GetDirectoryReference(owner).Register(address, LocalGrainDirectory.NUM_RETRIES);
                if (Router.IsValidSilo(address.Silo))
                {
                    // Caching optimization:
                    // cache the result of a successfull RegisterActivation call, only if it is not a duplicate activation.
                    // this way next local lookup will find this ActivationAddress in the cache and we will save a full lookup!
                    List<Tuple<SiloAddress, ActivationId>> cached;
                    if (!Router.DirectoryCache.LookUp(address.Grain, out cached))
                    {
                        cached = new List<Tuple<SiloAddress, ActivationId>>(1);
                    }
                    cached.Add(Tuple.Create(address.Silo, address.Activation));
                    // update the cache so next local lookup will find this ActivationAddress in the cache and we will save full lookup.
                    Router.DirectoryCache.AddOrUpdate(address.Grain, cached, eTag);
                }
            }
            return address;
        }
    }
}
