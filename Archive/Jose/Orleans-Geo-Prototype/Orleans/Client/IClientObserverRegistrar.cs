using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;



namespace Orleans
{
    /// <summary>
    /// Client gateway interface for forwarding client requests to silos.
    /// </summary>
    internal interface IClientObserverRegistrar : ISystemTarget
    {
       /// <summary>
        /// Registers a client observer object on this gateway.
        /// </summary>
        Task<ActivationAddress> RegisterClientObserver(GrainId id, Guid clientId);

        /// <summary>
        /// Unregisters client observer object.
        /// </summary>
        Task UnregisterClientObserver(ActivationAddress target);

        /// <summary>
        /// Unregisters client observer object from all gateways.
        /// </summary>
        Task UnregisterClientObserver(GrainId target);
    }
}
