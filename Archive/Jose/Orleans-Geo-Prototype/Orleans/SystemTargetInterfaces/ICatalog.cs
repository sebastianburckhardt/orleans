using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;


namespace Orleans
{
    /// <summary>
    /// Remote interface to grain and activation state
    /// </summary>
    internal interface ICatalog : ISystemTarget
    {
        #region Grains

        /// <summary>
        /// Create a new system grain
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="grainId"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        Task CreateSystemGrain(GrainId grainId, string type);

        /// <summary>
        /// Delete grain information and activations from this silo.
        /// For internal use only.
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="grainIds"></param>
        /// <returns></returns>
        Task DeleteGrainsLocal(List<GrainId> grainIds);

        #endregion

        #region Activations

        /// <summary>
        /// Delete activations from this silo
        /// </summary>
        /// <param name="target"></param>
        /// <param name="activationAddresses"></param>
        /// <returns></returns>
        Task DeleteActivationsLocal(List<ActivationAddress> activationAddresses);

        Task InvalidatePartitionCache(ActivationAddress activationAddress);

        #endregion
    }
}
