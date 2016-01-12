using System;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{
    /// <summary>
    /// A grain registrar takes responsibility of coordinating the registration of a grains,
    /// possibly involving multiple clusters. 
    /// The grain registrar is called only on the silo that is the owner for that grain.
    /// </summary>
    interface IGrainRegistrar
    {
        /// <summary>
        /// Registers a new activation with the directory service.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to register.</param>
        /// <param name="singleActivation">If true, use single-activation registration</param>
        /// <returns>The address registered for the grain's single activation.</returns>
        Task<AddressAndTag> RegisterAsync(ActivationAddress address, bool singleActivation);

        /// <summary>
        /// Removes the given activation for the grain.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address"></param>
        /// <param name="force"></param>
        /// <returns></returns>
        Task UnregisterAsync(ActivationAddress address, bool force);

        /// <summary>
        /// Deletes the grain activation.
        /// </summary>
        /// <returns></returns>
        Task DeleteAsync(GrainId gid);
    }
}
