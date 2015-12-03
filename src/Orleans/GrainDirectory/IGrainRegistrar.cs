using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
        /// <returns>The address registered for the grain's single activation.</returns>
        Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address, bool singleact);

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
