using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{
    interface IGrainDirectory
    {
        /// <summary>
        /// Record a new grain activation by adding it to the directory.
        /// </summary>
        /// <param name="address">The address of the new activation.</param>
        /// <param name="withRetry">Indicates whether or not to retry the operation.</param>
        /// <returns>The registered address and the version associated with this directory mapping.</returns>
        Task<Tuple<ActivationAddress, int>> Register(ActivationAddress address, bool withRetry = true);

        /// <summary>
        /// Removes the record for an existing activation from the directory service.
        /// This is used when an activation is being deleted.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        /// <returns>whether the operation was successfully applied locally. Returns false if this silo is not the owner anymore.</returns>
        Task<bool> Unregister(ActivationAddress address, bool force = true, bool withRetry = true);

        /// <summary>
        /// Unregister a batch of addresses at once
        /// </summary>
        /// <param name="addresses"></param>
        /// <returns>A list of activation address that did not belong to this silo and hence not unregistered.</returns>
        Task<List<ActivationAddress>> UnregisterManyAsync(List<ActivationAddress> addresses, bool withRetry = true);

        /// <summary>
        /// Removes all directory information about a grain.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <returns>An acknowledgement that the deletion has completed.
        /// It is safe to ignore this result.</returns>
        Task<bool> DeleteGrain(GrainId grain, bool withRetry = true);
    }
}
