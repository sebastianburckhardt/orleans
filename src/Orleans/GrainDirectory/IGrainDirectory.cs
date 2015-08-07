using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{
    /// <summary>
    /// An Activation can be in one of three states:
    /// OWNED means that the directory which contains the address is the definitive reference for the activation.
    /// TRY_OWN means that the directory which contains the address is _trying_ to be the definitive reference.
    /// CACHED means that the directory contains a cached copy of the activation address, but that it is not the owner.
    /// </summary>
    internal enum ActivationStatus
    {
        OWNED,                      // An activation in state OWNED is definitively owned by a silo.
        //DOUBTFUL,                   // Failed to contact one or more clusters while registering, so may be a duplicate. This state is not required.

        REQUESTED_OWNERSHIP,        // The silo is in the process of trying to create a grain's activation.
        CACHED,                     // The activation reference is cached.
        RACE_LOSER,                 // The activation lost a race condition.
    }

    interface IGrainDirectory
    {
        /// <summary>
        /// Record a new grain activation by adding it to the directory.
        /// </summary>
        /// <param name="address">The address of the new activation.</param>
        /// <param name="withRetry">Indicates whether or not to retry the operation.</param>
        /// <returns>The registered address and the version associated with this directory mapping.</returns>
        Task<Tuple<ActivationAddress, int>> RegisterAsync(ActivationAddress address, bool withRetry = true);

        /// <summary>
        /// Removes the record for an existing activation from the directory service.
        /// This is used when an activation is being deleted.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        /// <returns>whether the operation was successfully applied locally. Returns false if this silo is not the owner anymore.</returns>
        Task<bool> UnregisterAsync(ActivationAddress address, bool force = true, bool withRetry = true);

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
        Task<bool> DeleteGrainAsync(GrainId grain, bool withRetry = true);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="gid"></param>
        /// <param name="fullLookup"></param>
        /// <returns></returns>
        Task<Tuple<List<ActivationAddress>, int>> LookUpActivationAsync(GrainId gid, bool fullLookup = true);
    }
}
