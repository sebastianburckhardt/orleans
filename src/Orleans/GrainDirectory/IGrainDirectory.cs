﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{

    /// <summary>
    /// Recursive distributed operations on grain directories.
    /// Each operation may forward the request to a remote owner, increasing the hopcount.
    /// 
    /// The methods here can be called remotely (where extended by IRemoteGrainDirectory) or
    /// locally (where extended by ILocalGrainDirectory)
    /// </summary>
    interface IGrainDirectory
    {
        /// <summary>
        /// Record a new grain activation by adding it to the directory.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the new activation.</param>
        /// <param name="singleActivation">If true, use single-activation registration</param>
        /// <param name="withRetry">Indicates whether or not to retry the operation.</param>
        /// <param name="hopcount">Counts recursion depth across silos</param>
        /// <returns>The registered address and the version associated with this directory mapping.</returns>
        Task<AddressAndTag> RegisterAsync(ActivationAddress address, bool singleActivation, int hopcount = 0);

        /// <summary>
        /// Removes the record for an existing activation from the directory service.
        /// This is used when an activation is being deleted.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to remove.</param>
        /// <param name="hopcount">Counts recursion depth across silos</param>
        /// <returns>An acknowledgement that the unregistration has completed.</returns>
        Task UnregisterAsync(ActivationAddress address, bool force = true, int hopcount = 0);

        /// <summary>
        /// Unregister a batch of addresses at once
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="addresses"></param>
        /// <param name="hopcount">Counts recursion depth across silos</param>
        /// <returns>An acknowledgement that the unregistration has completed.</returns>
        Task UnregisterManyAsync(List<ActivationAddress> addresses, int hopcount = 0);

        /// <summary>
        /// Removes all directory information about a grain.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="grain">The ID of the grain.</param>
        /// <param name="hopcount">Counts recursion depth across silos</param>
        /// <returns>An acknowledgement that the deletion has completed.
        /// It is safe to ignore this result.</returns>
        Task DeleteGrainAsync(GrainId grain, int hopcount = 0);

        /// <summary>
        /// Fetches complete directory information for a grain.
        /// If there is no local information, then this method will query the appropriate remote directory node.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <param name="hopcount">Counts recursion depth across silos</param>
        /// <returns>A list of all known activations of the grain, and the e-tag.</returns>
        Task<AddressesAndTag> LookupAsync(GrainId gid, int hopcount = 0);
    }


    [Serializable]
    internal struct AddressAndTag
    {
        public ActivationAddress Address;
        public int VersionTag;
    }
    

    [Serializable]
    internal struct AddressesAndTag 
    {
        public List<ActivationAddress> Addresses;
        public int VersionTag;
    }
}
