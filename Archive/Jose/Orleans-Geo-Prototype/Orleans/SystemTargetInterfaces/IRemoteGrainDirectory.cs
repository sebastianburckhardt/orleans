using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;



namespace Orleans
{
    internal interface IActivationInfo
    {
        SiloAddress SiloAddress { get; }
        DateTime TimeCreated { get; }
    }

    /// <summary>
    /// For internal use only.
    /// </summary>
    internal interface IGrainInfo
    {
        Dictionary<ActivationId, IActivationInfo> Instances { get; }
        int VersionTag { get; }
        bool SingleInstance { get; }
        void AddActivation(ActivationAddress addr);
        void AddActivation(ActivationId act, SiloAddress silo);
        ActivationAddress AddSingleActivation(GrainId grain, ActivationId act, SiloAddress silo);
        bool RemoveActivation(ActivationAddress addr);
        bool RemoveActivation(ActivationId act, bool force);
        bool Merge(GrainId grain, IGrainInfo other);
    }

    /// <summary>
    /// Per-silo system interface for managing the distributed, partitioned grain-silo-activation directory.
    /// </summary>
    internal interface IRemoteGrainDirectory : ISystemTarget
    {
        /// <summary>
        /// Record a new grain activation by adding it to the directory.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="address">The address of the new activation.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        Task Register(ActivationAddress address, int retries = 0);

        /// <summary>
        /// Records a bunch of new grain activations.
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="addresses"></param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        Task RegisterMany(List<ActivationAddress> addresses, int retries = 0);

        /// <summary>
        /// Registers a new activation, in single activation mode, with the directory service.
        /// If there is already an activation registered for this grain, then the new activation will
        /// not be registered and the address of the existing activation will be returned.
        /// Otherwise, the passed-in address will be returned.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="address">The address of the potential new activation.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns>The address registered for the grain's single activation.</returns>
        Task<ActivationAddress> RegisterSingleActivation(ActivationAddress address, int retries = 0);

        /// <summary>
        /// Registers multiple new activations, in single activation mode, with the directory service.
        /// If there is already an activation registered for any of the grains, then the corresponding new activation will
        /// not be registered.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="silo"></param>
        /// <param name="addresses"></param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        Task RegisterManySingleActivation(List<ActivationAddress> addresses, int retries = 0);

        /// <summary>
        /// Remove an activation from the directory.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="address">The address of the activation to unregister.</param>
        /// <param name="force">If true, then the entry is removed; if false, then the entry is removed only if it is
        /// sufficiently old.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns>Success</returns>
        Task Unregister(ActivationAddress address, bool force, int retries = 0);

        /// <summary>
        /// Removes all directory information about a grain.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="grain">The ID of the grain to look up.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        Task DeleteGrain(GrainId grain, int retries = 0);

        /// <summary>
        /// Fetch the list of the current activations for a grain along with the version number of the list.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="grain">The ID of the grain.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns></returns>
        Task<Tuple<List<Tuple<SiloAddress, ActivationId>>, int>> LookUp(GrainId grain, int retries = 0);

        /// <summary>
        /// Fetch the updated information on the given list of grains.
        /// This method should be called only remotely to refresh directory caches.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="grainAndETagList">list of grains and generation (version) numbers. The latter denote the versions of 
        /// the lists of activations currently held by the invoker of this method.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns>list of tuples holding a grain, generation number of the list of activations, and the list of activations. 
        /// If the generation number of the invoker matches the number of the destination, the list is null. If the destination does not
        /// hold the information on the grain, generation counter -1 is returned (and the list of activations is null)</returns>
        Task<List<Tuple<GrainId, int, List<Tuple<SiloAddress, ActivationId>>>>> LookUpMany(List<Tuple<GrainId, int>> grainAndETagList, int retries = 0);

        /// <summary>
        /// Registers replica of the directory partition from source silo on the destination silo.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="source">The address of the owner of the replica.</param>
        /// <param name="start">The first GrainId covered by this chunk; null if this is the first chunk</param>
        /// <param name="end">The last GrainId covered by this chunk; null if this is the last chunk</param>
        /// <param name="partition">The (full or partial) replica of the directory partition to be registered.
        /// TODO: this probably should be just a simple List&lt;KeyValuePair&lt;GrainId, IGrainInfo&gt;&gt;, or List&lt;Tuple...&gt;</param>
        /// <param name="isFullCopy">Flag specifying whether it is a full replica (and thus any old replica should be just replaced) or the
        /// a delta replica (and thus the old replica should be updated by delta changes) </param>
        /// <returns></returns>
        Task RegisterReplica(SiloAddress source, GrainId start, GrainId end, Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy);

        /// <summary>
        /// Unregisters replica of the directory partition from source silo on the destination silo.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="source">The address of the owner of the replica.</param>
        /// <returns></returns>
        Task UnregisterReplica(SiloAddress source);

        /// <summary>
        /// Unregister a block of addresses at once
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="activationAddresses"></param>
        /// <param name="retries"></param>
        /// <returns></returns>
        Task UnregisterMany(List<ActivationAddress> activationAddresses, int retries);
    }
}
