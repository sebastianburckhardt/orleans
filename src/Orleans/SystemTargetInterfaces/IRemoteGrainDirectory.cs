/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans.GrainDirectory;


namespace Orleans.Runtime
{
    internal interface IActivationInfo
    {
        SiloAddress SiloAddress { get; }
        DateTime TimeCreated { get; }
        MultiClusterStatus RegistrationStatus { get; set; }
    }

    internal interface IGrainInfo
    {
        Dictionary<ActivationId, IActivationInfo> Instances { get; }
        int VersionTag { get; }
        bool SingleInstance { get; }
        bool AddActivation(ActivationId act, SiloAddress silo);
        ActivationAddress AddSingleActivation(GrainId grain, ActivationId act, SiloAddress silo, MultiClusterStatus registrationStatus = MultiClusterStatus.OWNED);
        bool RemoveActivation(ActivationAddress addr);
        bool RemoveActivation(ActivationId act, bool force);
        bool Merge(GrainId grain, IGrainInfo other);
        void CacheOrUpdateRemoteClusterRegistration(GrainId grain, ActivationId oldActivation, ActivationId activation, SiloAddress silo);
        bool UpdateClusterRegistrationStatus(ActivationId activationId, MultiClusterStatus registrationStatus, MultiClusterStatus? comparewith = null);
    }

    /// <summary>
    /// Per-silo system interface for managing the distributed, partitioned grain-silo-activation directory.
    /// </summary>
    internal interface IRemoteGrainDirectory : ISystemTarget, IGrainDirectory
    {        
        /// <summary>
        /// Records a bunch of new grain activations.
        /// This method should be called only remotely during handoff.
        /// </summary>
        /// <param name="addresses">The addresses of the grains to register</param>
        /// <param name="singleactivation">whether to use single-activation registration</param>
        /// <returns></returns>
        Task RegisterMany(List<ActivationAddress> addresses, bool singleactivation);

        /// <summary>
        /// Fetch the updated information on the given list of grains.
        /// This method should be called only remotely to refresh directory caches.
        /// </summary>
        /// <param name="grainAndETagList">list of grains and generation (version) numbers. The latter denote the versions of 
        /// the lists of activations currently held by the invoker of this method.</param>
        /// <param name="retries">Number of retries to execute the method in case the virtual ring (servers) changes.</param>
        /// <returns>list of tuples holding a grain, generation number of the list of activations, and the list of activations. 
        /// If the generation number of the invoker matches the number of the destination, the list is null. If the destination does not
        /// hold the information on the grain, generation counter -1 is returned (and the list of activations is null)</returns>
        Task<List<Tuple<GrainId, int, List<ActivationAddress>>>> LookUpMany(List<Tuple<GrainId, int>> grainAndETagList);

        /// <summary>
        /// Handoffs the the directory partition from source silo to the destination silo.
        /// </summary>
        /// <param name="source">The address of the owner of the partition.</param>
        /// <param name="partition">The (full or partial) copy of the directory partition to be Haded off.</param>
        /// <param name="isFullCopy">Flag specifying whether it is a full copy of the directory partition (and thus any old copy should be just replaced) or the
        /// a delta copy (and thus the old copy should be updated by delta changes) </param>
        /// <returns></returns>
        Task AcceptHandoffPartition(SiloAddress source, Dictionary<GrainId, IGrainInfo> partition, bool isFullCopy);

        /// <summary>
        /// Removes the handed off directory partition from source silo on the destination silo.
        /// </summary>
        /// <param name="source">The address of the owner of the partition.</param>
        /// <returns></returns>
        Task RemoveHandoffPartition(SiloAddress source);
    }
}
