using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans
{
    // a markup interface just to represent together all the different interfaces implemented by Membership Oracles.
    internal interface IMembershipOracle : ISiloStatusOracle, ISiloShutdownParticipant, IHealthCheckParticipant
    {
    }

    // Interface for local, per-silo authorative source of information about status of other silos.
    // A local interface for local communication between in-silo runtime components and this ISiloStatusOracle.
    internal interface ISiloStatusOracle
    {
        /// <summary>
        /// Current status of this silo.
        /// </summary>
        SiloStatus CurrentStatus { get; }

        /// <summary>
        /// Start this oracle. Will register this silo in the SiloDirectory with SiloStatus.Starting status.
        /// </summary>
        Task Start(bool waitForTableToInit);

        /// <summary>
        /// Turns this oracle into an Active state. Will update this silo in the SiloDirectory with SiloStatus.Active status.
        /// </summary>
        Task BecomeActive();

        /// <summary>
        /// ShutDown this oracle. Will update this silo in the SiloDirectory with SiloStatus.ShuttingDown status. 
        /// </summary>
        Task ShutDown();

        /// <summary>
        /// Stop this oracle. Will update this silo in the SiloDirectory with SiloStatus.Stopping status. 
        /// </summary>
        Task Stop();

        /// <summary>
        /// Completely kill this oracle. Will update this silo in the SiloDirectory with SiloStatus.Dead status. 
        /// </summary>
        Task KillMyself();

        /// <summary>
        /// Get the status of a given silo. 
        /// This method returns an approximate view on the status of a given silo. 
        /// In particular, this oracle may think the given silo is alive, while it may already have failed.
        /// If this oracle thinks the given silo is dead, it has been authoratively told so by ISiloDirectory.
        /// </summary>
        /// <param name="type">siloAddress</param>A silo whose status we are interested in.
        /// <returns>The status of a given silo.</returns>
        SiloStatus GetApproximateSiloStatus(SiloAddress siloAddress);

        /// <summary>
        /// Get the statuses of all silo. 
        /// This method returns an approximate view on the statuses of all silo.
        /// </summary>
        /// <param name="type">onlyActive</param>Include only silo who are currently considered to be active. If false, inlude all.
        /// <returns>A list of silo statuses.</returns>
        Dictionary<SiloAddress, SiloStatus> GetApproximateSiloStatuses(bool onlyActive = false);

        /// <summary>
        /// Determine if the current silo is valid for creating new activations on or for directoy lookups.
        /// </summary>
        /// <returns>The silo so ask about.</returns>
        bool IsValidSilo(SiloAddress siloAddress);

        /// <summary>
        /// Determine if the current silo is dead.
        /// </summary>
        /// <returns>The silo so ask about.</returns>
        bool IsDeadSilo(SiloAddress silo);

        ///// <summary>
        ///// Inform your local oracle about your suspicion of the liveness status of a given silo.
        ///// This may include raising suspicions about silo liveness as a result of comuncation failure.
        ///// </summary>
        ///// <param name="type">siloAddress</param>A silo whose liveness we suspect.
        //void SuspectOtherSilo(SiloAddress siloAddress);

        ///// <summary>
        ///// Inform your local oracle about your confidence in the liveness of a given silo.
        ///// This may include notifying the oracle that this silo is estimated to be alive, due to recent sucessfull communication.
        ///// </summary>
        ///// <param name="type">siloAddress</param>A silo whose liveness status we want to un-suspect.
        ///// <returns>bool value indicating if the given un-suspect update was accepted by the oracle for consideration or rejected. 
        ///// In particular, if the oracle knows for sure that the given silo is dead (as authoratively told by a ISiloDirectory)
        ///// an estimate that this silo is alive would be rejected with false.</returns>
        //bool UnSuspectOtherSilo(SiloAddress siloAddress);

        /// <summary>
        /// Subscribe to status events about all silos. 
        /// </summary>
        /// <param name="type">observer</param>An observer async interface to receive silo status notifications.
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool SubscribeToSiloStatusEvents(ISiloStatusListener observer);

        /// <summary>
        /// UnSubscribe from status events about all silos. 
        /// </summary>
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool UnSubscribeFromSiloStatusEvents(ISiloStatusListener observer);
    }

    // IRemoteSiloStatusOracle System Target interface.
    // Interface for remove, async communication between ISiloDirectoryGrain and this IRemoteSiloStatusOracle,
    // as well as potentially remote to remote IRemoteSiloStatusOracle.
    internal interface IRemoteSiloStatusOracle : ISystemTarget
    {
        /// <summary>
        /// Receive notifications about silo status events. 
        /// </summary>
        ///<param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="type">updatedSilo</param>A silo to update about.
        /// <param name="type">status</param>The status of a silo.
        /// <returns></returns>
        Task SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status);

        ///// <summary>
        ///// Receive notifications about statuses of multiple events. 
        ///// </summary>
        /////<param name="destination">The address of the silo this message will be sent to.</param>
        ///// <param name="type">statuses</param>The statuses of the silos.
        ///// <returns></returns>
        //AsyncCompletion SiloStatusChangeBulkNotification(SiloAddress destination, Dictionary<SiloAddress, SiloStatus> statuses);

        /// <summary>
        /// Ping request from another silo that probes the liveness of the recipient silo.
        /// </summary>
        /// <param name="destination">The address of the silo this message will be sent to.</param>
        /// <param name="pingNumber">A unique sequence number for ping message, to facilitate testijng and debugging.</param>
        /// <returns></returns>
        Task Ping(int pingNumber);
    }

    // Interface to receive notifications from ISiloStatusOracle about status updates of different silos.
    // To be implemented by different in-silo runtime components that are interested in silo status notifications from ISiloStatusOracle.
    internal interface ISiloStatusListener
    {
        /// <summary>
        /// Receive notifications about silo status events. 
        /// </summary>
        /// <param name="type">updatedSilo</param>A silo to update about.
        /// <param name="type">status</param>The status of a silo.
        /// <returns></returns>
        void SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status);
    }
}