using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

using Orleans.Scheduler;
using Orleans.Counters;
using Orleans.Runtime.MembershipService;
using Orleans.Runtime;


namespace Orleans.Runtime.MembershipService
{
    // Just a wrapper on NamingServiceMembershipOracle to be MarshalByRefObject, so silos can be hosted in separate app domains on WF.
    // NamingServiceMembershipOracle cannot be MarshalByRefObject, since it already inherits SystemTarget (no multiple inheritance in C#).
    internal class WFMembershipOracleWrapper : MarshalByRefObject, IMembershipNamingServiceListener
    {
        private readonly NamingServiceMembershipOracle actualOracle;

        internal WFMembershipOracleWrapper(NamingServiceMembershipOracle oracle)
        {
            actualOracle = oracle;
        }

        public void AllMembersNamingServiceNotification(NamingServiceData silos)
        {
            actualOracle.AllMembersNamingServiceNotification(silos);
        }
    }

    internal class NamingServiceMembershipOracle : SystemTarget, IMembershipOracle, IMembershipNamingServiceListener
    {
        private readonly IMembershipNamingService namingServiceProvider;
        private readonly MembershipOracleData membershipOracleData;

        private readonly Logger logger;
        private readonly OrleansConfiguration orleansConfig;
        private readonly NodeConfiguration nodeConfig;
        internal SiloAddress MyAddress { get { return membershipOracleData.MyAddress; } }
        private bool shutdownCanFinish;
        private OrleansTimerInsideGrain timerReadNamingService;
        private AsyncCompletionResolver activationPromise;

        public SiloStatus CurrentStatus { get { return membershipOracleData.CurrentStatus; } } // current status of this silo.

        internal NamingServiceMembershipOracle(Silo silo, IMembershipNamingService namingServicePrvdr)
            : base(Constants.MembershipOracleId, silo.SiloAddress)
        {
            this.logger = Logger.GetLogger("MembershipOracle");
            this.namingServiceProvider = namingServicePrvdr;
            this.membershipOracleData = new MembershipOracleData(silo, logger);
            this.orleansConfig = silo.OrleansConfig;
            this.nodeConfig = silo.LocalConfig;
            this.shutdownCanFinish = false;
            this.activationPromise = new AsyncCompletionResolver();
        }

        #region ISiloStatusOracle Members

        public Task Start(bool waitForTableToInit)
        {
            logger.Info(ErrorCode.NSMBRStarting, "WFMembershipOracle starting on host = " + membershipOracleData.MyHostname + " address = " + MyAddress.ToLongString() + " at " + Logger.PrintDate(membershipOracleData.SiloStartTime));
            logger.Info(ErrorCode.NSMBRNSDetails, String.Format("WFMembershipOracle is registered in the Naming Service under name: {0}, details: {1}",
                namingServiceProvider.GetServiceName(),
                namingServiceProvider.GetServiceFullUri()));

            this.namingServiceProvider.SubscribeToNamingServiceEvents(new WFMembershipOracleWrapper(this));

            Action configure = () =>
            {
                var random = new SafeRandom();
                TimeSpan randomTableOffset = random.NextTimeSpan(orleansConfig.Globals.TableRefreshTimeout);
                if (timerReadNamingService != null)
                    timerReadNamingService.Dispose();
                timerReadNamingService = OrleansTimerInsideGrain.FromTimerCallback(OnReadNamingServiceTimer, null, randomTableOffset, orleansConfig.Globals.TableRefreshTimeout, name: "Membership.ReadNamingServiceTimer", options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
                timerReadNamingService.Start();
            };
            orleansConfig.OnConfigChange("Globals/Liveness", () => InsideGrainClient.Current.Scheduler.RunOrQueueAction(configure, this.SchedulingContext), false);
            configure();

            return TaskDone.Done;
        }

        public Task BecomeActive()
        {
            logger.Info(ErrorCode.NSMBRBecomeActive, "-BecomeActive");

            AsyncValue<NamingServiceData>.ExecuteOnThreadPool(namingServiceProvider.GetAllServiceInstances).ContinueWith((NamingServiceData table) => 
                {
                    ProcessNamingServiceUpdate(table, "BecomeActive", true);
                    logger.Info(ErrorCode.MBRFinishBecomeActive, "-Finished BecomeActive.");
                }).LogErrors(logger, ErrorCode.NSMBRFailedToBecomeActive).ContinueWith(() => {}, (Exception exc) => activationPromise.TryBreak(exc)).Ignore();

            return activationPromise.AsyncCompletion.AsTask();
        }

        public Task ShutDown()
        {
            logger.Info(ErrorCode.NSMBRShutDown, "-ShutDown");
            namingServiceProvider.UnSubscribeFromNamingServiceEvents(this);
            membershipOracleData.UpdateMyStatusLocal(SiloStatus.ShuttingDown);
            return TaskDone.Done;
        }

        public Task Stop()
        {
            logger.Info(ErrorCode.NSMBRStop, "-Stop");
            namingServiceProvider.UnSubscribeFromNamingServiceEvents(this);
            membershipOracleData.UpdateMyStatusLocal(SiloStatus.Stopping);
            return TaskDone.Done;
        }

        public Task KillMyself()
        {
            logger.Info(ErrorCode.NSMBRKillMyself, "-KillMyself");
            DisposeTimers();
            namingServiceProvider.UnSubscribeFromNamingServiceEvents(this);
            membershipOracleData.UpdateMyStatusLocal(SiloStatus.Dead);
            return TaskDone.Done;
        }

        private void KillMyselfLocally(string reason)
        {
            string msg = "I have been told I am dead, so this silo will commit suicide! " + reason;
            logger.Error(ErrorCode.NSMBRKillMyselfLocally, msg);
            DisposeTimers();
            namingServiceProvider.UnSubscribeFromNamingServiceEvents(this);
            membershipOracleData.UpdateMyStatusLocal(SiloStatus.Dead);
            logger.Fail(ErrorCode.NSMBRKillMyselfLocally, msg);
        }

        // ONLY access localTableCopy and not the localTable, to prevent races, as this method may be called outside the turn.
        public SiloStatus GetApproximateSiloStatus(SiloAddress siloAddress)
        {
            return membershipOracleData.GetApproximateSiloStatus(siloAddress);
        }

        // ONLY access localTableCopy or localTableCopyOnlyActive and not the localTable, to prevent races, as this method may be called outside the turn.
        public Dictionary<SiloAddress, SiloStatus> GetApproximateSiloStatuses(bool onlyActive = false)
        {
            return membershipOracleData.GetApproximateSiloStatuses(onlyActive);
        }

        public bool IsValidSilo(SiloAddress silo)
        {
            return membershipOracleData.IsValidSilo(silo);
        }

        public bool IsDeadSilo(SiloAddress silo)
        {
            return membershipOracleData.IsDeadSilo(silo);
        }

        public bool SubscribeToSiloStatusEvents(ISiloStatusListener observer)
        {
            return membershipOracleData.SubscribeToSiloStatusEvents(observer);
        }

        public bool UnSubscribeFromSiloStatusEvents(ISiloStatusListener observer)
        {
            return membershipOracleData.UnSubscribeFromSiloStatusEvents(observer);
        }

        #endregion

        public void AllMembersNamingServiceNotification(NamingServiceData table)
        {
            InsideGrainClient.Current.Scheduler.RunOrQueueAction(
                () => ProcessNamingServiceUpdate(table, "NamingServiceNotification"), this.SchedulingContext).LogErrors(logger, ErrorCode.NSMBRNotificationProcessingFailure).Ignore();
        }

        private void OnReadNamingServiceTimer(object data)
        {
            if (logger.IsVerbose2) logger.Verbose2("-{0} fired {1}. CurrentStatus {2}", timerReadNamingService.GetName(), timerReadNamingService.GetNumTicks(), CurrentStatus);

            timerReadNamingService.CheckTimerDelay();

            AsyncValue<NamingServiceData>.ExecuteOnThreadPool(namingServiceProvider.GetAllServiceInstances).ContinueWith(table => ProcessNamingServiceUpdate(table, "timer")).LogErrors(logger, ErrorCode.NSMBRTimerProcessingFailure).Ignore();
        }

        private void ProcessNamingServiceUpdate(NamingServiceData table, string caller, bool logAtInfoLevel = false)
        {
            if (logAtInfoLevel) logger.Info(ErrorCode.NSMBRReadAll_1, "-ProcessNamingServiceUpdate (called from {0}) NamingService data {1}", caller, table.ToString());
            else if (logger.IsVerbose) logger.Verbose("-ProcessNamingServiceUpdate (called from {0}) NamingService data {1}", caller, table.ToString());

            bool localViewChanged = CheckIfIAmActive(table);

            if (CurrentStatus.Equals(SiloStatus.Active))
            {
                CheckIfIShouldBeDead(table);
            }

            // only process the table if in the active or ShuttingDown state. In other states I am not ready yet.
            if (membershipOracleData.IsFunctional(CurrentStatus))
            {
                //foreach (MembershipEntry entry in table.Members.Where(item => !item.SiloAddress.Endpoint.Equals(MyAddress.Endpoint)))
                {
                    bool changed = membershipOracleData.TryUpdateStatusesAndNotify(table.Members);
                    //bool changed = membershipOracleData.TryUpdateStatusAndNotify(entry);
                    localViewChanged = localViewChanged || changed;
                }
            }
            if (localViewChanged) logger.Info(ErrorCode.NSMBRReadAll_2,
                "-ProcessNamingServiceUpdate (called from {0}, after local view changed) local view now: {1}",
                caller, membershipOracleData.ToString());
        }

        private bool CheckIfIAmActive(NamingServiceData table)
        {
            if (table.Contains(MyAddress))
            {
                if (CurrentStatus.Equals(SiloStatus.Created))
                {
                    membershipOracleData.UpdateMyStatusLocal(SiloStatus.Active);
                    activationPromise.TryResolve();
                    return true;
                }
            }
            return false;
        }

        private void CheckIfIShouldBeDead(NamingServiceData table)
        {
            if (!table.Contains(MyAddress.Endpoint))
            {
                string msg = string.Format("Oops - I should be Dead according to the NamingService. I am not in the table.");
                logger.Warn(ErrorCode.NSMBRFoundMyselfDead2, msg);
                KillMyselfLocally(msg);
                return;
            }

            foreach (var entry in table.Members.Values.Where(tuple => tuple.SiloAddress.Endpoint.Equals(MyAddress.Endpoint)))
            {
                SiloAddress siloAddress = entry.SiloAddress;
                if (siloAddress.Generation.Equals(MyAddress.Generation))
                {
                    continue;
                }

                if (logger.IsVerbose) logger.Verbose("Temporal anomaly detected in membership table -- Me={0} Other me={1}",
                    MyAddress.ToLongString(), siloAddress.ToLongString());

                // Temporal paradox - There is an older clone of this silo in the membership table
                if (siloAddress.Generation < MyAddress.Generation)
                {
                    logger.Warn(ErrorCode.NSMBRDetectedOlder, "Detected older version of myself -- Current Me={0} Older Me={1}, Old entry= {2}",
                        MyAddress.ToLongString(), siloAddress.ToLongString(), entry.ToFullString());
                }
                else if (siloAddress.Generation > MyAddress.Generation)
                {
                    // I am the older clone - Newer version of me should survive - I need to kill myself
                    string msg = string.Format("Detected newer version of myself - I am the older clone so will commit suicide -- Current Me={0} Newer Me={1}, Current entry= {2}",
                        MyAddress.ToLongString(), siloAddress.ToLongString(), entry.ToFullString());
                    logger.Warn(ErrorCode.NSMBRDetectedNewer, msg);
                    KillMyselfLocally(msg); // No point continuing!
                    return;
                }
            }
        }

        private void DisposeTimers()
        {
            if (timerReadNamingService != null)
            {
                timerReadNamingService.Dispose();
            }
        }

        #region Implementation of IHealthCheckParticipant

        public bool CheckHealth(DateTime lastCheckTime)
        {
            bool ok = timerReadNamingService.CheckTimerFreeze(lastCheckTime);
            return ok;
        }

        #endregion

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            if (logger.IsVerbose) logger.Verbose( "-BeginShutdown");

            shutdownCanFinish = false;
            Action canFinish = () =>
            {
                shutdownCanFinish = true;
                if (logger.IsVerbose) logger.Verbose("-BeginShutdown done.");
                tryFinishShutdown();
            };
            AsyncCompletion.FromTask(ShutDown()).ContinueWith(canFinish).LogErrors(logger, ErrorCode.NSMBRShutDownFailure).Ignore();
            // timeout in case primary is dead
            var shutDownTimer = OrleansTimerInsideGrain.FromTimerCallback(_ => canFinish(), null, TimeSpan.FromSeconds(5), Constants.INFINITE_TIMESPAN);
            shutDownTimer.Start();
        }

        public bool CanFinishShutdown()
        {
            return shutdownCanFinish;
        }

        public void FinishShutdown()
        {
            AsyncCompletion.FromTask(KillMyself()).LogErrors(logger, ErrorCode.NSMBRKillMyselfFailure).Ignore();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Late; } }

        #endregion
    }
}
