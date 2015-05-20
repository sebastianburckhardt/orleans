using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Runtime.Messaging;
using Orleans.Scheduler;
using Orleans.Counters;

namespace Orleans.Runtime.MembershipService
{
    internal class MembershipOracle : SystemTarget, IMembershipOracle, IRemoteSiloStatusOracle
    {
        private readonly IMembershipTable membershipTableProvider;
        private readonly MembershipOracleData membershipOracleData;

        private Dictionary<SiloAddress, int> probedSilos;  // map from currently probed silos to the number of failed probes
        private readonly Logger logger;
        private readonly OrleansConfiguration orleansConfig;
        private readonly NodeConfiguration nodeConfig;
        internal SiloAddress MyAddress { get { return membershipOracleData.MyAddress; } }
        private bool shutdownCanFinish;
        private OrleansTimerInsideGrain timerGetTableUpdates;
        private OrleansTimerInsideGrain timerProbeOtherSilos;
        private OrleansTimerInsideGrain timerIAmAliveUpdateInTable;
        private int pingCounter;                              // for logging and diagnostics only

        private static readonly int NUM_CONDITIONAL_WRITE_CONTENTION_ATTEMPTS = -1; // unlimited
        private static readonly int NUM_CONDITIONAL_WRITE_ERROR_ATTEMPTS = -1;
        private static readonly TimeSpan EXP_BACKOFF_ERROR_MIN = SiloMessageSender.CONNECTION_RETRY_DELAY;
        private static readonly TimeSpan EXP_BACKOFF_CONTENTION_MIN = TimeSpan.FromMilliseconds(100);
        private static TimeSpan EXP_BACKOFF_ERROR_MAX;
        private static TimeSpan EXP_BACKOFF_CONTENTION_MAX; // set based on config
        private static readonly TimeSpan EXP_BACKOFF_STEP = TimeSpan.FromMilliseconds(1000);

        public SiloStatus CurrentStatus { get { return membershipOracleData.CurrentStatus; } } // current status of this silo.

        internal MembershipOracle(Silo silo, IMembershipTable membershipTable)
            : base(Constants.MembershipOracleId, silo.SiloAddress)
        {
            this.logger = Logger.GetLogger("MembershipOracle");
            this.membershipTableProvider = membershipTable;
            this.membershipOracleData = new MembershipOracleData(silo, logger);
            this.probedSilos = new Dictionary<SiloAddress, int>();
            this.orleansConfig = silo.OrleansConfig;
            this.nodeConfig = silo.LocalConfig;
            this.shutdownCanFinish = false;
            this.pingCounter = 0;
            TimeSpan backOffMax = StandardExtensions.Max(EXP_BACKOFF_STEP.Multiply(orleansConfig.Globals.ExpectedClusterSize), SiloMessageSender.CONNECTION_RETRY_DELAY.Multiply(2));
            MembershipOracle.EXP_BACKOFF_CONTENTION_MAX = backOffMax;
            MembershipOracle.EXP_BACKOFF_ERROR_MAX = backOffMax;
        }

        #region ISiloStatusOracle Members

        public async Task Start(bool waitForTableToInit)
        {
            try
            {
                logger.Info(ErrorCode.MBRStarting, "MembershipOracle starting on host = " + membershipOracleData.MyHostname + " address = " + MyAddress.ToLongString() + " at " + Logger.PrintDate(membershipOracleData.SiloStartTime) + ", backOffMax = " + EXP_BACKOFF_CONTENTION_MAX);

                if (waitForTableToInit)
                    await WaitForTableToInit();

                // randomly delay the startup, so not all silos write to the table at once.
                // Use random time not larger than MaxJoinAttemptTime, one minute and 0.5sec*ExpectedClusterSize;
                var random = new SafeRandom();
                TimeSpan maxDelay = TimeSpan.FromMilliseconds(500).Multiply(orleansConfig.Globals.ExpectedClusterSize);
                maxDelay = StandardExtensions.Min(maxDelay, StandardExtensions.Min(orleansConfig.Globals.MaxJoinAttemptTime, TimeSpan.FromMinutes(1)));
                TimeSpan randomDelay = random.NextTimeSpan(maxDelay);
                await Task.Delay(randomDelay);

                // first, cleanup all outdated entries of myself from the table
                await CleanupTable().AsTask();

                // write myself to the table
                await UpdateMyStatusGlobal(SiloStatus.Joining).AsTask();

                StartIAmAliveUpdateTimer();

                // read the table and look for my node migration occurrences
                await DetectNodeMigration(membershipOracleData.MyHostname).AsTask();
            }
            catch (Exception exc)
            {
                logger.Error(ErrorCode.MBRFailedToStart, "MBRFailedToStart", exc);
                throw;
            }
        }

        private AsyncCompletion DetectNodeMigration(string myHostname)
        {
            return AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith((MembershipTableData table) =>
            {
                if (logger.IsVerbose) logger.Verbose("-ReadAll MBR table {0}", table.ToString());
                CheckMissedIAmAlives(table);

                string myInstanceName = nodeConfig.SiloName;
                MembershipEntry mostRecentPreviousEntry = null;
                // look for silo instances that are same as me, find most recent with Generation before me.
                foreach (MembershipEntry entry in table.Members.Select(tuple => tuple.Item1).Where(data => myInstanceName.Equals(data.InstanceName)))
                {
                    bool iAmLater = MyAddress.Generation.CompareTo(entry.SiloAddress.Generation) > 0;
                    // more recent
                    if (iAmLater && (mostRecentPreviousEntry == null || entry.SiloAddress.Generation.CompareTo(mostRecentPreviousEntry.SiloAddress.Generation) > 0))
                    {
                        mostRecentPreviousEntry = entry;
                    }
                }

                if (mostRecentPreviousEntry != null)
                {
                    bool physicalHostChanged = !myHostname.Equals(mostRecentPreviousEntry.HostName) || !MyAddress.Endpoint.Equals(mostRecentPreviousEntry.SiloAddress.Endpoint);
                    if (physicalHostChanged)
                    {
                        string error = String.Format("Silo instance {0} migrated from host {1} silo address {2} to host {3} silo address {4}.",
                            myInstanceName, myHostname, MyAddress.ToLongString(), mostRecentPreviousEntry.HostName, mostRecentPreviousEntry.SiloAddress.ToLongString());
                        logger.Warn(ErrorCode.MBRNodeMigrated, error);
                    }
                    else
                    {
                        string error = String.Format("Silo instance {0} restarted on same host {1} New silo address = {2} Previous silo address = {3}",
                            myInstanceName, myHostname, MyAddress.ToLongString(), mostRecentPreviousEntry.SiloAddress.ToLongString());
                        logger.Warn(ErrorCode.MBRNodeRestarted, error);
                    }
                }
            });
        }

        public Task BecomeActive()
        {
            logger.Info(ErrorCode.MBRBecomeActive, "-BecomeActive");
#if DEBUG_MEMBERSHIP
            new MembershipServiceTestAgent(this, DateTime.UtcNow).Start();
#endif
            // write myself to the table
            // read the table and store locally the list of live silos
            return UpdateMyStatusGlobal(SiloStatus.Active).ContinueWith(() =>
            {
                return AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith((MembershipTableData table) =>
                {
                    return ProcessTableUpdate(table, "BecomeActive", true).ContinueWith(() =>
                    {
                        GossipMyStatus(); // only now read and stored the table locally.

                        Action configure = () =>
                        {
                            var random = new SafeRandom();
                            TimeSpan randomTableOffset = random.NextTimeSpan(orleansConfig.Globals.TableRefreshTimeout);
                            TimeSpan randomProbeOffset = random.NextTimeSpan(orleansConfig.Globals.ProbeTimeout);
                            if (timerGetTableUpdates != null)
                                timerGetTableUpdates.Dispose();
                            timerGetTableUpdates =
                                OrleansTimerInsideGrain.FromTimerCallback(OnGetTableUpdateTimer, null, randomTableOffset, orleansConfig.Globals.TableRefreshTimeout, name: "Membership.ReadTableTimer", options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
                            timerGetTableUpdates.Start();

                            if (timerProbeOtherSilos != null)
                                timerProbeOtherSilos.Dispose();
                            timerProbeOtherSilos =
                                OrleansTimerInsideGrain.FromTimerCallback(OnProbeOtherSilosTimer, null, randomProbeOffset, orleansConfig.Globals.ProbeTimeout, name: "Membership.ProbeTimer", options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
                            timerProbeOtherSilos.Start();
                        };
                        orleansConfig.OnConfigChange("Globals/Liveness", () => InsideGrainClient.Current.Scheduler.RunOrQueueAction(configure, this.SchedulingContext), false);
                        configure();
                        logger.Info(ErrorCode.MBRFinishBecomeActive, "-Finished BecomeActive.");
                        return AsyncCompletion.Done;
                    });
                });
            }).LogErrors(logger, ErrorCode.MBRFailedToBecomeActive).AsTask();
        }

        private void StartIAmAliveUpdateTimer()
        {
            logger.Info(ErrorCode.MBRStartingIAmAliveTimer, "Starting IAmAliveUpdateTimer.");
        
            if (timerIAmAliveUpdateInTable != null)
                timerIAmAliveUpdateInTable.Dispose();
            timerIAmAliveUpdateInTable = OrleansTimerInsideGrain.FromTimerCallback(OnIAmAliveUpdateInTableTimer, null, TimeSpan.Zero, orleansConfig.Globals.IAmAliveTablePublishTimeout, name: "Membership.IAmAliveTimer", options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
            timerIAmAliveUpdateInTable.Start();
        }

        public Task ShutDown()
        {
            logger.Info(ErrorCode.MBRShutDown, "-ShutDown");
            return UpdateMyStatusGlobal(SiloStatus.ShuttingDown).LogErrors(logger, ErrorCode.MBRFailedToShutdown).AsTask();
        }

        public Task Stop()
        {
            logger.Info(ErrorCode.MBRStop, "-Stop");
            return UpdateMyStatusGlobal(SiloStatus.Stopping).LogErrors(logger, ErrorCode.MBRFailedToStop).AsTask();
        }

        public Task KillMyself()
        {
            logger.Info(ErrorCode.MBRKillMyself, "-KillMyself");
            DisposeTimers();
            return UpdateMyStatusGlobal(SiloStatus.Dead).LogErrors(logger, ErrorCode.MBRFailedToKillMyself).AsTask();
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


        #region IRemoteSiloStatusOracle Members

        // Treat this gossip msg as a trigger to read the table (and just ignore the input parameters).
        // This simplified a lot of the races when we get gossip info which is outdated with the table truth.
        public Task SiloStatusChangeNotification(SiloAddress updatedSilo, SiloStatus status)
        {
            if (logger.IsVerbose2) logger.Verbose2("-Received GOSSIP SiloStatusChangeNotification about {0} status {1}. Going to read the table.", updatedSilo, status);
            if (membershipOracleData.IsFunctional(CurrentStatus))
            {
                return AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith(table => ProcessTableUpdate(table, "gossip")).LogErrors(logger, ErrorCode.MBRGossipProcessingFailure).AsTask();
            }
            else
            {
                return TaskDone.Done;
            }
        }

        public Task Ping(int pingNumber)
        {
            // do not do anything here -- simply returning back will indirectly notify the prober that this silo is alive
            return TaskDone.Done;
        }

        #endregion

        private async Task WaitForTableToInit()
        {
            TimeSpan timespan = Debugger.IsAttached ? TimeSpan.FromMinutes(5) : TimeSpan.FromSeconds(5);
            // This is a hack to enable primary node to start fully before secondaries.
            // Secondary silos waits untill GrainBasedMembershipTable is created. 
            for (int i = 0; i < 100; i++)
            {
                bool needToWait = false;
                try
                {
                    MembershipTableData table = await membershipTableProvider.ReadAll().WithTimeout(timespan);
                    if (table.Members.Any(tuple => tuple.Item1.Primary))
                    {
                        logger.Info(ErrorCode.MBRTableGrainInit1, "-Connected to membership table provider and also found primary silo registered in the table.");
                        return;
                    }
                    else
                    {
                        logger.Info(ErrorCode.MBRTableGrainInit2, "-Connected to membership table provider but did not find primary silo registered in the table. Going to try again for a {0}th time.", i);
                    }
                }
                catch (Exception exc)
                {
                    Type type = exc.GetBaseException().GetType();
                    if (type.Equals(typeof(TimeoutException)) || type.Equals(typeof(OrleansException)))
                    {
                        logger.Info(ErrorCode.MBRTableGrainInit3, "-Waiting for membership table provider to initialize. Going to sleep for {0} and re-try to reconnect.", timespan);
                        needToWait = true;
                    }
                    else
                    {
                        logger.Info(ErrorCode.MBRTableGrainInit4, "-Membership table provider failed to initialize. Giving up.");
                        throw;
                    }
                }

                if (needToWait)
                {
                    await Task.Delay(timespan);
                }
            }
        }

        #region Table update/insert processing

        private AsyncValue<bool> MBRExecuteWithRetries(Func<int, AsyncValue<bool>> avTFunction, TimeSpan timeout)
        {
            try
            {
                Func<int, Task<bool>> taskFunction = (int i) => { return avTFunction(i).AsTask(); };
                return AsyncValue.FromTask(
                    AsyncExecutorWithRetries.ExecuteWithRetries<bool>(
                        taskFunction,
                        NUM_CONDITIONAL_WRITE_CONTENTION_ATTEMPTS,
                        NUM_CONDITIONAL_WRITE_ERROR_ATTEMPTS,
                        (bool result, int i) => { return result == false; },   // if failed to Update on contention - retry   
                        (Exception exc, int i) => { return true; },            // Retry on errors.          
                        timeout,
                        new ExponentialBackoff(EXP_BACKOFF_CONTENTION_MIN, EXP_BACKOFF_CONTENTION_MAX, EXP_BACKOFF_STEP), // how long to wait between successful retries
                        new ExponentialBackoff(EXP_BACKOFF_ERROR_MIN, EXP_BACKOFF_ERROR_MAX, EXP_BACKOFF_STEP)  // how long to wait between error retries
                ));
            }
            catch (Exception exc)
            {
                return new AsyncValue<bool>(exc);
            }
        }

        private AsyncCompletion CleanupTable()
        {
            return MBRExecuteWithRetries(
                (int counter) =>
                {
                    if (logger.IsVerbose) logger.Verbose("-Attempting CleanupTableEntries #{0}", counter);
                    return AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith(table =>
                    {
                        logger.Info(ErrorCode.MBRReadAll_Cleanup, "-CleanupTable called on silo startup. MBR table {0}", table.ToString());
                        return CleanupTableEntries(table);
                    });
                }, orleansConfig.Globals.MaxJoinAttemptTime);
        }

        private AsyncCompletion UpdateMyStatusGlobal(SiloStatus status)
        {
            string errorString = null;
            int numCalls = 0;

            return MBRExecuteWithRetries(
                (int counter) =>
                {
                    numCalls++;
                    if (logger.IsVerbose) logger.Verbose("-Going to try to UpdateMyStatusGlobal_AttempOnce #{0}", counter);
                    return UpdateMyStatusGlobal_AttempOnce(status);  // function to retry
                }, orleansConfig.Globals.MaxJoinAttemptTime).ContinueWith((bool ret) =>
                {
                    if (ret)
                    {
                        if (logger.IsVerbose) logger.Verbose("-Silo {0} Successfully updated my Status in the MBR table to {1}", MyAddress.ToLongString(), status);
                        membershipOracleData.UpdateMyStatusLocal(status);
                        GossipMyStatus();
                    }
                    else
                    {
                        errorString = String.Format("-Silo {0} failed to update its status to {1} in the table due to precondition failures after {2} attempts.", MyAddress.ToLongString(), status, numCalls);
                        logger.Error(ErrorCode.MBRFailedToWriteConditional, errorString);
                        throw new OrleansException(errorString);
                    }
                }, (Exception exc) =>
                {
                    if (errorString == null)
                    {
                        errorString = String.Format("-Silo {0} failed to update its status to {1} in the table due to failures (socket failures or table read/write failures) after {2} attempts: {3}", MyAddress.ToLongString(), status, numCalls, exc.Message);
                        logger.Error(ErrorCode.MBRFailedToWrite, errorString);
                    }
                    throw new OrleansException(errorString, exc);
                });
        }

        // read the table
        // find all currently active nodes and test pings to all of them
        //      try to ping all
        //      if all pings succeeded
        //             try to change my status to Active and in the same write transaction update MBR version row, conditioned on both etags
        //      if failed (on ping or on write exception or on etag) - retry the whole AttemptToJoinActiveNodes
        private AsyncValue<bool> UpdateMyStatusGlobal_AttempOnce(SiloStatus newStatus)
        {
            AsyncValue<MembershipTableData> tablePromise = null;
            if (newStatus.Equals(SiloStatus.Active))
            {
                tablePromise = AsyncValue.FromTask(membershipTableProvider.ReadAll());
            }
            else
            {
                tablePromise = AsyncValue.FromTask(membershipTableProvider.ReadRow(MyAddress));
            }
            return tablePromise.ContinueWith((MembershipTableData table) =>
            {
                if (logger.IsVerbose) logger.Verbose("-UpdateMyStatusGlobal_AttempOnce: Read{0} MBR table {1}", (newStatus.Equals(SiloStatus.Active) ? "All" : " my entry from"), table.ToString());
                CheckMissedIAmAlives(table);

                MembershipEntry myEntry = null;
                string myEtag = null;
                if (table.Contains(MyAddress))
                {
                    var myTuple = table.Get(MyAddress);
                    myEntry = myTuple.Item1;
                    myEtag = myTuple.Item2;
                    myEntry.TryUpdateStartTime(membershipOracleData.SiloStartTime);
                    if (myEntry.Status == SiloStatus.Dead) // check if the table already knows that I am dead
                    {
                        string msg = string.Format("Oops - I should be Dead according to membership table (in UpdateMyStatusGlobal_AttempOnce): myEntry = {0}.", myEntry.ToFullString());
                        logger.Warn(ErrorCode.MBRFoundMyselfDead1, msg);
                        KillMyselfLocally(msg);
                        return true;
                    }
                }
                else // first write attempt of this silo. Insert instead of Update.
                {
                    myEntry = membershipOracleData.CreateNewMembershipEntry(nodeConfig, newStatus);
                }
                if (newStatus == SiloStatus.Dead)
                {
                    myEntry.AddSuspector(new Tuple<SiloAddress, DateTime>(MyAddress, DateTime.UtcNow)); // add the killer (myself) to the suspect list, for easier diagnostics later on.
                }
                myEntry.Status = newStatus;
                myEntry.IAmAliveTime = DateTime.UtcNow;

                AsyncCompletion preConditionPromise = AsyncCompletion.Done;
                if (newStatus.Equals(SiloStatus.Active))
                {
                    preConditionPromise = GetJoiningPreconditionPromise(table);
                }
                return preConditionPromise.ContinueWith(() =>
                {
                    if (myEtag != null) // no previous etag for my entry -> its firts write to this entry, so insert instead of update.
                        return AsyncValue.FromTask(membershipTableProvider.UpdateRow(myEntry, myEtag, table.Version.Next()));
                    else
                        return AsyncValue.FromTask(membershipTableProvider.InsertRow(myEntry, table.Version.Next()));
                });
            });
        }

        private AsyncCompletion GetJoiningPreconditionPromise(MembershipTableData table)
        {
            // send pings to all Active nodes, that are known to be alive
            List<MembershipEntry> members = table.Members.Select(tuple => tuple.Item1)
                .Where(entry =>
                            entry.Status.Equals(SiloStatus.Active) &&
                            !entry.SiloAddress.Equals(MyAddress) &&
                            !HasMissedIAmAlives(entry, false)
                 ).ToList();
            logger.Info(ErrorCode.MBRSendingPreJoinPing, "About to send pings to {0} nodes in order to validate communication in the Joining state. Pinged nodes = {1}",
                members.Count, Utils.IEnumerableToString(members, entry => entry.SiloAddress.ToLongString()));

            List<AsyncCompletion> pingPromises = new List<AsyncCompletion>();
            foreach (MembershipEntry entry in members)
            {
                SiloAddress siloCapture = entry.SiloAddress; // Capture loop variable
                int counterCapture = pingCounter++;
                AsyncCompletion ac = SendPing(siloCapture, counterCapture)
                            .ContinueWith(() => { }, (Exception exc) =>
                            {
                                LogFailedProbe(siloCapture, counterCapture, exc);
                                throw exc;
                            });
                pingPromises.Add(ac);
            }
            return AsyncCompletion.JoinAll(pingPromises);
        }

        #endregion

        private AsyncCompletion ProcessTableUpdate(MembershipTableData table, string caller, bool logAtInfoLevel = false)
        {
            if (logAtInfoLevel) logger.Info(ErrorCode.MBRReadAll_1, "-ReadAll (called from {0}) MBR table {1}", caller, table.ToString());
            else if (logger.IsVerbose) logger.Verbose("-ReadAll (called from {0}) MBR table {1}", caller, table.ToString());

            // Even if failed to clean up old entries from the table, still process the new entries. Will retry cleanup next time.
            return CleanupTableEntries(table).ContinueWith(() => { }, (Exception exc) => { }) // just eat the exception.
                    .ContinueWith(() =>
                    {
                        bool localViewChanged = false;
                        CheckMissedIAmAlives(table);
                        // only process the table if in the active or ShuttingDown state. In other states I am not ready yet.
                        if (membershipOracleData.IsFunctional(CurrentStatus))
                        {
                            foreach (MembershipEntry entry in table.Members.Select(tuple => tuple.Item1).Where(item => !item.SiloAddress.Endpoint.Equals(MyAddress.Endpoint)))
                            {
                                bool changed = membershipOracleData.TryUpdateStatusAndNotify(entry);
                                localViewChanged = localViewChanged || changed;
                            }
                            if (localViewChanged)
                            {
                                UpdateListOfProbedSilos();
                            }
                        }
                        if (localViewChanged) logger.Info(ErrorCode.MBRReadAll_2,
                            "-ReadAll (called from {0}, after local view changed, with removed duplicate deads) MBR table: {1}",
                            caller, table.SupressDuplicateDeads().ToString());
                    });
        }

        private void CheckMissedIAmAlives(MembershipTableData table)
        {
            foreach (MembershipEntry entry in table.Members.Select(tuple => tuple.Item1).
                                                            Where(entry => !entry.SiloAddress.Equals(MyAddress)).
                                                            Where(entry => entry.Status.Equals(SiloStatus.Active)))
            {
                HasMissedIAmAlives(entry, true);
            }
        }

        private bool HasMissedIAmAlives(MembershipEntry entry, bool writeWarning)
        {
            DateTime now = Logger.ParseDate(Logger.PrintDate(DateTime.UtcNow));
            TimeSpan allowableIAmAliveMissPeriod = orleansConfig.Globals.IAmAliveTablePublishTimeout.Multiply(orleansConfig.Globals.NumMissedTableIAmAliveLimit);
            DateTime lastIAmAlive = entry.IAmAliveTime;
            if (entry.IAmAliveTime.Equals(default(DateTime)))
            {
                lastIAmAlive = entry.StartTime; // he has not written first IAmAlive yet, use its start time instead.
            }
            if (now - lastIAmAlive > allowableIAmAliveMissPeriod)
            {
                if (writeWarning)
                {
                    logger.Warn(ErrorCode.MBRMissedIAmAliveTableUpdate,
                        String.Format("Noticed that silo {0} has not updated it's IAmAliveTime table column recently. Last update was at {1}, now is {2}, no update for {3}, which is more than {4}.",
                            entry.SiloAddress.ToLongString(),
                            lastIAmAlive,
                            now,
                            now - lastIAmAlive,
                            allowableIAmAliveMissPeriod));
                }
                return true;
            }
            return false;
        }

        private AsyncValue<bool> CleanupTableEntries(MembershipTableData table)
        {
            List<Tuple<MembershipEntry, string>> silosToDeclareDead = new List<Tuple<MembershipEntry, string>>();
            foreach (var tuple in table.Members.Where(tuple => tuple.Item1.SiloAddress.Endpoint.Equals(MyAddress.Endpoint)))
            {
                MembershipEntry entry = tuple.Item1;
                SiloAddress siloAddress = entry.SiloAddress;
                //if (logger.IsVerbose2) logger.Verbose2("CleanupTableEntries: found my entry in the table: {0}", entry.ToFullString());

                if (siloAddress.Generation.Equals(MyAddress.Generation))
                {
                    if (entry.Status == SiloStatus.Dead)
                    {
                        string msg = string.Format("Oops - I should be Dead according to membership table (in CleanupTableEntries): entry = {0}.", entry.ToFullString());
                        logger.Warn(ErrorCode.MBRFoundMyselfDead2, msg);
                        KillMyselfLocally(msg);
                    }
                    continue;
                }
                else if (entry.Status == SiloStatus.Dead)
                {
                    if (logger.IsVerbose2) logger.Verbose2("Skipping my previous old Dead entry in membership table: {0}", entry.ToFullString());
                    continue;
                }

                if (logger.IsVerbose) logger.Verbose("Temporal anomaly detected in membership table -- Me={0} Other me={1}",
                    MyAddress.ToLongString(), siloAddress.ToLongString());

                // Temporal paradox - There is an older clone of this silo in the membership table
                if (siloAddress.Generation < MyAddress.Generation)
                {
                    logger.Warn(ErrorCode.MBRDetectedOlder, "Detected older version of myself - Marking other older clone as Dead -- Current Me={0} Older Me={1}, Old entry= {2}",
                        MyAddress.ToLongString(), siloAddress.ToLongString(), entry.ToFullString());
                    // Declare older clone of me as Dead.
                    silosToDeclareDead.Add(tuple);   //return DeclareDead(entry, eTag, tableVersion);
                }
                else if (siloAddress.Generation > MyAddress.Generation)
                {
                    // I am the older clone - Newer version of me should survive - I need to kill myself
                    string msg = string.Format("Detected newer version of myself - I am the older clone so will commit suicide -- Current Me={0} Newer Me={1}, Current entry= {2}",
                        MyAddress.ToLongString(), siloAddress.ToLongString(), entry.ToFullString());
                    logger.Warn(ErrorCode.MBRDetectedNewer, msg);
                    return AsyncCompletion.FromTask(KillMyself()).ContinueWith(() => KillMyselfLocally(msg)).ContinueWith(() => true); // No point continuing!
                }

            }
            if (silosToDeclareDead.Count == 0) return true;

            if (logger.IsVerbose) logger.Verbose("CleanupTableEntries: About to DeclareDead {0} outdated silos in the table: {1}", silosToDeclareDead.Count,
                Utils.IEnumerableToString(silosToDeclareDead.Select(tuple => tuple.Item1), entry => entry.ToString()));

            List<bool> retValues = new List<bool>();
            TableVersion nextVersion = table.Version;
            return AsyncLoop.For(silosToDeclareDead.Count, (int index) =>
            {
                MembershipEntry entry = silosToDeclareDead[index].Item1;
                string eTag = silosToDeclareDead[index].Item2;
                return DeclareDead(entry, eTag, nextVersion).ContinueWith((bool ret) =>
                {
                    retValues.Add(ret);
                    nextVersion = nextVersion.Next(); // advance the table version (if write succeded, we advanced the version. if failed, someone else did. It is safe anyway).
                });
            }).ContinueWith(() => retValues.All(elem => elem));  // if at least one has failed, return false.
        }

        private void KillMyselfLocally(string reason)
        {
            string msg = "I have been told I am dead, so this silo will commit suicide! " + reason;
            logger.Error(ErrorCode.MBRKillMyselfLocally, msg);
            bool alreadyStopping = CurrentStatus == SiloStatus.Dead || CurrentStatus == SiloStatus.ShuttingDown || CurrentStatus == SiloStatus.Stopping;
            DisposeTimers();
            membershipOracleData.UpdateMyStatusLocal(SiloStatus.Dead);

            if (alreadyStopping && orleansConfig.IsRunningAsUnitTest)
            {
                // do not abort in unit tests.
            }
            else
            {
                logger.Fail(ErrorCode.MBRKillMyselfLocally, msg);
            }
            // Logger.Fail does Process.Exit()
        }

        private void GossipMyStatus()
        {
            GossipToOthers(MyAddress, CurrentStatus);
        }

        private void GossipToOthers(SiloAddress updatedSilo, SiloStatus updatedStatus)
        {
            if (orleansConfig.Globals.UseLivenessGossip)
            {
                // spread the rumor that some silo has just been marked dead
                foreach (SiloAddress silo in membershipOracleData.GetSiloStatuses(status => membershipOracleData.IsFunctional(status), false).Keys)
                {
                    if (logger.IsVerbose2) logger.Verbose2("-Sending status update GOSSIP notification about silo {0}, status {1}, to silo {2}", updatedSilo.ToLongString(), updatedStatus, silo.ToLongString());
                    AsyncCompletion.FromTask(GetOracleReference(silo).SiloStatusChangeNotification(updatedSilo, updatedStatus)).LogErrors(logger, ErrorCode.MBRGossipSendFailure).Ignore();
                }
            }
        }

        private void UpdateListOfProbedSilos()
        {
            // if I am still not fully functional, I should not be probing others.
            if (!membershipOracleData.IsFunctional(CurrentStatus))
            {
                return;
            }
            // keep watching shutting-down silos as well, so we can properly ensure they are dead.
            List<SiloAddress> tmpList = membershipOracleData.GetSiloStatuses(status => membershipOracleData.IsFunctional(status), true).Keys.ToList();
            tmpList.Sort((x, y) => x.GetConsistentHashCode().CompareTo(y.GetConsistentHashCode()));

            int myIndex = tmpList.FindIndex(el => el.Equals(MyAddress));
            if (myIndex < 0)
            {
                // this should not happen ...
                string error = String.Format("This silo {0} status {1} is not in its own local silo list! This is a bug!", MyAddress.ToLongString(), CurrentStatus);
                logger.Error(ErrorCode.Runtime_Error_100305, error);
                throw new Exception(error);
            }

            // an old implementation by Alex that has the deadlocking problem
            //for (int i = 0; i < Math.Min(globalConfig.NumProbedSilos, tmpList.Count - 1); i++)
            //{
            //    SiloAddress candidate = tmpList[(myIndex + i + 1) % tmpList.Count];
            //    probedSilos.Add(candidate, 0);
            //}

            // The below implementation does not have the deadlock problem.
            // Go over every node excluding me,
            // Find up to NumProbedSilos silos after me, which are not suspected by anyone and add them to the probedSilos,
            // In addition, every suspected silo you encounter on the way, add him to the probedSilos.
            List<SiloAddress> silosToWatch = new List<SiloAddress>();
            List<SiloAddress> additionalSilos = new List<SiloAddress>();
            for (int i = 0; i < tmpList.Count - 1 && silosToWatch.Count < orleansConfig.Globals.NumProbedSilos; i++)
            {
                SiloAddress candidate = tmpList[(myIndex + i + 1) % tmpList.Count];
                bool isSuspected = membershipOracleData.GetSiloEntry(candidate).GetFreshVotes(orleansConfig.Globals.DeathVoteExpirationTimeout).Count > 0;
                //bool isShuttingDown = localTable[candidate].MembershipEntry.Status == SiloStatus.ShuttingDown;
                if (isSuspected) // || isShuttingDown)
                {
                    additionalSilos.Add(candidate);
                }
                else
                {
                    silosToWatch.Add(candidate);
                }
            }

            // take new watched silos, but leave the probe counters for the old ones.
            Dictionary<SiloAddress, int> newProbedSilos = new Dictionary<SiloAddress, int>();
            foreach (var silo in silosToWatch.Union(additionalSilos))
            {
                int oldValue = 0;
                probedSilos.TryGetValue(silo, out oldValue);
                if (!newProbedSilos.ContainsKey(silo)) // duplication suppression.
                {
                    newProbedSilos[silo] = oldValue;
                }
            }
            if (!AreTheSame(probedSilos.Keys, newProbedSilos.Keys))
            {
                logger.Info(ErrorCode.MBRWatchList, "Will watch (actively ping) {0} silos: {1}",
                            newProbedSilos.Count, Utils.IEnumerableToString(newProbedSilos.Keys, (silo) => silo.ToLongString()));
            }
            probedSilos = newProbedSilos;
        }

        private static bool AreTheSame<T, V>(Dictionary<T, V>.KeyCollection first, Dictionary<T, V>.KeyCollection second)
        {
            int count = first.Count;
            if (count != second.Count) return false;
            return first.Intersect(second).Count() == count;
        }

        private void OnGetTableUpdateTimer(object data)
        {
            if (logger.IsVerbose2) logger.Verbose2("-{0} fired {1}. CurrentStatus {2}", timerGetTableUpdates.GetName(), timerGetTableUpdates.GetNumTicks(), CurrentStatus);

            timerGetTableUpdates.CheckTimerDelay();

            AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith(table => ProcessTableUpdate(table, "timer")).LogErrors(logger, ErrorCode.MBRTimerProcessingFailure).Ignore();
        }

        private void OnProbeOtherSilosTimer(object data)
        {
            if (logger.IsVerbose2) logger.Verbose2("-{0} fired {1}. CurrentStatus {2}", timerProbeOtherSilos.GetName(), timerProbeOtherSilos.GetNumTicks(), CurrentStatus);

            timerProbeOtherSilos.CheckTimerDelay();

            foreach (SiloAddress silo in probedSilos.Keys)
            {
                SiloAddress siloAddress = silo; // Capture loop variable
                int counterCapture = pingCounter++;
                SendPing(siloAddress, counterCapture).ContinueWith(() => ResetFailedProbes(siloAddress, counterCapture), exc => IncFailedProbes(siloAddress, counterCapture, exc))
                    .LogErrors(logger, ErrorCode.MBRSendPingFailure).Ignore();
            }
        }

        private void OnIAmAliveUpdateInTableTimer(object data)
        {
            if (logger.IsVerbose2) logger.Verbose2("-{0} fired {1}. CurrentStatus {2}", timerIAmAliveUpdateInTable.GetName(), timerIAmAliveUpdateInTable.GetNumTicks(), CurrentStatus);

            timerIAmAliveUpdateInTable.CheckTimerDelay();

            MembershipEntry entry = new MembershipEntry
            {
                SiloAddress = MyAddress,
                IAmAliveTime = DateTime.UtcNow
            };
            AsyncCompletion.FromTask(membershipTableProvider.MergeColumn(entry)).LogErrors(logger, ErrorCode.MBRMergeColumnFailure).Ignore();
        }

        private AsyncCompletion SendPing(SiloAddress siloAddress, int pingNumber)
        {
            if (logger.IsVerbose2) logger.Verbose2("-Going to send Ping #{0} to probe silo {1}", pingNumber, siloAddress.ToLongString());
            AsyncCompletion promise;
            try
            {
                RequestContext.Set(Message.Header.PingApplicationHeader, true);
                promise = AsyncCompletion.FromTask(GetOracleReference(siloAddress).Ping(pingNumber));
            }
            finally
            {
                RequestContext.Remove(Message.Header.PingApplicationHeader);
            }
            MessagingStatisticsGroup.OnPingSend(siloAddress);
            return promise;
        }

        private void ResetFailedProbes(SiloAddress silo, int pingNumber)
        {
            if (logger.IsVerbose2) logger.Verbose2("-Got successful ping response for ping #{0} from {1}", pingNumber, silo.ToLongString());
            MessagingStatisticsGroup.OnPingReplyReceived(silo);
            if (probedSilos.ContainsKey(silo))
            {
                // need this check to avoid races with changed membership; 
                // otherwise, we might insert here a new entry to the 'probedSilos' dictionary
                probedSilos[silo] = 0;
            }
        }

        private void IncFailedProbes(SiloAddress silo, int pingNumber, Exception failureReason)
        {
            MessagingStatisticsGroup.OnPingReplyMissed(silo);
            if (!probedSilos.ContainsKey(silo))
            {
                // need this check to avoid races with changed membership (I was watching him, but then read the table, learned he is already dead and thus no longer wtaching him); 
                // otherwise, we might here insert a new entry to the 'probedSilos' dictionary
                logger.Info(ErrorCode.MBRPingedSiloNotInWatchList, "-Does not have {0} in the list of probes, ignoring", silo.ToLongString());
                return;
            }

            LogFailedProbe(silo, pingNumber, failureReason);

            probedSilos[silo] = probedSilos[silo] + 1;

            if (logger.IsVerbose2) logger.Verbose2("-Current number of failed probes for {0}: {1}", silo.ToLongString(), probedSilos[silo]);
            if (probedSilos[silo] < orleansConfig.Globals.NumMissedProbesLimit)
            {
                return;
            }

            MBRExecuteWithRetries(
                    (int counter) => TryToSuspectOrKill(silo),
                    orleansConfig.Globals.MaxJoinAttemptTime).LogErrors(logger, ErrorCode.MBRFailedToSuspect).Ignore();
        }

        private void LogFailedProbe(SiloAddress silo, int pingNumber, Exception failureReason)
        {
            string reason = String.Format("Original Exc Type: {0} Message:{1}", failureReason.GetBaseException().GetType(), failureReason.GetBaseException().Message);
            logger.Warn(ErrorCode.MBRMissedPing, "-Did not get ping response for ping #{0} from {1}. Reason = {2}", pingNumber, silo.ToLongString(), reason);
        }

        private AsyncValue<bool> TryToSuspectOrKill(SiloAddress silo)
        {
            return AsyncValue.FromTask(membershipTableProvider.ReadAll()).ContinueWith(table =>
            {
                //if (logger.IsVerbose) logger.Verbose("-Read an entry of silo {0} from MBR table {1}", silo.ToLongString(), table.ToString());
                if (logger.IsVerbose) logger.Verbose("-TryToSuspectOrKill: Read MBR table {0}", table.ToString());
                if (table.Contains(MyAddress))
                {
                    MembershipEntry myEntry = table.Get(MyAddress).Item1;
                    if (myEntry.Status == SiloStatus.Dead) // check if the table already knows that I am dead
                    {
                        string msg = string.Format("Oops - I should be Dead according to membership table (in TryToSuspectOrKill): entry = {0}.", myEntry.ToFullString());
                        logger.Warn(ErrorCode.MBRFoundMyselfDead3, msg);
                        KillMyselfLocally(msg);
                        return true;
                    }
                }

                if (!table.Contains(silo))
                {
                    // this should not happen ...
                    string str = String.Format("-Could not find silo entry for silo {0} in the table.", silo.ToLongString());
                    logger.Error(ErrorCode.MBRFailedToReadSilo, str);
                    throw new KeyNotFoundException(str);
                }

                var tuple = table.Get(silo);
                MembershipEntry entry = tuple.Item1;
                string eTag = tuple.Item2;
                if (logger.IsVerbose) logger.Verbose("-TryToSuspectOrKill {0}: The current status of {0} in the table is {1}, its entry is {2}", entry.SiloAddress.ToLongString(), entry.Status, entry.ToFullString());
                // check if the table already knows that this silo is dead
                if (entry.Status == SiloStatus.Dead)
                {
                    // try update our local table and notify
                    bool changed = membershipOracleData.TryUpdateStatusAndNotify(entry);
                    if (changed)
                    {
                        UpdateListOfProbedSilos();
                    }
                    return true;
                }

                List<Tuple<SiloAddress, DateTime>> allVotes = entry.SuspectTimes ?? new List<Tuple<SiloAddress, DateTime>>();

                // get all valid (non-expired) votes
                List<Tuple<SiloAddress, DateTime>> freshVotes = entry.GetFreshVotes(orleansConfig.Globals.DeathVoteExpirationTimeout);

                if (logger.IsVerbose2) logger.Verbose2("-Current number of fresh Voters for {0} is {1}", silo.ToLongString(), freshVotes.Count);

                if (freshVotes.Count >= orleansConfig.Globals.NumVotesForDeathDeclaration)
                {
                    // this should not happen ...
                    string str = String.Format("-Silo {0} is suspected by {1} which is more or equal than {2}, but is not marked as dead. This is a bug!!!",
                        entry.SiloAddress.ToLongString(), freshVotes.Count, orleansConfig.Globals.NumVotesForDeathDeclaration);
                    logger.Error(ErrorCode.Runtime_Error_100053, str);
                    KillMyselfLocally("Found a bug 1! Will commit suicide.");
                    return false;
                }

                // handle the corner case when the number of active silos is very small (then my only vote is enough)
                int activeSilos = membershipOracleData.GetSiloStatuses(status => status.Equals(SiloStatus.Active), true).Count;
                // find if I have already voted
                int myVoteIndex = freshVotes.FindIndex(voter => MyAddress.Equals(voter.Item1));

                // Try to kill:
                //  if there is NumVotesForDeathDeclaration votes (including me) to kill - kill.
                //  otherwise, if there is a majority of nodes (including me) voting to kill – kill.
                bool declareDead = false;
                int myAdditionalVote = myVoteIndex == -1 ? 1 : 0;
                if (freshVotes.Count + myAdditionalVote >= orleansConfig.Globals.NumVotesForDeathDeclaration)
                {
                    declareDead = true;
                }
                if (freshVotes.Count + myAdditionalVote >= (activeSilos + 1) / 2)
                {
                    declareDead = true;
                }
                if (declareDead)
                {
                    // kick this silo off
                    logger.Info(ErrorCode.MBRMarkingAsDead, 
                        "-Going to mark silo {0} as DEAD in the table #1. I am the last voter: #freshVotes={1}, myVoteIndex = {2}, NumVotesForDeathDeclaration={3} , #activeSilos={4}, suspect list={5}",
                                entry.SiloAddress.ToLongString(), 
                                freshVotes.Count, 
                                myVoteIndex, 
                                orleansConfig.Globals.NumVotesForDeathDeclaration, 
                                activeSilos, 
                                PrintSuspectList(allVotes));
                    return DeclareDead(entry, eTag, table.Version);
                }

                // we still do not have enough votes - need to vote                             
                // find voting place:
                //      update my vote, if I voted previously
                //      OR if the list is not full - just add a new vote
                //      OR overwrite the oldest entry.
                int indexToWrite = allVotes.FindIndex(voter => MyAddress.Equals(voter.Item1));
                if (indexToWrite == -1)
                {
                    // My vote is not recorded. Find the most outdated vote if the list is full, and overwrite it
                    if (allVotes.Count >= orleansConfig.Globals.NumVotesForDeathDeclaration) // if the list is full
                    {
                        // The list is full.
                        DateTime minVoteTime = allVotes.Min(voter => voter.Item2); // pick the most outdated vote
                        indexToWrite = allVotes.FindIndex(voter => voter.Item2.Equals(minVoteTime));
                    }
                }
                var prevList = allVotes.ToList(); // take a copy
                var newEntry = new Tuple<SiloAddress, DateTime>(MyAddress, DateTime.UtcNow);
                if (indexToWrite == -1)
                {
                    // if did not find specific place to write (the list is not full), just add a new element to the list
                    entry.AddSuspector(newEntry);
                }
                else
                {
                    entry.SuspectTimes[indexToWrite] = newEntry;
                }
                logger.Info(ErrorCode.MBRVotingForKill,
                                    "-Putting my vote to mark silo {0} as DEAD #2. Previous suspect list is {1}, trying to update to {2}, eTag={3}, freshVotes is {4}",
                                    entry.SiloAddress.ToLongString(), 
                                    PrintSuspectList(prevList), 
                                    PrintSuspectList(entry.SuspectTimes),
                                    eTag,
                                    PrintSuspectList(freshVotes));
                // GKTODO: retry if we fail to update here.
                return AsyncValue.FromTask(membershipTableProvider.UpdateRow(entry, eTag, table.Version.Next()));
            });
        }

        private AsyncValue<bool> DeclareDead(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (orleansConfig.Globals.LivenessEnabled)
            {
                // add the killer (myself) to the suspect list, for easier diagnosis later on.
                entry.AddSuspector(new Tuple<SiloAddress, DateTime>(MyAddress, DateTime.UtcNow));

                if (logger.IsVerbose) logger.Verbose("-Going to DeclareDead silo {0} in the table. About to write entry {1}.", entry.SiloAddress.ToLongString(), entry.ToFullString());
                entry.Status = SiloStatus.Dead;
                return AsyncValue.FromTask(membershipTableProvider.UpdateRow(entry, etag, tableVersion.Next())).ContinueWith(ret =>
                {
                    if (ret)
                    {
                        if (logger.IsVerbose) logger.Verbose("-Successfully updated {0} status to Dead in the MBR table.", entry.SiloAddress.ToLongString());
                        if (!entry.SiloAddress.Endpoint.Equals(MyAddress.Endpoint))
                        {
                            bool changed = membershipOracleData.TryUpdateStatusAndNotify(entry);
                            if (changed)
                            {
                                UpdateListOfProbedSilos();
                            }
                        }
                        GossipToOthers(entry.SiloAddress, entry.Status);
                        return true;
                    }
                    else
                    {
                        logger.Info(ErrorCode.MBRMarkDeadWriteFailed, "-Failed to update {0} status to Dead in the MBR table, due to write conflicts. Will retry.", entry.SiloAddress.ToLongString());
                        return false;
                    }
                });
            }
            else
            {
                logger.Info(ErrorCode.MBRCantWriteLivenessDisabled, "-Want to mark silo {0} as DEAD, but will ignore because Liveness is Disabled.", entry.SiloAddress.ToLongString());
                return true;
            }
        }

        private static string PrintSuspectList(List<Tuple<SiloAddress, DateTime>> list)
        {
            return Utils.IEnumerableToString(list, (Tuple<SiloAddress, DateTime> t) => String.Format("<{0}, {1}>", t.Item1.ToLongString(), Logger.PrintDate(t.Item2)));
        }

        private void DisposeTimers()
        {
            if (timerGetTableUpdates != null)
            {
                timerGetTableUpdates.Dispose();
                timerGetTableUpdates = null;
            }
            if (timerProbeOtherSilos != null)
            {
                timerProbeOtherSilos.Dispose();
                timerProbeOtherSilos = null;
            }
            if (timerIAmAliveUpdateInTable != null)
            {
                timerIAmAliveUpdateInTable.Dispose();
                timerIAmAliveUpdateInTable = null;
            }
        }

        #region Implementation of IHealthCheckParticipant

        public bool CheckHealth(DateTime lastCheckTime)
        {
            bool ok = (timerGetTableUpdates != null) && timerGetTableUpdates.CheckTimerFreeze(lastCheckTime);
            ok &= (timerProbeOtherSilos != null) && timerProbeOtherSilos.CheckTimerFreeze(lastCheckTime);
            ok &= (timerIAmAliveUpdateInTable != null) && timerIAmAliveUpdateInTable.CheckTimerFreeze(lastCheckTime);
            return ok;
        }

        #endregion

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            if (logger.IsVerbose) logger.Verbose("-BeginShutdown");

            shutdownCanFinish = false;
            Action canFinish = () =>
            {
                shutdownCanFinish = true;
                if (logger.IsVerbose) logger.Verbose("-BeginShutdown done.");
                tryFinishShutdown();
            };
            AsyncCompletion.FromTask(ShutDown()).ContinueWith(canFinish).LogErrors(logger, ErrorCode.MBRShutDownFailure).Ignore();
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
            AsyncCompletion.FromTask(KillMyself()).LogErrors(logger, ErrorCode.MBRKillMyselfFailure).Ignore();
        }

        public SiloShutdownPhase Phase { get { return SiloShutdownPhase.Late; } }

        #endregion

        private IRemoteSiloStatusOracle GetOracleReference(SiloAddress silo)
        {
            return RemoteSiloStatusOracleFactory.GetSystemTarget(Constants.MembershipOracleId, silo);
        }
    }
}
