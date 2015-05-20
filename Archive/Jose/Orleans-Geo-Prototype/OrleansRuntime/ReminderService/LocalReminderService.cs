using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Orleans.Counters;
using Orleans.Runtime;

using Orleans.Scheduler;


namespace Orleans.Runtime.ReminderService
{
    internal class LocalReminderService : SystemTarget, IReminderService, IRingRangeListener
    {
        public enum ReminderServiceStatus
        {
            Booting = 0,
            Started,
            Stopped,
        }

        private readonly HashSet<LocalReminderData> localReminders;
        private readonly IConsistentRingProvider ring;
        private readonly IReminderTable reminderTable;
        private readonly OrleansTaskScheduler Scheduler;
        private ReminderServiceStatus status;
        private IRingRange myRange;
        private long localTableSequence;
        private readonly Logger logger;
        private OrleansTimerInsideGrain listRefresher; // timer that refreshes our list of reminders to reflect global reminder table
        private readonly AverageTimeSpanStatistic tardinessStat;
        private IntValueStatistic reminderCountStat;
        private readonly CounterStatistic ticksDeliveredStat;
        private TaskCompletionSource<bool> startedTask;

        internal LocalReminderService(SiloAddress addr, GrainId id, IConsistentRingProvider ring, OrleansTaskScheduler localScheduler, IReminderTable reminderTable)
            : base(id, addr)
        {
            this.logger = Logger.GetLogger("ReminderService", Logger.LoggerType.Runtime);

            this.localReminders = new HashSet<LocalReminderData>();
            this.ring = ring;
            this.Scheduler = localScheduler;
            this.reminderTable = reminderTable;
            this.status = ReminderServiceStatus.Booting;
            this.myRange = null;
            this.localTableSequence = 0;
            this.tardinessStat = 
                AverageTimeSpanStatistic.FindOrCreate(StatNames.STAT_REMINDERS_AVERAGE_TARDINESS_SECONDS);
            this.reminderCountStat = 
                IntValueStatistic.FindOrCreate(
                    StatNames.STAT_REMINDERS_COUNTERS_ACTIVE,
                    () =>
                        this.localReminders.Count);
            this.ticksDeliveredStat = 
                CounterStatistic.FindOrCreate(
                    StatNames.STAT_REMINDERS_COUNTERS_TICKS_DELIVERED, 
                    useDelta: true);
            this.startedTask = new TaskCompletionSource<bool>();
        }

        #region Public methods

        /// <summary>
        /// Attempt to retrieve reminders, that are my responsibility, from the global reminder table when starting this silo (reminder service instance)
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="waitForTableToInit"></param>
        /// <returns></returns>
        public async Task Start(bool waitForTableToInit)
        {
            if (waitForTableToInit)
            {
                await WaitForTableToInit();
            }

            myRange = ring.GetMyRange();
            logger.Info("Starting reminder system target on: {0} x{1,8:X8}, with range {2}", CurrentSilo, CurrentSilo.GetConsistentHashCode(), myRange);

            await ReadAndUpdateReminders().AsTask();

            logger.Info("Reminder system target started OK on: {0} x{1,8:X8}, with range {2}", CurrentSilo, CurrentSilo.GetConsistentHashCode(), myRange);
            status = ReminderServiceStatus.Started;
            startedTask.TrySetResult(true);
            SafeRandom random = new SafeRandom();
            TimeSpan dueTime = random.NextTimeSpan(Constants.REFRESH_REMINDER_LIST);
            listRefresher =
                OrleansTimerInsideGrain.FromTaskCallback(
                    _ =>
                        ReadAndUpdateReminders().AsTask(),
                    null,
                    dueTime,
                    Constants.REFRESH_REMINDER_LIST,
                    name: "ReminderService.ReminderListRefresher",
                    options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
            listRefresher.Start();
        }

        private async Task WaitForTableToInit()
        {
            logger.Info("-Waiting for reminder table to init.");
            TimeSpan timespan = TimeSpan.FromMilliseconds(500);
            // This is a hack to enable primary node to start fully before secondaries.
            // Secondary silos waits until GrainBasedReminderTable is created. 
            for (int i = 0; i < 10; i++)
            {
                bool needToWait = false;
                try
                {
                    ReminderTableData table = await reminderTable.ReadRows(RangeFactory.CreateFullRange()).WithTimeout(timespan);
                    logger.Info(ErrorCode.RS_TableGrainInit1, "-Connected to reminder table provider on the primary silo.");
                    break;
                }
                catch (Exception exc)
                {
                    Type type = exc.GetBaseException().GetType();
                    if (type.Equals(typeof(TimeoutException)) || type.Equals(typeof(OrleansException)))
                    {
                        logger.Info(ErrorCode.RS_TableGrainInit2, "-Waiting for reminder table provider to initialize ({0}). Going to sleep for {1} and re-try to reconnect.", i, timespan);
                        Thread.Sleep(timespan);
                        needToWait = true;
                    }
                    else
                    {
                        logger.Info(ErrorCode.RS_TableGrainInit3, "-Reminder table provider failed to initialize. Giving up.");
                        throw;
                    }
                }
                if (needToWait)
                {
                    await Task.Delay(timespan);
                }
            }
        }

        public Task Stop()
        {
            logger.Info("-Stop");
            status = ReminderServiceStatus.Stopped;

            if (listRefresher != null)
            {
                listRefresher.Dispose();
                listRefresher = null;
            }
            foreach (LocalReminderData r in localReminders)
            {
                r.StopReminder(logger);
            }

            // for a graceful shutdown, also handover reminder responsibilities to new owner, and update the ReminderTable
            // currently, this is taken care of by periodically reading the reminder table
            return TaskDone.Done;
        }

        public async Task<IOrleansReminder> RegisterOrUpdateReminder(GrainId grainId, string reminderName, TimeSpan dueTime, TimeSpan period)
        {
            ReminderEntry entry = new ReminderEntry
            {
                GrainId = grainId,
                ReminderName = reminderName,
                StartAt = DateTime.UtcNow.Add(dueTime),
                Period = period,
                //ETag = eTag, the table will decide what eTag to give to this reminder
            };

            if (logger.IsInfo) logger.Info("Adding reminder {0}", entry.ToString());

            await DoResponsibilitySanityCheck(grainId, "RegisterReminder");

            var newEtag = await reminderTable.UpsertRow(entry); //InsertRowIfNotExists(e)

            if (newEtag != null)
            {
                if (logger.IsVerbose) logger.Verbose("Registered reminder {0} {1} in table with eTag {2}, assigned localSequence {3}", grainId.ToStringWithHashCode(), reminderName, newEtag, localTableSequence);
                entry.ETag = newEtag;
                StartAndAddTimer(entry);
                if (logger.IsVerbose3) PrintReminders();
                return new OrleansReminderData(grainId, reminderName, newEtag) as IOrleansReminder;
            }
            else
            {
                string msg = string.Format("Could not register reminder {0} to reminder table due to a race. Please try again later.", reminderName);
                logger.Error(ErrorCode.RS_Register_TableError, msg);
                throw new ReminderException(msg);
            }
        }

        /// <summary>
        /// Stop the reminder locally, and remove it from the external storage system
        /// </summary>
        /// <param name="reminder"></param>
        /// <returns></returns>
        public async Task UnregisterReminder(IOrleansReminder reminder)
        {
            OrleansReminderData remData = (OrleansReminderData)reminder;
            GrainId grainId = remData.GrainId;
            string reminderName = remData.ReminderName;
            string eTag = remData.ETag;

            logger.Info("Removing reminder {0} {1}, LocalTableSequence {2}", grainId.ToStringWithHashCode(), reminderName, localTableSequence);

            await DoResponsibilitySanityCheck(grainId, "RemoveReminder");

            // it may happen that we dont have this reminder locally ... even then, we attempt to remove the reminder from the reminder 
            // table ... the periodic mechanism will stop this reminder at any silo's LocalReminderService that might have this reminder locally

            // remove from persistent/memory store
            var success = await reminderTable.RemoveRow(grainId, reminderName, eTag);
            if (success)
            {
                bool removed = TryStopPreviousTimer(grainId, reminderName);
                if (removed)
                {
                    logger.Info("Stopped reminder {0}.{1}", grainId.ToStringWithHashCode(), reminderName);
                    if (logger.IsVerbose3) PrintReminders(string.Format("After removing {0}.{1}", grainId.ToStringWithHashCode(), reminderName));
                }
                else
                {
                    // no-op
                    logger.Info("Removed reminder from table which I didn't have locally: {0}.{1}", grainId.ToStringWithHashCode(), reminderName);
                }
            }
            else
            {
                string msg = string.Format("Could not unregister reminder {0}.{1} from the reminder table. Please try again later.", grainId.ToStringWithHashCode(), reminderName);
                logger.Error(ErrorCode.RS_Unregister_TableError, msg);
                throw new ReminderException(msg);
            }
        }

        public async Task<IOrleansReminder> GetReminder(GrainId grainId, string reminderName)
        {
            logger.Info("GrainId={0} ReminderName={1}", grainId.ToStringWithHashCode(), reminderName);
            var entry = await reminderTable.ReadRow(grainId, reminderName);
            return entry.ToIOrleansReminder();
        }

        public async Task<List<IOrleansReminder>> GetReminders(GrainId grainId)
        {
            var tableData = await reminderTable.ReadRows(grainId);
            return tableData.Reminders.Select((entry) => entry.ToIOrleansReminder()).ToList();
        }

        #endregion

        /// <summary>
        /// Attempt to retrieve reminders from the global reminder table
        /// </summary>
        private AsyncCompletion ReadAndUpdateReminders()
        {
            // try to retrieve reminder from all my subranges
            myRange = ring.GetMyRange();
            var acks = new List<AsyncCompletion>();
            foreach (SingleRange range in RangeFactory.GetSubRanges(myRange))
            {
                if (logger.IsVerbose3) logger.Verbose3("Reading rows for range {0}", range);
                acks.Add(ReadTableAndStartTimers(range));
            }
            return AsyncCompletion.JoinAll(acks).ContinueWith(() => { if (logger.IsVerbose3) PrintReminders(); });
        }


        #region Change in membership, e.g., failure of predecessor
        /// <summary>
        /// Actions to take when the range of this silo changes on the ring due to a failure or a join
        /// </summary>
        /// <param name="old">my previous responsibility range</param>
        /// <param name="now">my new/current responsibility range</param>
        /// <param name="increased">True: my responsibility increased, false otherwise</param>
        public void RangeChangeNotification(IRingRange old, IRingRange now, bool increased)
        {
            // run on my own turn & context
            Scheduler.QueueAsyncCompletion(() => OnRangeChange(old, now, increased), this.SchedulingContext).Ignore();
        }

        private AsyncCompletion OnRangeChange(IRingRange oldRange, IRingRange newRange, bool increased)
        {
            logger.Info("My range changed from {0} to {1} increased = {2}", oldRange, newRange, increased);
            myRange = newRange;
            if (status != ReminderServiceStatus.Started)
            {
                return AsyncCompletion.Done;
            }
            return ReadAndUpdateReminders();
        }
        #endregion

        #region Internal implementation methods

        private AsyncCompletion ReadTableAndStartTimers(IRingRange range)
        {
            if (logger.IsVerbose) logger.Verbose("Reading rows from {0}", range.ToString());
            localTableSequence++;
            long cachedSequence = localTableSequence;

            AsyncValue<ReminderTableData> promise = AsyncValue.FromTask(reminderTable.ReadRows(range)); // get all reminders, even the ones we already have
            return promise.ContinueWith((ReminderTableData table) =>
            {
                HashSet<LocalReminderData> remindersNotInTable = new HashSet<LocalReminderData>(localReminders); // shallow copy
                if (logger.IsVerbose) logger.Verbose("For range {0}, I read in {1} reminders from table. LocalTableSequence {2}, CachedSequence {3}", range.ToString(), table.Reminders.Count, localTableSequence, cachedSequence);

                foreach (ReminderEntry entry in table.Reminders)
                {
                    ///var reminder = new LocalReminderData(re);
                    LocalReminderData localRem = localReminders.FirstOrDefault(r => (r.Equals(entry)));
                    if (localRem != default(LocalReminderData)) // exists in table and locally
                    {
                        if (cachedSequence > localRem.LocalSequenceNumber) // info read from table is same or newer than local info
                        {
                            if (localRem.Timer != null) // if ticking
                            {
                                if (logger.IsVerbose2) logger.Verbose2("In table, In local, Old, & Ticking {0}", localRem);
                                // it might happen that our local reminder is different than the one in the table, i.e., eTag is different
                                // if so, stop the local timer for the old reminder, and start again with new info
                                if (!localRem.ETag.Equals(entry.ETag))
                                // this reminder needs a restart
                                {
                                    if (logger.IsVerbose2) logger.Verbose2("{0} Needs a restart", localRem);
                                    localRem.StopReminder(logger);
                                    localReminders.Remove(localRem);
                                    if (ring.GetMyRange().InRange(GrainReference.FromGrainId(entry.GrainId)))
                                    // if its not my responsibility, I shouldn't start it locally
                                    {
                                        StartAndAddTimer(entry);
                                    }
                                }
                            }
                            else // if not ticking
                            {
                                // no-op
                                if (logger.IsVerbose2) logger.Verbose2("In table, In local, Old, & Not Ticking {0}", localRem);
                            }
                        }
                        else // cachedSequence < localRem.LocalSequenceNumber ... // info read from table is older than local info
                        {
                            if (localRem.Timer != null) // if ticking
                            {
                                // no-op
                                if (logger.IsVerbose2) logger.Verbose2("In table, In local, Newer, & Ticking {0}", localRem);
                            }
                            else // if not ticking
                            {
                                // no-op
                                if (logger.IsVerbose2) logger.Verbose2("In table, In local, Newer, & Not Ticking {0}", localRem);
                            }
                        }
                    }
                    else // exists in table, but not locally
                    {
                        if (logger.IsVerbose2) logger.Verbose2("In table, Not in local, {0}", entry);
                        // create and start the reminder
                        if (ring.GetMyRange().InRange(GrainReference.FromGrainId(entry.GrainId))) // if its not my responsibility, I shouldn't start it locally
                        {
                            StartAndAddTimer(entry);
                        }
                    }
                    // keep a track of extra reminders ... this 'reminder' is useful, so remove it from extra list
                    remindersNotInTable.RemoveWhere((LocalReminderData data) => data.Equals(entry));
                } // foreach reminder read from table

                // foreach reminder that is not in global table, but exists locally
                foreach (LocalReminderData reminder in remindersNotInTable)
                {
                    if (cachedSequence < reminder.LocalSequenceNumber)
                    {
                        // no-op
                        if (logger.IsVerbose2) logger.Verbose2("Not in table, In local, Newer, {0}", reminder);
                    }
                    else // cachedSequence > reminder.LocalSequenceNumber
                    {
                        if (logger.IsVerbose2) logger.Verbose2("Not in table, In local, Old, so removing. {0}", reminder);
                        // remove locally
                        reminder.StopReminder(logger);
                        localReminders.Remove(reminder);
                    }
                }
                return AsyncCompletion.Done;
            }).LogErrors(logger, ErrorCode.RS_FailedToReadTableAndStartTimer, "Failed to read rows from table.");
        }

        private void StartAndAddTimer(ReminderEntry entry)
        {
            // it might happen that we already have a local reminder with a different eTag
            // if so, stop the local timer for the old reminder, and start again with new info
            // Note: it can happen here that we restart a reminder that has the same eTag as what we just registered ... its a rare case, and restarting it doesn't hurt, so we don't check for it
            LocalReminderData prevReminder = localReminders.FirstOrDefault(r => r.Equals(entry));
            if (prevReminder != default(LocalReminderData)) // if found locally
            {
                logger.Info("Unregistering reminder {0} as it is different than newly registered reminder {1}", prevReminder, entry);
                prevReminder.StopReminder(logger);
                localReminders.Remove(prevReminder);
            }

            LocalReminderData newReminder = new LocalReminderData(entry);
            localTableSequence++;
            newReminder.LocalSequenceNumber = localTableSequence;
            localReminders.Add(newReminder);
            newReminder.StartTimer(AsyncTimerCallback, logger);
            logger.Info("Started reminder {0}.", entry.ToString());
        }

        // stop without removing it. will remove later.
        private bool TryStopPreviousTimer(GrainId grainId, string reminderName)
        {
            // we stop the locally running timer for this reminder
            LocalReminderData localRem = localReminders.FirstOrDefault(r => r.GrainId.Equals(grainId) && r.ReminderName.Equals(reminderName));
            if (localRem != default(LocalReminderData)) // if we have it locally
            {
                localTableSequence++; // move to next sequence
                localRem.LocalSequenceNumber = localTableSequence;
                localRem.StopReminder(logger);
                return true;
            }
            return false;
        }

        #endregion

        /// <summary>
        /// Local timer expired ... notify it as a 'tick' to the grain who registered this reminder
        /// </summary>
        /// <param name="rem">Reminder that this timeout represents</param>
        private async Task AsyncTimerCallback(object rem)
        {
            var reminder = (LocalReminderData)rem;

            if (!localReminders.Contains(reminder) // we have already stopped this timer 
                || reminder.Timer == null) // this timer was unregistered, and is waiting to be gc-ied
                return;

            await reminder.OnTimerTick(this.tardinessStat, this.logger);
            this.ticksDeliveredStat.Increment();
        }

        #region Utility (less useful) methods

        private async Task DoResponsibilitySanityCheck(GrainId grainId, string debugInfo)
        {
            if (status != ReminderServiceStatus.Started)
            {
                // TODO: TMS if this service hasn't started yet, we can't route register/unregister requests to someone else as we are the owner
                // should we queue the request to execute after a delay?
                // GK: TODO: queue the request to execute after we go inot Started state.
                //string err = string.Format("Reminder service has not started yet, can't process request: '{0}'.", debugInfo);
                //logger.Warn(ErrorCode.RS_Not_Started, err);
                //throw new ReminderException(err);
                await startedTask.Task;
            }
            if (!myRange.InRange(GrainReference.FromGrainId(grainId)))
            {
                logger.Warn(ErrorCode.RS_NotResponsible, "I shouldn't have received request '{0}' for {1}. It is not in my responsibility range: {2}", debugInfo, grainId.ToStringWithHashCode(), myRange);
                // TODO: TMS for now, we still let the caller proceed without throwing an exception... the periodical mechanism will take care of reminders being registered at the wrong silo
                // otherwise, we can either reject the request, or re-route the request
            }
        }

        // Note: The list of reminders can be huge in production!
        private void PrintReminders(string msg = null)
        {
            if (logger.IsVerbose3)
            {
                String str = String.Format("{0}{1}{2}",
                    (msg ?? "Current list of reminders:"),
                    Environment.NewLine,
                    Utils.IEnumerableToString(localReminders, null, Environment.NewLine));
                logger.Verbose3(str);
            }
        }

        #endregion

        #region The Reminder Object

        internal class LocalReminderData : IEquatable<LocalReminderData> // merge this with IReminderReceiver?
        {
            internal readonly GrainId GrainId;
            internal readonly DateTime FirstTickTime; // time for the first tick of this reminder
            internal readonly TimeSpan Period;
            internal readonly IRemindable GrainRef;
            internal string ETag;
            public string ReminderName { get; private set; }
            internal OrleansTimerInsideGrain Timer;
            internal long LocalSequenceNumber; // locally, we use this for resolving races between the periodic table reader, and any concurrent local register/unregister requests
            private Stopwatch stopwatch;

            internal LocalReminderData(ReminderEntry entry)
            {
                this.GrainId = entry.GrainId;
                this.ReminderName = entry.ReminderName;
                this.FirstTickTime = entry.StartAt;
                this.Period = entry.Period;
                this.GrainRef = RemindableFactory.Cast(GrainReference.FromGrainId(GrainId));
                this.ETag = entry.ETag;
                this.LocalSequenceNumber = -1;
            }

            public void StartTimer(Func<object, Task> asyncCallback, Logger logger)
            {
                StopReminder(logger); // just to make sure.
                TimeSpan dueTimeSpan = this.CalculateDueTime();
                this.Timer = OrleansTimerInsideGrain.FromTaskCallback(asyncCallback, this, dueTimeSpan, this.Period, name: this.ReminderName, options: OrleansTimerInsideGrain.OptionFlags.CountTicks);
                if (logger.IsVerbose) logger.Verbose("Reminder {0}, Due time{1}", this, dueTimeSpan);
                this.Timer.Start();
            }

            public void StopReminder(Logger logger)
            {
                if (this.Timer != null)
                {
                    this.Timer.Dispose();
                }
                this.Timer = null;
            }

            private TimeSpan CalculateDueTime()
            {
                TimeSpan dueTimeSpan;
                DateTime now = DateTime.UtcNow;
                if (now < FirstTickTime) // if the time for first tick hasn't passed yet
                {
                    dueTimeSpan = FirstTickTime.Subtract(now); // then duetime is duration between now and the first tick time
                }
                else // the first tick happened in the past ... compute duetime based on the first tick time, and period
                {
                    // formula used:
                    // due = period - 'time passed since last tick (==sinceLast)'
                    // due = period - ((Now - FirstTickTime) % period)
                    // explanation of formula:
                    // (Now - FirstTickTime) => gives amount of time since first tick happened
                    // (Now - FirstTickTime) % period => gives amount of time passed since the last tick should have triggered
                    TimeSpan sinceFirstTick = now.Subtract(FirstTickTime);
                    TimeSpan sinceLastTick = TimeSpan.FromTicks(sinceFirstTick.Ticks % Period.Ticks);
                    dueTimeSpan = Period.Subtract(sinceLastTick);
                    // in corner cases, dueTime can be equal to period ... so, take another mod
                    dueTimeSpan = TimeSpan.FromTicks(dueTimeSpan.Ticks % Period.Ticks);
                }
                return dueTimeSpan;
            }

            public async Task OnTimerTick(AverageTimeSpanStatistic tardinessStat, Logger logger)
            {
                var before = DateTime.UtcNow;
                var status = TickStatus.NewStruct(FirstTickTime, Period, before);

                if (logger.IsVerbose2) 
                    logger.Verbose2(
                        "Triggering tick for Grain {0}, name {1}, status {2}, now {3}, period {4} sec",
                        GrainId.ToStringWithHashCode(), 
                        ReminderName, 
                        status, 
                        before, 
                        Period.TotalSeconds);

                try
                {
                    if (null != this.stopwatch)
                    {
                        this.stopwatch.Stop();
                        var tardiness = this.stopwatch.Elapsed - this.Period;
                        tardinessStat.AddSample(Math.Max(0, tardiness.Ticks));
                    }
                    await GrainRef.ReceiveReminder(ReminderName, status);
                    if (null == stopwatch)
                        stopwatch = new Stopwatch();
                    stopwatch.Restart();

                    var after = DateTime.UtcNow;
                    if (logger.IsVerbose2) 
                        logger.Verbose2(
                            "Tick triggered and handled by Grain {0}, name {1}, dt {2} sec, next@~ {3}", 
                            GrainId.ToStringWithHashCode(),
                            ReminderName, 
                            (after - before).TotalSeconds, 
                            // [mlr] the next tick isn't actually scheduled until we return control to
                            // AsyncSafeTimer but we can approximate it by adding the period of the reminder
                            // to the after time.
                            after + Period);
                }
                catch (Exception exc)
                {
                    var after = DateTime.UtcNow;
                    logger.Error(
                        ErrorCode.RS_Tick_Delivery_Error, 
                        string.Format(
                            "Could not deliver reminder for Grain {0}, ReminderName {1}, next@~ {2} : {3}, ", 
                            GrainId.ToStringWithHashCode(),
                            ReminderName,
                            after + Period,
                            exc.GetBaseException()));
                    // TODO: TMS see what to do with repeated failures to deliver a reminder's ticks
                }
            }

            #region IEquatable<Reminder> Members

            /// <summary>
            /// If the GrainId and Name are the same, then the Reminder objects are considered to be the same
            /// </summary>
            /// <param name="other"></param>
            /// <returns></returns>
            public bool Equals(LocalReminderData other)
            {
                return other != null && (GrainId.Equals(other.GrainId) && ReminderName.Equals(other.ReminderName));
            }

            #endregion

            public override bool Equals(object obj)
            {
                return Equals(obj as LocalReminderData);
            }

            public bool Equals(ReminderEntry other)
            {
                return other != null && (GrainId.Equals(other.GrainId) && ReminderName.Equals(other.ReminderName));
            }

            public override int GetHashCode()
            {
                return GrainId.GetUniformHashCode() ^ ReminderName.GetHashCode();
            }

            public override string ToString()
            {
                return string.Format("[{0}, {1}, {2}, {3}, {4}, {5}, {6}]",
                                        ReminderName,
                                        GrainId,
                                        Period,
                                        Logger.PrintDate(FirstTickTime),
                                        ETag,
                                        LocalSequenceNumber,
                                        Timer == null ? "Not_ticking" : "Ticking");
            }
        }
        #endregion
    }
}
