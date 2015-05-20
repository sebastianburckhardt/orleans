using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;


namespace Orleans
{
    #region The reminder service interface
    internal interface IReminderService : ISystemTarget
    {
        // TODO: TMS is there a better way of doing start and stop? ... check other SystemTargets
        Task Start(bool waitForTableToInit);
        Task Stop();

        // the following are for usage by the client grain / InsideGrainClient

        /// <summary>
        /// Register a persistent reminder
        /// </summary>
        /// <param name="destination"></param>
        /// <param name="grainId"></param>
        /// <param name="reminderName"></param>
        /// <param name="dueTime"></param>
        /// <param name="period"></param>
        /// <returns></returns>
        Task<IOrleansReminder> RegisterOrUpdateReminder(GrainId grainId, string reminderName, TimeSpan dueTime, TimeSpan period);
        
        Task UnregisterReminder(IOrleansReminder reminder);

        Task<IOrleansReminder> GetReminder(GrainId grainId, string reminderName);

        Task<List<IOrleansReminder>> GetReminders(GrainId grainId);
    }
    #endregion

    #region The interface that the holder of the reminder table should implement
    [Unordered]
    internal interface IReminderTable : IGrain
    {
        Task<ReminderTableData> ReadRows(GrainId key);

        /// <summary>
        /// Return all rows that have their GrainId's.GetUniformHashCode() in the range (start, end]
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        Task<ReminderTableData> ReadRows(IRingRange range);

        Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName);

        Task<string> UpsertRow(ReminderEntry entry);

        /// <summary>
        /// Remove a row from the table
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="reminderName"></param>
        /// /// <param name="eTag"></param>
        /// <returns>true if a row with <paramref name="grainId"/> and <paramref name="reminderName"/> existed and was removed successfully, false otherwise</returns>
        Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag);

        //[ReadOnly]
        //AsyncValue<ReminderTableData> ReadAll();

        ///// <summary>
        ///// Insert <paramref name="entry"/> if a reminder with the same 'grainId' and 'reminderName' as <paramref name="entry"/> doesn't already exist
        ///// The insert is successful (returns true) only if a same reminder row didn't already exist in the table
        ///// </summary>
        ///// <param name="entry">the row to insert</param>
        ///// <returns>true if such a reminder didn't already exist, false otherwise</returns>
        //AsyncValue<bool> InsertRowIfNotExists(ReminderEntry entry);

        ///// <summary>
        ///// Remove all rows (reminders) with grainId equal to <paramref name="grainId"/>
        ///// </summary>
        ///// <param name="grainId"></param>
        ///// <returns>true if atleast one row was removed, false otherwise</returns>
        // AsyncValue<bool> RemoveRows(GrainId grainId);

        // future extensions
        //AsyncValue<bool> Exists(GrainId grainId, string reminderName, TableVersion tableVersion);
        //AsyncValue<bool> UpdateEntry();

        Task Clear();
    }
    #endregion

    #region The reminder table

    internal class ReminderTableData
    {
        public List<ReminderEntry> Reminders { get; private set; }

        public ReminderTableData(List<ReminderEntry> list)
        {
            Reminders = new List<ReminderEntry>(list);
        }

        public ReminderTableData(ReminderEntry entry)
        {
            Reminders = new List<ReminderEntry>();
            Reminders.Add(entry);
        }

        public ReminderTableData()
        {
            Reminders = new List<ReminderEntry>();
        }

        public override string ToString()
        {
            return string.Format("{0} reminders: {1}.", Reminders.Count, 
                Utils.IEnumerableToString(Reminders, (e) => e.ToFullString()));
        }
    }

    #endregion

    #region An entry in the reminder table
    [Serializable]
    internal class ReminderEntry
    {
        // 1 & 2 combine to form a unique key, i.e., a reminder is uniquely identified using these two together
        public GrainId GrainId { get; set; }        // 1
        public string ReminderName { get; set; }    // 2

        public DateTime StartAt { get; set; }
        public TimeSpan Period { get; set; }

        public string ETag { get; set; }

        //public DateTime LastTriggedAt { get; set; } // to be used in extentions/future
        //public long SequenceNumber { get; set; } // to be used in extenstions/future

        public static bool COMPACT_RMD_LOGGING = true;

        public override string ToString()
        {
            return string.Format("GrainId={0} ReminderName={1} Period={2}", GrainId.ToStringWithHashCode(), ReminderName, Period);
        }

        internal string ToFullString()
        {
            if (ReminderEntry.COMPACT_RMD_LOGGING)
            {
                return ToString();
            }
            else
            {
                return string.Format("GrainId={0} ReminderName={1} Period={2} StartAt={3} Id={4}", GrainId, ReminderName, Period, Logger.PrintDate(StartAt), ETag);
            }
        }

        internal IOrleansReminder ToIOrleansReminder()
        {
            return new OrleansReminderData(GrainId, ReminderName, ETag);
        }
    }

    #endregion

    [Serializable]
    internal class OrleansReminderData : IOrleansReminder
    {
        public GrainId GrainId { get; private set; }
        public string ReminderName { get; private set; }
        public string ETag { get; private set; }

        internal OrleansReminderData(GrainId grainId, string reminderName, string eTag)
        {
            GrainId = grainId;
            ReminderName = reminderName;
            ETag = eTag;
        }
    }
}
