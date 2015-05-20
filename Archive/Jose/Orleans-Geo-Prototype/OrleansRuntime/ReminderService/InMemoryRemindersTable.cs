using System;
using System.Collections.Generic;
using System.Linq;



namespace Orleans.Runtime.ReminderService
{
    [Serializable]
    internal class InMemoryRemindersTable
    {
        // key: GrainId
        // value: V
        //      V.key: ReminderName
        //      V.Value: ReminderEntry
        private Dictionary<GrainId, Dictionary<string, ReminderEntry>> reminderTable;

        // in our first version, we do not support 'updates', so we aren't using these
        // enable after adding updates ... even then, you will probably only need etags per row, not the whole
        // table version, as each read/insert/update should touch & depend on only one row at a time
        //internal TableVersion TableVersion;
        //private long LastETagCounter;

        [NonSerialized]
        Logger logger = Logger.GetLogger("InMemoryReminderTable", Logger.LoggerType.Runtime);

        public InMemoryRemindersTable()
        {
            Reset();
        }

        public ReminderTableData ReadRows(GrainId key)
        {
            Dictionary<string, ReminderEntry> reminders;
            reminderTable.TryGetValue(key, out reminders);
            if (reminders == null)
                return new ReminderTableData();

            ReminderTableData r = new ReminderTableData(reminders.Values.AsList());
            return r;
        }
        
        /// <summary>
        /// Return all rows that have their GrainId's.GetUniformHashCode() in the range (start, end]
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public ReminderTableData ReadRows(IRingRange range)
        {
            IEnumerable<GrainId> keys = reminderTable.Keys.Where(key => range.InRange(GrainReference.FromGrainId(key)));
            //if (start < end)
            //{
            //    keys = reminderTable.Keys.Where(key => (key.GetUniformHashCode() > start && key.GetUniformHashCode() <= end));
            //}
            //else
            //{
            //    keys = reminderTable.Keys.Where(key => (key.GetUniformHashCode() > start || key.GetUniformHashCode() <= end));
            //}
      
            // is there a sleaker way of doing this in C#?
            List<ReminderEntry> list = new List<ReminderEntry>();
            foreach (GrainId k in keys)
            {
                list.AddRange(reminderTable[k].Values);
            }

            logger.Verbose3("Selected {0} out of {1} reminders from memory for {2}. List is: {3}{4}", list.Count, reminderTable.Count, range.ToString(), 
                Environment.NewLine, Utils.IEnumerableToString(list, e => e.ToString()));

            return new ReminderTableData(list);
        }
        
        /// <summary>
        /// Return all rows that have their GrainId's.GetUniformHashCode() in the range (start, end]
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="reminderName"></param>
        /// <returns></returns>
        public ReminderEntry ReadRow(GrainId grainId, string reminderName)
        {
            ReminderEntry r = reminderTable[grainId][reminderName];
            if (logger.IsVerbose3) logger.Verbose3("Read for grain {0} reminder {1} row {2}", grainId, reminderName, r.ToFullString());
            return r;
        }

        public AsyncValue<string> UpsertRow(ReminderEntry entry)
        {
            entry.ETag = Guid.NewGuid().ToString();
            Dictionary<string, ReminderEntry> d;
            if (!reminderTable.ContainsKey(entry.GrainId))
            {
                d = new Dictionary<string, ReminderEntry>();
                reminderTable.Add(entry.GrainId, d);
            }
            d = reminderTable[entry.GrainId];

            ReminderEntry old; // tracing purposes only
            d.TryGetValue(entry.ReminderName, out old); // tracing purposes only
            // add or over-write
            d[entry.ReminderName] = entry;
            if (logger.IsVerbose3) logger.Verbose3("Upserted entry {0}, replaced {1}", entry, old);
            return entry.ETag;
        }
        
        /// <summary>
        /// Remove a row from the table
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="reminderName"></param>
        /// /// <param name="eTag"></param>
        /// <returns>true if a row with <paramref name="grainId"/> and <paramref name="reminderName"/> existed and was removed successfully, false otherwise</returns>
        public bool RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            Dictionary<string, ReminderEntry> data = null;
            ReminderEntry e = null;

            // assuming the calling grain executes one call at a time, so no need to lock 
            reminderTable.TryGetValue(grainId, out data);
            if (data == null)
            {
                return false;
            }

            data.TryGetValue(reminderName, out e); // check if eTag matches
            if (e == null || !e.ETag.Equals(eTag))
            {
                return false;
            }

            if (!data.Remove(reminderName))
            {
                return false;
            }

            if (data.Count == 0)
            {
                return reminderTable.Remove(grainId);
            }
            return true;
        }

        //[ReadOnly]
        //AsyncValue<ReminderTableData> ReadAll();

        ///// <summary>
        ///// Insert <paramref name="entry"/> if a reminder with the same 'grainId' and 'reminderName' as <paramref name="entry"/> doesn't already exist
        ///// The insert is successful (returns true) only if a same reminder row didn't already exist in the table
        ///// </summary>
        ///// <param name="entry">the row to insert</param>
        ///// <returns>true if such a reminder didn't already exist, false otherwise</returns>
        //public bool InsertRowIfNotExists(ReminderEntry entry)
        //{
        //    Dictionary<string, ReminderEntry> data = null;
        //    reminderTable.TryGetValue(entry.GrainId, out data);
        //    if (data == null)
        //    {
        //        data = new Dictionary<string, ReminderEntry>();
        //    }
        //    if (!data.ContainsKey(entry.ReminderName))
        //    {
        //        data.Add(entry.ReminderName, entry);
        //        reminderTable[entry.GrainId] = data;
        //        return true;
        //    }
        //    return false;
        //}

        ///// <summary>
        ///// Remove all rows (reminders) with grainId equal to <paramref name="grainId"/>
        ///// </summary>
        ///// <param name="grainId"></param>
        ///// <returns>true if rows for grainId successfully removed, false otherwise</returns>
        //public bool RemoveRows(GrainId grainId)
        //{
        //    if (logger.IsVerbose3) logger.Verbose3("Removing {0} reminders for grain {1}. List removed: {2}", reminderTable[grainId] == null ? 0 : reminderTable[grainId].Count, grainId,
        //                            Utils.IEnumerableToString(reminderTable[grainId]));
        //    return reminderTable.Remove(grainId);
        //}

        // use only for internal printing during testing ... the reminder table can be huge in a real deployment!
        public ReminderTableData ReadAll()
        {
            // is there a sleaker way of doing this in C#?
            List<ReminderEntry> list = new List<ReminderEntry>();
            foreach (GrainId k in reminderTable.Keys)
            {
                list.AddRange(reminderTable[k].Values);
            }            
            return new ReminderTableData(list);
        }

        public void Reset()
        {
            reminderTable = new Dictionary<GrainId, Dictionary<string, ReminderEntry>>();
        }
    }
}
