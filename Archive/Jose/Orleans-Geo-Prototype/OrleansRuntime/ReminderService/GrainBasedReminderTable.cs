using System;
using System.Threading.Tasks;



namespace Orleans.Runtime.ReminderService
{
    [Reentrant]
    internal class GrainBasedReminderTable : GrainBase, IReminderTable
    {
        private InMemoryRemindersTable remTable;
        private Logger logger;

        public override Task ActivateAsync()
        {
            logger = Logger.GetLogger("GrainBasedReminderTable", Logger.LoggerType.Runtime);
            logger.Info(ErrorCode.RS_GrainBasedTable1, "GrainBasedReminderTable Activated.");
            remTable = new InMemoryRemindersTable();
            return TaskDone.Done;
        }

        public Task<ReminderTableData> ReadRows(GrainId key)
        {
            return Task.FromResult(remTable.ReadRows(key));
        }

        public Task<ReminderTableData> ReadRows(IRingRange range)
        {
            //return remTable.ReadRows(start, end); // this is enough ... the following is just for debugging
            ReminderTableData t = remTable.ReadRows(range);
            logger.Verbose("Read {0} reminders from memory: {1}, {2}", t.Reminders.Count, Environment.NewLine, Utils.IEnumerableToString(t.Reminders));
            return Task.FromResult(t);
        }

        public Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            return Task.FromResult(remTable.ReadRow(grainId, reminderName));
        }

        public Task<string> UpsertRow(ReminderEntry entry)
        {
            return remTable.UpsertRow(entry).AsTask();
        }

        /// <summary>
        /// Remove a row from the table
        /// </summary>
        /// <param name="grainId"></param>
        /// <param name="reminderName"></param>
        /// <param name="eTag"></param>
        /// <returns>true if a row with <paramref name="grainId"/> and <paramref name="reminderName"/> existed and was removed successfully, false otherwise</returns>
        public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            if (logger.IsVerbose) logger.Verbose("RemoveRow entry grainId = {0}, reminderName = {1}, eTag = {2}", grainId, reminderName, eTag);
            bool result = remTable.RemoveRow(grainId, reminderName, eTag);
            if (result == false)
                logger.Warn(ErrorCode.RS_Table_Remove, "RemoveRow of grainId = {0}, reminderName = {1}, eTag = {2}. Table now is {3}", grainId, reminderName, eTag, remTable.ReadAll());
            return Task.FromResult(result);
        }

        ///// <summary>
        ///// Insert <paramref name="entry"/> if a reminder with the same 'grainId' and 'reminderName' as <paramref name="entry"/> doesn't already exist
        ///// The insert is successful (returns true) only if a same reminder row didn't already exist in the table
        ///// </summary>
        ///// <param name="entry">the row to insert</param>
        ///// <returns>true if such a reminder didn't already exist, false otherwise</returns>
        //public AsyncValue<bool> InsertRowIfNotExists(ReminderEntry entry)
        //{
        //    if (logger.IsVerbose) logger.Verbose("InsertRow entry = {0}", entry.ToFullString());
        //    bool result = remTable.InsertRowIfNotExists(entry);
        //    if (result == false)
        //        logger.Warn(ErrorCode.RS_Table_Insert, "Insert of {0} and failed. Table now is {1}", entry.ToFullString(), remTable.ReadAll());
        //    return result;
        //}

        ///// <summary>
        ///// Remove all rows (reminders) with grainId equal to <paramref name="grainId"/>
        ///// </summary>
        ///// <param name="grainId"></param>
        ///// <returns>true if rows for grainId successfully removed, false otherwise</returns>
        //public AsyncValue<bool> RemoveRows(GrainId grainId)
        //{
        //    return remTable.RemoveRows(grainId);
        //}

        public Task Clear()
        {
            remTable.Reset();
            return TaskDone.Done;
        }
    }
}
