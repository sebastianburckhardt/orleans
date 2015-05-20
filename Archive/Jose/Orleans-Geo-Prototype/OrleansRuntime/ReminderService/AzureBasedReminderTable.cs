using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;


namespace Orleans.Runtime.ReminderService
{
    internal class AzureBasedReminderTable : IReminderTable
    {
        private readonly Logger logger;
        private RemindersTableManager remTableManager;

        private AzureBasedReminderTable()
        {
            logger = Logger.GetLogger("AzureReminderTable", Logger.LoggerType.Runtime);
        }

        public static async Task<AzureBasedReminderTable> GetAzureBasedReminderTable(string deploymentId, string connectionString)
        {
            AzureBasedReminderTable table = new AzureBasedReminderTable();
            table.remTableManager = await RemindersTableManager.GetManager(deploymentId, connectionString);
            return table;
        }

        // -------------------------------------- Util methods ---------------------------------------------------------------------------

        private static ReminderTableData ConvertFromTableEntryList(List<Tuple<ReminderTableEntry, string>> entries)
        {
            List<ReminderEntry> remEntries = entries.Select(entry => ConvertFromTableEntry(entry.Item1, entry.Item2)).ToList();

            ReminderTableData data = new ReminderTableData(remEntries);
            return data;
        }

        private static ReminderEntry ConvertFromTableEntry(ReminderTableEntry tableEntry, string eTag)
        {
            return new ReminderEntry
            {
                GrainId = GrainId.FromParsableString(tableEntry.GrainId),
                ReminderName = tableEntry.ReminderName,
                StartAt = Logger.ParseDate(tableEntry.StartAt),
                Period = TimeSpan.Parse(tableEntry.Period),
                ETag = eTag,
            };
        }

        private static ReminderTableEntry ConvertToTableEntry(ReminderEntry remEntry, string deploymentId)
        {
            string partitionKey = ReminderTableEntry.ConstructPartitionKey(deploymentId, remEntry.GrainId);
            string rowKey = ReminderTableEntry.ConstructRowKey(remEntry.GrainId, remEntry.ReminderName);

            long consistentHash = unchecked((long)remEntry.GrainId.GetUniformHashCode());

            return new ReminderTableEntry
            {
                PartitionKey = partitionKey,
                RowKey = rowKey,

                DeploymentId = deploymentId,
                GrainId = remEntry.GrainId.ToParsableString(),
                ReminderName = remEntry.ReminderName,

                StartAt = Logger.PrintDate(remEntry.StartAt),
                Period = remEntry.Period.ToString(),

                GrainIdConsistentHash = consistentHash,
                ETag = remEntry.ETag,
            };
        }

        // -------------------------------------------------------------------------------------------------------------------------------

        public Task Clear()
        {
            return remTableManager.DeleteTableEntries().AsTask();
        }

        public Task<ReminderTableData> ReadRows(GrainId key)
        {
            return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
                () =>
                {
                    return remTableManager.FindReminderEntries(key).ContinueWith(entries =>
                    {
                        ReminderTableData data = ConvertFromTableEntryList(entries);
                        if (logger.IsVerbose2) logger.Verbose2("Read for grain {0} Table=\n{1}", key, data.ToString());
                        return data;
                    });
                }, (Exception exc) =>
                {
                    logger.Warn(ErrorCode.AzureTable_47, String.Format("Intermediate error reading reminders for grain {0} in table {1}.",
                                    key, remTableManager.TableName), exc);
                    throw exc;
                }).AsTask();
        }

        public Task<ReminderTableData> ReadRows(IRingRange range)
        {
            return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
                () =>
                {
                    return remTableManager.FindReminderEntries(range).ContinueWith(entries =>
                    {
                        ReminderTableData data = ConvertFromTableEntryList(entries);
                        if (logger.IsVerbose2) logger.Verbose2("Read in {0} Table=\n{1}", range, data);
                        return data;
                    });
                }, (Exception exc) =>
                {
                    logger.Warn(ErrorCode.AzureTable_40, String.Format("Intermediate error reading reminders in range {0} for table {1}.",
                                    range, remTableManager.TableName), exc);
                    throw exc;
                }).AsTask();
        }

        public Task<ReminderEntry> ReadRow(GrainId grainId, string reminderName)
        {
            return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
                () =>
                {
                    if (logger.IsVerbose) logger.Verbose("ReadRow grainId = {0} reminderName = {1}", grainId, reminderName);

                    return remTableManager.FindReminderEntry(grainId, reminderName)
                            .ContinueWith(result =>
                            {
                                //if (result == false)
                                //    logger.Warn(ErrorCode.AzureTable_41, String.Format("Insert failed on the reminder table. Will retry. Entry = {0}", entry.ToFullString()));
                                return ConvertFromTableEntry(result.Item1, result.Item2);
                            });
                }, (Exception exc) =>
                {
                    logger.Warn(ErrorCode.AzureTable_46, String.Format("Intermediate error reading row with grainId = {0} reminderName = {1} from table {2}.",
                                    grainId, reminderName, remTableManager.TableName), exc);
                    throw exc;
                }).AsTask();
        }

        public Task<string> UpsertRow(ReminderEntry entry)
        {
            return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
                () =>
                {
                    if (logger.IsVerbose) logger.Verbose("UpsertRow entry = {0}", entry.ToFullString());
                    ReminderTableEntry remTableEntry = ConvertToTableEntry(entry, remTableManager.DeploymentId);

                    return remTableManager.UpsertRow(remTableEntry)
                            .ContinueWith((string result) =>
                            {
                                if (result == null)
                                    logger.Warn(ErrorCode.AzureTable_45, String.Format("Upsert failed on the reminder table. Will retry. Entry = {0}", entry.ToFullString()));

                                return result;
                            });
                }, (Exception exc) =>
                {
                    logger.Warn(ErrorCode.AzureTable_42, String.Format("Intermediate error upserting reminder entry {0} to the table {1}.",
                                    entry.ToFullString(), remTableManager.TableName), exc);
                    throw exc;
                }).AsTask();
        }

        public Task<bool> RemoveRow(GrainId grainId, string reminderName, string eTag)
        {
            int grainHashCode = grainId.GetUniformHashCode();
            ReminderTableEntry entry = new ReminderTableEntry
            {
                PartitionKey = ReminderTableEntry.ConstructPartitionKey(remTableManager.DeploymentId, grainId),
                RowKey = ReminderTableEntry.ConstructRowKey(grainId, reminderName),

                DeploymentId = remTableManager.DeploymentId,
                GrainId = grainId.ToParsableString(),
                ReminderName = reminderName,

                GrainIdConsistentHash = unchecked((long)grainHashCode),
                ETag = eTag,
            };

            return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
                () =>
                {
                    if (logger.IsVerbose2) logger.Verbose2("RemoveRow entry = {0}", entry.ToString());

                    return remTableManager.DeleteReminderEntryConditionally(entry, eTag)
                            .ContinueWith((bool result) =>
                            {
                                if (result == false)
                                    logger.Warn(ErrorCode.AzureTable_43, 
                                        String.Format("Delete failed on the reminder table. Will retry. Entry = {0}", entry));

                                return result;
                            });
                }, (Exception exc) =>
                {
                    logger.Warn(ErrorCode.AzureTable_44, 
                        String.Format("Intermediate error when deleting reminder entry {0} to the table {1}.",
                            entry, remTableManager.TableName), exc);
                    throw exc;
                }).AsTask();
        }

        //public AsyncValue<bool> InsertRowIfNotExists(ReminderEntry entry)
        //{
        //    return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
        //        () =>
        //        {
        //            if (logger.IsVerbose) logger.Verbose("InsertRowIfNotExists entry = {0}", entry.ToFullString());
        //            ReminderTableEntry remTableEntry = ConvertToTableEntry(entry, remTableManager.DeploymentId);

        //            return remTableManager.InsertReminderEntryConditionally(remTableEntry)
        //                    .ContinueWith((bool result) =>
        //                    {
        //                        if (result == false)
        //                            logger.Warn(ErrorCode.AzureTable_41, String.Format("Insert failed on the reminder table. Will retry. Entry = {0}", entry.ToFullString()));
        //                        return result;
        //                    });
        //        }, (Exception exc) =>
        //        {
        //            logger.Warn(ErrorCode.AzureTable_42, String.Format("Intermediate error inserting reminder entry {0} to the table {1}.",
        //                            entry.ToFullString(), remTableManager.TableName), exc);
        //            throw exc;
        //        });
        //}

        //public AsyncValue<bool> RemoveRows(GrainId grainId)
        //{
        //    return AsyncCompletionExtensions.ExecuteWithSafeTryCatch(
        //        () =>
        //        {
        //            if (logger.IsVerbose) logger.Verbose("RemoveRows for grain = {0}", grainId);

        //            return remTableManager.DeleteReminderEntries(grainId)
        //                    .ContinueWith((bool result) =>
        //                    {
        //                        if (result == false)
        //                            logger.Warn(ErrorCode.AzureTable_48, String.Format("Delete failed on the reminder table for grain {0}. Will retry.", grainId));
        //                        return result;
        //                    });
        //        }, (Exception exc) =>
        //        {
        //            logger.Warn(ErrorCode.AzureTable_49, String.Format("Intermediate error when deleting reminders for grain {0} to the table {1}.",
        //                            grainId, remTableManager.TableName), exc);
        //            throw exc;
        //        });
        //}
    }
}
