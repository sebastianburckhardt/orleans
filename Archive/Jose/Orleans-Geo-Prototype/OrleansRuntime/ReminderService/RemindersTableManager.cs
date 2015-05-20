using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.AzureUtils;


namespace Orleans.Runtime.ReminderService
{
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class ReminderTableEntry : TableServiceEntity
    {
        public string DeploymentId { get; set; }    // Mandatory
        public string GrainId { get; set; }         // RowKey
        public string ReminderName { get; set; }    // RowKey

        public string StartAt { get; set; }         // Mandatory
        public string Period { get; set; }          // Mandatory

        public long GrainIdConsistentHash { get; set; }   // PartitionKey encodes consistent hash of the grain

        public string ETag { get; set; }              // uniquely identifies this reminder
        
        public static string ConstructRowKey(GrainId grainId, string reminderName)
        {
            return string.Format("{0}-{1}", grainId.ToParsableString(), reminderName); //grainId.ToString(), reminderName);
        }

        public static string ConstructPartitionKey(string deploymentId, GrainId grainId)
        {
            return ConstructPartitionKey(deploymentId, unchecked((uint)grainId.GetUniformHashCode()));
        }

        public static string ConstructPartitionKey(string deploymentId, uint number)
        {
            // this format of partition key makes sure that the comparisons in FindReminderEntries(begin, end) work correctly
            // the idea is that when converting to string, negative numbers start with 0, and positive start with 1. Now,
            // when comparisons will be done on strings, this will ensure that positive numbers are always greater than negative

            // IMPORTNANT NOTE: Other code using this return data is very sensitive to format changes, 
            //       so take great care when making any changes here!!!

            string grainHash = number < 0 ? string.Format("0{0}", number.ToString("X")) : string.Format("1{0:d16}", number);
            // We put deploymentId at the end so that range query can work with String.Compare, 
            // since Azure table only supports prefix comparison (at least as far as I could find out)
            return string.Format("{0}_{1}", grainHash, deploymentId);
        }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            
            sb.Append("Reminder [");

            sb.Append(" Deployment=").Append(DeploymentId);
            sb.Append(" GrainId=").Append(GrainId);
            sb.Append(" ReminderName=").Append(ReminderName);

            sb.Append(" StartAt=").Append(StartAt);
            sb.Append(" Period=").Append(Period);

            sb.Append(" GrainIdConsistentHash=").Append(GrainIdConsistentHash);
            sb.Append("]");
            
            return sb.ToString();
        }
    }
    
    internal class RemindersTableManager : AzureTableDataManager<ReminderTableEntry>
    {
        private const string REMINDERS_TABLE_NAME = "OrleansReminders";

        public string DeploymentId { get; private set; }

        public static async Task<RemindersTableManager> GetManager(string deploymentId, string storageConnectionString)
        {
            RemindersTableManager singleton = new RemindersTableManager(deploymentId, storageConnectionString);
            try
            {
                await singleton.InitTableAsync().WithTimeout(AzureTableDefaultPolicies.TableCreation_TIMEOUT);
            }
            catch (TimeoutException)
            {
                singleton.logger.Fail(ErrorCode.AzureTable_38, String.Format("Unable to create or connect to the Azure table in {0}", AzureTableDefaultPolicies.TableCreation_TIMEOUT));
            }
            catch (Exception ex)
            {
                singleton.logger.Fail(ErrorCode.AzureTable_39, String.Format("Exception trying to create or connect to the Azure table: {0}", ex));
            }
            return singleton;
        }

        private RemindersTableManager(string deploymentId, string storageConnectionString)
            : base(REMINDERS_TABLE_NAME, storageConnectionString)
        {
            this.DeploymentId = deploymentId;
        }

        internal AsyncValue<List<Tuple<ReminderTableEntry, string>>> FindReminderEntries(IRingRange range)
        {
            uint begin = ((SingleRange)range).Begin;
            uint end = ((SingleRange)range).End;
            string sBegin = ReminderTableEntry.ConstructPartitionKey(DeploymentId, begin);
            string sEnd = ReminderTableEntry.ConstructPartitionKey(DeploymentId, end);
            if (begin < end)
            {
                Expression<Func<ReminderTableEntry, bool>> query = 
                    (ReminderTableEntry e) =>
                        String.Compare(e.PartitionKey, sBegin) > 0
                        && String.Compare(e.PartitionKey, sEnd) <= 0;

                return AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(query))
                    .ContinueWith(queryResults => queryResults.ToList());
            }
            else if (begin == end)
            {
                Expression<Func<ReminderTableEntry, bool>> query =
                    (ReminderTableEntry e) => true;

                return AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(query))
                    .ContinueWith(queryResults => queryResults.ToList());
            }
            else // (begin > end)
            {
                Expression<Func<ReminderTableEntry, bool>> p1Query = 
                    e => String.Compare(e.PartitionKey, sBegin) > 0;
                Expression<Func<ReminderTableEntry, bool>> p2Query =
                    e => String.Compare(e.PartitionKey, sEnd) <= 0;

                var p1 = AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(p1Query));
                var p2 = AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(p2Query));

                return AsyncValue<IEnumerable<Tuple<ReminderTableEntry, string>>>.JoinAll(new[] { p1, p2 })
                    .ContinueWith((IEnumerable<Tuple<ReminderTableEntry, string>>[] arr) => arr[0].Union(arr[1]).ToList());
            }
        }

        internal AsyncValue<List<Tuple<ReminderTableEntry, string>>> FindReminderEntries(GrainId grainId)
        {
            var partitionKey = ReminderTableEntry.ConstructPartitionKey(DeploymentId, grainId);

            Expression<Func<ReminderTableEntry, bool>> query = 
                instance =>
                    instance.PartitionKey == partitionKey
                    && String.Compare(instance.RowKey, grainId.ToParsableString() + '-') > 0
                    && String.Compare(instance.RowKey, grainId.ToParsableString() + (char)('-' + 1)) <= 0;

            var queryResultsPromise = AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(query));

            return queryResultsPromise.ContinueWith(queryResults => queryResults.ToList());
        }

        internal AsyncValue<Tuple<ReminderTableEntry, string>> FindReminderEntry(GrainId grainId, string reminderName)
        {
            string partitionKey = ReminderTableEntry.ConstructPartitionKey(DeploymentId, grainId);
            string rowKey = ReminderTableEntry.ConstructRowKey(grainId, reminderName);

            return AsyncValue.FromTask(ReadSingleTableEntryAsync(partitionKey, rowKey));
        }

        private AsyncValue<List<Tuple<ReminderTableEntry, string>>> FindAllReminderEntries()
        {
            Expression<Func<ReminderTableEntry, bool>> query =
                instance => instance.DeploymentId == DeploymentId;

            var queryResultsPromise = AsyncValue.FromTask(ReadTableEntriesAndEtagsAsync(query));
            return queryResultsPromise.ContinueWith(queryResults => queryResults.ToList());
        }

        internal AsyncValue<string> UpsertRow(ReminderTableEntry reminderEntry)
        {
            return AsyncValue.FromTask(UpsertTableEntryAsync(reminderEntry))
                .ContinueWith((eTag) => eTag,
                    (Exception exc) =>
                    {
                        HttpStatusCode httpStatusCode;
                        string restStatus;
                        if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus))
                        {
                            if (logger.IsVerbose2) logger.Verbose2("UpsertRow failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                            if (AzureStorageUtils.IsContentionError(httpStatusCode)) return null; // false;
                        }
                        throw exc;
                    });
        }


        internal AsyncValue<bool> DeleteReminderEntryConditionally(ReminderTableEntry reminderEntry, string eTag)
        {
            return AsyncCompletion.FromTask(DeleteTableEntryAsync(reminderEntry, eTag)) //"*")
                .ContinueWith(() =>
                {
                    return true;
                },
                    (Exception exc) =>
                    {
                        HttpStatusCode httpStatusCode;
                        string restStatus;
                        if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus))
                        {
                            if (logger.IsVerbose2) logger.Verbose2("DeleteReminderEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
                            if (AzureStorageUtils.IsContentionError(httpStatusCode)) return false;
                        }
                        throw exc;
                    });
        }

        //internal AsyncValue<bool> InsertReminderEntryConditionally(ReminderTableEntry reminderEntry)
        //{
        //    return AsyncValue<bool>.FromTask(InsertTableEntryConditionally(reminderEntry, null, null, false))
        //        .ContinueWith(() =>
        //        {
        //            return true;
        //        },
        //            (Exception exc) =>
        //            {
        //                HttpStatusCode httpStatusCode;
        //                string restStatus;
        //                if (AzureStorageUtils.EvaluateException(exc, out httpStatusCode, out restStatus))
        //                {
        //                    if (logger.IsVerbose2) logger.Verbose2("InsertReminderEntryConditionally failed with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
        //                    if (AzureStorageUtils.IsContentionError(httpStatusCode)) return false;
        //                }
        //                throw exc;
        //            });
        //}

        //internal AsyncValue<bool> DeleteReminderEntries(GrainId grainId)
        //{
        //    // note that if there was a reminder registration between 'find' and 'delete', it will not be removed!
        //    AsyncValue<List<Tuple<ReminderTableEntry, string>>> entriesPromise = FindReminderEntries(grainId);
        //    return entriesPromise.ContinueWith(entries =>
        //        {
        //            //bool result = DeleteTableEntries(entries).ContinueWith(() => true);
        //            //return result;
        //            AsyncCompletion delpromise = DeleteTableEntries(entries);
        //            return delpromise.ContinueWith(() => true,
        //            (Exception exc) =>
        //            {
        //                HttpStatusCode httpStatusCode;
        //                string restStatus;
        //                if (EvaluateException(exc, out httpStatusCode, out restStatus))
        //                {
        //                    if (logger.IsVerbose2) logger.Verbose2("DeleteReminderEntries failed in second step with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
        //                    if (IsContentionError(httpStatusCode)) return false;
        //                }
        //                throw exc;
        //            });
        //        }, (Exception exc) =>
        //            {
        //                HttpStatusCode httpStatusCode;
        //                string restStatus;
        //                if (EvaluateException(exc, out httpStatusCode, out restStatus))
        //                {
        //                    if (logger.IsVerbose2) logger.Verbose2("DeleteReminderEntries failed in first step with httpStatusCode={0}, restStatus={1}", httpStatusCode, restStatus);
        //                    if (IsContentionError(httpStatusCode)) return false;
        //                }
        //                throw exc;
        //            });
        //}

        #region Table operations

        internal AsyncCompletion DeleteTableEntries()
        {
            // [mlr][todo] shouldn't this be String.IsNullOrEmpty()?
            if (DeploymentId == null)
                return AsyncCompletion.FromTask(DeleteTableAsync());
            else
            {
                AsyncValue<List<Tuple<ReminderTableEntry, string>>> entriesPromise = FindAllReminderEntries();
                return entriesPromise.ContinueWith(entries =>
                {
                    // return manager.DeleteTableEntries(entries); // this doesnt work as entries can be across partitions, which is not allowed
                    // group by grain hashcode so each query goes to different partition
                    List<AsyncCompletion> list = new List<AsyncCompletion>();
                    var groupedByHash = entries
                        .Where(tuple => tuple.Item1.DeploymentId.Equals(DeploymentId))  // delete only entries that belong to our DeploymentId.
                        .GroupBy(x => x.Item1.GrainIdConsistentHash).ToDictionary(g => g.Key, g => g.ToList());
                    foreach (var entriesPerPartition in groupedByHash.Values)
                    {
                        list.Add(AsyncCompletion.FromTask(DeleteTableEntriesAsync(entriesPerPartition)));
                    }
                    // unoptimized version, without grouping
                    //foreach (var entry in entries)
                    //{
                    //    list.Add(manager.DeleteTableEntry(entry.Item1, entry.Item2));
                    //}
                    return AsyncCompletion.JoinAll(list);
                });
            }
        }

        #endregion
    }
}
