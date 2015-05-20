using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Threading.Tasks;
using Orleans;



namespace Orleans.Runtime.MembershipService
{
    [Reentrant]
    internal class GrainBasedMembershipTable : GrainBase, IMembershipTable
    {
        private InMemoryMembershipTable table;
        private Logger logger;

        public override Task ActivateAsync()
        {
            logger = Logger.GetLogger("GrainBasedMembershipTable", Logger.LoggerType.Runtime);
            logger.Info(ErrorCode.MBRGrainBasedTable1, "GrainBasedMembershipTable Activated.");
            table = new InMemoryMembershipTable();
            return TaskDone.Done;
        }
        public Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            return Task.FromResult(table.Read(key));
        }

        public Task<MembershipTableData> ReadAll()
        {
            var t = table.ReadAll();
            //logger.Info("GBMT t={0}", t);
            return Task.FromResult(t);
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            if (logger.IsVerbose) logger.Verbose("InsertRow entry = {0}, table version = {1}", entry.ToFullString(), tableVersion);
            bool result = table.Insert(entry, tableVersion);
            if (result == false)
                logger.Info(ErrorCode.MBRGrainBasedTable2, "Insert of {0} and table version {1} failed. Table now is {2}", entry.ToFullString(), tableVersion, table.ReadAll());
            return Task.FromResult(result);
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (logger.IsVerbose) logger.Verbose("UpdateRow entry = {0}, etag = {1}, table version = {2}", entry.ToFullString(), etag, tableVersion);
            bool result = table.Update(entry, etag, tableVersion);
            if (result == false)
                logger.Info(ErrorCode.MBRGrainBasedTable3, "Update of {0}, eTag {1}, table version {2} failed. Table now is {3}", entry.ToFullString(), etag, tableVersion, table.ReadAll());
            return Task.FromResult(result);
        }

        public Task MergeColumn(MembershipEntry entry)
        {
            if (logger.IsVerbose) logger.Verbose("MergeColumn entry = {0}", entry.ToFullString());
            table.MergeColumn(entry);
            return TaskDone.Done;
        }
    }
}

