using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.Diagnostics;
using System.Threading;
using System.IO;
using System.Threading.Tasks;
using Orleans;


using Orleans.Serialization;
using System.Runtime.InteropServices;

namespace Orleans.Runtime.MembershipService
{
    internal class FileBasedMembershipTable : IMembershipTable
    {
        private readonly Logger logger;
        private readonly string filename;
        private static readonly string MembershipTableFileName = "MembershipTableFile.txt";

        public FileBasedMembershipTable(string directory)
        {
            filename = directory + "\\" + MembershipTableFileName;
            if (!Directory.Exists(directory))
            {
                throw new ArgumentException(String.Format("Directory {0} for FileBasedMembershipTable does not exist.", directory));
            }

            logger = Logger.GetLogger("FileBasedMembershipTable", Logger.LoggerType.Runtime);
            logger.Info(ErrorCode.MBRFileBasedTable1, String.Format("FileBasedMembershipTable created with file {0}.", filename));
        }

        public static void DeleteMembershipTableFile(string directory)
        {
            string filename = directory + "\\" + MembershipTableFileName;
            File.Delete(filename);
        }

        public Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            InMemoryMembershipTable table = OpenReadCloseFile(filename, logger);
            if (logger.IsVerbose2) logger.Verbose2("ReadRow {0}. Table=\n{1}", key.ToLongString(), table.ToString());
            return Task.FromResult(table.Read(key));
        }

        public Task<MembershipTableData> ReadAll()
        {
            InMemoryMembershipTable table = OpenReadCloseFile(filename, logger);
            if (logger.IsVerbose2) logger.Verbose2("ReadAll Table=\n{0}", table.ToString());
            return Task.FromResult(table.ReadAll());
        }

        public Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            if (logger.IsVerbose) logger.Verbose("InsertRow entry = {0}, table version = {1}", entry, tableVersion);
            InMemoryMembershipTable table = null;
            bool result = OpenWriteCloseFile(entry, null, tableVersion, 0, out table);
            if (result == false)
                logger.Info(ErrorCode.MBRFileBasedTable2, "Update of entry {0}, table version {1} failed", entry, tableVersion);
            return Task.FromResult(result);
        }

        public Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            if (logger.IsVerbose) logger.Verbose("UpdateRow entry = {0}, etag = {1}, table version = {2}", entry, etag, tableVersion);
            InMemoryMembershipTable table = null;
            bool result = OpenWriteCloseFile(entry, etag, tableVersion, 1, out table);
            if (result == false)
                logger.Info(ErrorCode.MBRFileBasedTable3, "Update of entry {0}, eTag {1}, table version {2} failed", entry, etag, tableVersion);
            return Task.FromResult(result);
        }

        public Task MergeColumn(MembershipEntry entry)
        {
            if (logger.IsVerbose) logger.Verbose("MergeColumn entry = {0}", entry);
            InMemoryMembershipTable table = null;
            bool result = OpenWriteCloseFile(entry, null, null, 2, out table);
            if (result == false)
                logger.Info(ErrorCode.MBRFileBasedTable4, "MergeColumn of entry {0} failed", entry);
            return TaskDone.Done;
        }

        #region File manupilation methods

        private bool OpenWriteCloseFile(MembershipEntry entry, string etag, TableVersion tableVersion, int operation, out InMemoryMembershipTable table)
        {
            FileStream stream = null;
            try
            {
                stream = OpenFile(filename, FileAccess.ReadWrite, FileShare.None, logger);
                table = ReadFromFile(stream, logger);
                bool res = true;
                if (operation == 0)
                    res = table.Insert(entry, tableVersion);
                else if (operation == 1)
                    res = table.Update(entry, etag, tableVersion);
                else if (operation == 2)
                    table.MergeColumn(entry);

                if (res)
                {
                    WriteToFile(table, stream);
                }
                CloseFile(stream);
                return res;
            }
            catch (Exception e1)
            {
                CloseFile(stream);
                logger.Warn(ErrorCode.Runtime_Error_100190, String.Format("Error reading membership table file {0}.", filename), e1);
                throw;
            }
        }

        private static InMemoryMembershipTable OpenReadCloseFile(string filename, Logger logger)
        {
            FileStream stream = null;
            try
            {
                stream = OpenFile(filename, FileAccess.Read, FileShare.Read, logger);
                InMemoryMembershipTable table = ReadFromFile(stream, logger);
                CloseFile(stream);
                return table;
            }
            catch (Exception e1)
            {
                CloseFile(stream);
                logger.Warn(ErrorCode.Runtime_Error_100193, String.Format("Error reading membership table file {0}.", filename), e1);
                throw;
            }
        }

        private static FileStream OpenFile(string filename, FileAccess access, FileShare share, Logger logger)
        {
            int numRetries = 100;
            for (int i = 0; i < numRetries; i++)
            {
                try
                {
                    FileStream stream = File.Open(filename, FileMode.OpenOrCreate, access, share);
                    if (logger.IsVerbose) logger.Verbose(String.Format("!! OPENED {0} for access={1}, share={2}", filename, access, share));
                    //Thread.Sleep(3000);
                    return stream;
                }
                catch (IOException e)
                {
                    if (!IsFileLocked(e))
                        throw;
                    logger.Error(ErrorCode.Runtime_Error_100050, String.Format("RETRYING to OPEN {0} for access={1}, share={2}", filename, access, share));
                    Thread.Sleep(10);
                    if (i == numRetries - 1) // throw upon last retry.
                        throw;
                }
            }
            return null;
        }

        private static bool IsFileLocked(IOException exception)
        {
            int errorCode = Marshal.GetHRForException(exception) & ((1 << 16) - 1);
            return errorCode == 32 || errorCode == 33;
        }


        private static InMemoryMembershipTable ReadFromFile(FileStream stream, Logger logger)
        {
            int len = (int)stream.Length;
            byte[] result = new byte[len];
            stream.Seek(0, SeekOrigin.Begin);
            int actual = stream.Read(result, 0, len);
            if (actual != len)
            {
                logger.Warn(ErrorCode.Runtime_Error_100195, String.Format("Error reading membership table file {0} - data length is wrong.", stream.Name));
                throw new IOException(stream.Name);
            }
            if (len == 0) // empty file
            {
                return new InMemoryMembershipTable();
            }
            InMemoryMembershipTable table = SerializationManager.DeserializeFromByteArray<InMemoryMembershipTable>(result);
            return table;
        }

        private static void WriteToFile(InMemoryMembershipTable table, FileStream stream)
        {
            byte[] data = SerializationManager.SerializeToByteArray(table);
            stream.Seek(0, SeekOrigin.Begin);
            stream.Write(data, 0, data.Length);
            stream.SetLength(data.Length);
            stream.Flush();

            //string data = JsonSerializer.Serialize(table);
            //stream.Seek(0, SeekOrigin.Begin);
            //stream.SetLength(0);
            //StreamWriter sw = new StreamWriter(stream);
            //sw.Write(data);            
            //stream.Flush();
        }

        private static void CloseFile(FileStream stream)
        {
            try
            {
                if (stream != null)
                {
                    stream.Close();
                }
            }
            catch (Exception) { }
        }

        #endregion
    }
}

