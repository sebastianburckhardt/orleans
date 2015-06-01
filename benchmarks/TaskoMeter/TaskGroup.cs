using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Common;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Text.RegularExpressions;

namespace TaskoMeter
{
    public class TaskGroup
    {

        internal static long offset = 0;
        internal static long end = 0;

        internal static Dictionary<string, TaskGroup> TaskGroups = new Dictionary<string, TaskGroup>();

        internal List<Interval> Intervals = new List<Interval>();
        internal int seqnum;
        internal bool Enabled;
        internal string Name;

        internal int threadoffset;
        internal List<Interval> temp = new List<Interval>();

        /// <summary>
        /// Represents an individual interval.
        /// </summary>
        public class Interval : IComparable<Interval>
        {
            public long Start;
            public long End;
            public int Tid;
            public int reqno;
            public int filecount;

            public Interval(long start, long end, int reqno, int filecount)
            {
                this.Start = start;
                this.End = end;
                this.Tid = 0;
                this.reqno = reqno;
                this.filecount = filecount;
            }

            public int CompareTo(Interval other)
            {
                return Start.CompareTo(other.Start);
            }
        }

        public static int FindAvailableThread(List<long> threads, long begin, long end)
        {
            for (int i = 0; i < threads.Count; i++)
            {
                if (threads[i] <= begin)
                {
                    threads[i] = end;
                    return i;
                }
            }
            threads.Add(end);
            return threads.Count - 1;
        }


    

        /// <summary>
        ///  change this line to access different storage
        /// </summary>
        public static StorageAccounts.Account storagemode = StorageAccounts.Account.OrleansGeoSharedStorage;
    
        public static bool ReadFromFile(string foldername, string deploymentid)
        {
          
            long minticks = long.MaxValue;
            long maxticks = 0;


            var storageaccount = CloudStorageAccount.Parse(StorageAccounts.GetConnectionString(storagemode));
            var blobClient = storageaccount.CreateCloudBlobClient();

            var blobcontainer = blobClient.GetContainerReference(foldername);

            string pattern;
            if (deploymentid == null || deploymentid.Length == 0)
            {
                pattern = "^.*";
            }
            else
            {
                pattern = "^" + Regex.Escape(deploymentid);
            }
            Regex deploymentRegex = new Regex(pattern);

            try
            {
                blobcontainer.FetchAttributes();
                // the container exists if no exception is thrown  
            }
            catch (Exception)
            {
                return false;
            }


            int taskmetercounter = 0;
            int filecount = 0;

            foreach (var x in blobcontainer.ListBlobs())
            {
                var blockblob = x as CloudBlockBlob;
                if (blockblob == null || !deploymentRegex.IsMatch(blockblob.Name))
                    continue;

                var blobstream = blockblob.OpenRead();
                var reader = new BinaryReader(blobstream);

                try
                {
                    while (true)
                    {

                        var groupname = reader.ReadString();
                        var reqno = reader.ReadInt32();
                        var start = reader.ReadInt64();
                        var end = reader.ReadInt64();

                        if (minticks > start)
                            minticks = start;
                        if (maxticks < end)
                            maxticks = end;

                        TaskGroup meter = null;
                        if (!TaskGroups.TryGetValue(groupname, out meter))
                        {
                            meter = new TaskGroup() { Name = groupname, seqnum = taskmetercounter++ };
                            TaskGroups[groupname] = meter;
                        }

                        meter.temp.Add(new Interval(start, end, reqno, filecount));
                    }
                }
                catch (EndOfStreamException)
                {
                }
                finally
                {
                    blobstream.Close();
                }

                // sort intervals and assign thread
                foreach (var meter in TaskGroups.Values)
                {
                    meter.temp.Sort();
                    var threads = new List<long>();
                    
                    foreach(var interval in meter.temp)
                        interval.Tid = meter.threadoffset + FindAvailableThread(threads, interval.Start, interval.End);

                    meter.threadoffset += threads.Count();

                    meter.Intervals.AddRange(meter.temp);
                    meter.temp.Clear();
                }


                filecount++;
            }

            if (minticks > maxticks)
            {
                minticks = 0;
                maxticks = 1;
            }

            TaskGroup.offset = minticks;
            TaskGroup.end = maxticks - minticks;

            return true;

         }


    }
}
