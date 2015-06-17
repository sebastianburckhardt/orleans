using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Web;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using System.Threading.Tasks;
using System.Diagnostics;
#pragma warning disable 0649
#pragma warning disable 0420

namespace GeoOrleans.Runtime.Common
{
    public class TraceInterval : IDisposable
    {
        private string taskgroup;
        private long start;
        private long end;
        private int color;

        private TraceInterval next;

        private static Object worklock = new object();
        private static volatile Thread bgworker;

        private static volatile TraceInterval head = null;

        // single global lock around single stopwatch - may need to make this more scalable at some point
        private static Object sgl = new Object(); 
        private static Stopwatch stopwatch = null;
        private static long offset = 0;

        // use this to globally switch tracing on/off
        public static bool CollectPerfTraces = false;

        private static long ReadWatch()
        {
            if (stopwatch == null)
            {
                stopwatch = new Stopwatch();
                offset = DateTime.UtcNow.Ticks;
                stopwatch.Start();
            }
            return stopwatch.ElapsedTicks + offset;
        }

        public TraceInterval(string taskgroup, int color = 0)
        {
            this.taskgroup = taskgroup;
            this.color = color;

            //this.start = DateTime.UtcNow.Ticks;
            lock (sgl)
                this.start = ReadWatch();
        }

        public void Dispose()
        {
            this.EndInterval();
        }

        private TraceInterval() // cannot call this, it is private
        {

        }

        //public static TraceInterval BeginInterval(string taskgroup, int reqid)
        //{
        //   var t = new TraceInterval() { 
        //      taskgroup = taskgroup,
        //      reqid = reqid
        //  };
        //   t.start = DateTime.UtcNow.Ticks;
        //   return t;
        //}

        public void EndInterval()
        {
            lock (sgl)
            {
                end = ReadWatch();
                next = head;
                head = this;
            }
      
            // while (true)
            // {
            //     next = head;
            //     if (next == System.Threading.Interlocked.CompareExchange(ref head, this, next))
            //         break;
            // }

            if (!CollectPerfTraces)
                return;

            if (bgworker == null)
            {
                lock (worklock)
                {
                    if (head != null && bgworker == null)
                    {
                        bgworker = new Thread(BgWork);
                        bgworker.Name = "Task Metering";
                        bgworker.Start();
                    }
                }
            }
        }

        private const int backoff_msec = 500;
        private const int shutdown_msec = 10000;

        // if heartbeat is set to true, generate trace intervals periodically
        private static volatile bool doheartbeat = false;

        
        private static string GetBlobName()
        {
            string deploymentid = SecUtility.Escape(Util.MyDeploymentId);
            string datetime = DateTime.UtcNow.ToString("yyyy-MM-dd HH-mm fffffff");
            string instance = SecUtility.Escape(Util.MyInstanceName);
            return string.Format("{0} {1} {2}", deploymentid, datetime, instance);
        }

        private static List<TraceInterval> HeartBeats = new List<TraceInterval>();

        public static void BgWork()
        {
            try
            {
                // open stream
                var connectionstring = StorageAccounts.GetConnectionString(StorageAccounts.GetTracingAccount());
                var account = CloudStorageAccount.Parse(connectionstring);
                var blobClient = account.CreateCloudBlobClient();
                var c = blobClient.GetContainerReference("traces");
                c.CreateIfNotExists();
                var blob = c.GetBlockBlobReference(GetBlobName());
                var blobstream = blob.OpenWrite();
                BinaryWriter writer = new BinaryWriter(blobstream);
                List<TraceInterval> list = new List<TraceInterval>();
                int quietfor = 0;

                doheartbeat = true;
                lock (HeartBeats)
                    HeartBeats.Clear();
                var bgtask = HeartBeat();

                while (quietfor < shutdown_msec)
                {
                    TraceInterval work;

                    while (true)
                    {
                        work = head;

                        if (work == System.Threading.Interlocked.CompareExchange(ref head, null, work))
                            break;
                    }

                    if (work == null)
                    {
                        Thread.Sleep(backoff_msec);
                        quietfor += backoff_msec;
                    }
                    else
                    {

                        quietfor = 0;

                        do
                        {
                            list.Add(work);
                            work = work.next;
                        }
                        while (work != null);

                        // get heartbeats
                        lock (HeartBeats)
                        {
                            list.AddRange(HeartBeats);
                            HeartBeats.Clear();
                        }

                        for (int i = list.Count() - 1; i >= 0; i--)
                        {
                            var r = list[i];
                            writer.Write(r.taskgroup);
                            writer.Write(r.color);
                            writer.Write(r.start);
                            writer.Write(r.end);
                        }

                        list.Clear();
                    }
                }

                bgworker = null; // we may lose some entries here if there is a race with check. No matter.

                doheartbeat = false; // stop collecting heartbeat

                // close stream
                blobstream.Close();
            }
            catch (Exception e)
            {
                Trace.WriteLine("[TraceInterval.cs] Could not write to trace blob because of exception: e=" + e);
            }
        }

        public static async Task HeartBeat()
        {
            try
            {
                while (doheartbeat)
                {
                    var ti = new TraceInterval() { taskgroup = "HeartBeat" };

                    lock (sgl)
                        ti.start = ReadWatch();

                    //ti.start = DateTime.UtcNow.Ticks;

                    await Task.Delay(50);

                    lock (sgl)
                        ti.end = ReadWatch();
                    //ti.end = DateTime.UtcNow.Ticks;

                    lock (HeartBeats)
                    {
                        HeartBeats.Add(ti);
                    }
                }
            }
            catch (Exception e)
            {
                Trace.WriteLine("[TraceInterval.cs] Could not write to trace blob because of exception: e=" + e);
            }
        }
    }
}