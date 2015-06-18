using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.IO;

namespace PileusApp.Utils
{
    public class PerfMetrics
    {
        public TimeSpan TotalProcessorTime { get; set; }
        public TimeSpan UserProcessorTime { get; set; }
        public TimeSpan PrivilegedProcessorTime { get; set; }

        public long PeakVirtualMemorySize64 { get; set; }
        public long VirtualMemorySize64 { get; set; }

        public long PeakPagedMemorySize64 { get; set; }
        public long PagedMemorySize64 { get; set; }

        public long PeakWorkingSet64 { get; set; }
        public long WorkingSet64 { get; set; }

        public long PrivateMemorySize64 { get; set; }

        public long NonpagedSystemMemorySize64 { get; set; }

        public void Update(Process proc)
        {
            this.TotalProcessorTime = proc.TotalProcessorTime;
            this.UserProcessorTime = proc.UserProcessorTime;
            this.PrivilegedProcessorTime = proc.PrivilegedProcessorTime;

            this.PeakVirtualMemorySize64 = proc.PeakVirtualMemorySize64;
            this.VirtualMemorySize64 = proc.VirtualMemorySize64;

            this.PeakPagedMemorySize64 = proc.PeakPagedMemorySize64;
            this.PagedMemorySize64 = proc.PagedMemorySize64;

            this.PeakWorkingSet64 = proc.PeakWorkingSet64;
            this.WorkingSet64 = proc.WorkingSet64;

            this.PrivateMemorySize64 = proc.PrivateMemorySize64;
            this.NonpagedSystemMemorySize64 = proc.NonpagedSystemMemorySize64;
        }

        public void Print(StreamWriter sw)
        {
            Console.WriteLine("Processor Time (ms) (User/ Priv / Total) = {0} / {1} /{2}", this.UserProcessorTime.TotalMilliseconds, this.PrivilegedProcessorTime.TotalMilliseconds, this.TotalProcessorTime.TotalMilliseconds);
            sw.Write(String.Format("UserProcessorTime,, PrivilegedProcessorTime, TotalProcessorTime\n"));
            sw.Write(String.Format("{0},,{1},{2},\n", this.UserProcessorTime.TotalMilliseconds, this.PrivilegedProcessorTime.TotalMilliseconds, this.TotalProcessorTime.TotalMilliseconds));

            Console.WriteLine("Working Set (Current / Peak KB) = {0} / {1}", this.WorkingSet64 / (1024), this.PeakWorkingSet64 / (1024));
            sw.Write(String.Format("Working Set - Current,, Working Set - Peak in KB\n"));
            sw.Write(String.Format("{0},,{1},\n", this.WorkingSet64 / (1024), this.PeakWorkingSet64 / (1024)));

            Console.WriteLine("VirtualMemory (Current / Peak KB) = {0} / {1}", this.VirtualMemorySize64 / (1024), this.PeakVirtualMemorySize64 / (1024));
            sw.Write(String.Format("Virtual Memory - Current,, Virtual Memory - Peak in KB\n"));
            sw.Write(String.Format("{0},,{1},\n", this.VirtualMemorySize64 / (1024), this.PeakVirtualMemorySize64 / (1024)));

            Console.WriteLine("PagedMemory (Current / Peak KB) = {0} / {1}", this.PagedMemorySize64 / (1024), this.PeakPagedMemorySize64 / (1024));
            sw.Write(String.Format("PagedMemory - Current,, PagedMemory - Peak in KB\n"));
            sw.Write(String.Format("{0},,{1},\n", this.PagedMemorySize64 / (1024), this.PeakPagedMemorySize64 / (1024)));

            Console.WriteLine("NonpagedSystemMemorySize = {0}", this.NonpagedSystemMemorySize64 / 1024);
            sw.Write(String.Format("NonPagedMemory in KB\n"));
            sw.Write(String.Format("{0}\n", this.NonpagedSystemMemorySize64 / 1024));

            Console.WriteLine("PrivateMemory = {0}", this.PrivateMemorySize64 / 1024);
            sw.Write(String.Format("PagedMemory in KB\n"));
            sw.Write(String.Format("{0}\n", this.PrivateMemorySize64 / 1024));
        }
    }
}
