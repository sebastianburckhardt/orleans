using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;
using Orleans.Counters;

namespace Orleans
{
    internal interface ITimeInterval
    {
        void Start();

        void Stop();

        void Restart();

        TimeSpan Elapsed { get; }
    }

    internal static class TimeIntervalFactory
    {
        public static ITimeInterval CreateTimeInterval(bool measureFineGrainedTime)
        {
            if (measureFineGrainedTime)
            {
                return new TimeInterval_StopWatchBased();
            }
            else
            {
                return new TimeInterval_DateTimeBased();
            }
        }
    }

    internal class TimeInterval_StopWatchBased : ITimeInterval
    {
        private Stopwatch stopwatch;

        public TimeInterval_StopWatchBased()
        {
            stopwatch = new Stopwatch();
        }

        public void Start()
        {
            stopwatch.Start();
        }

        public void Stop()
        {
            stopwatch.Stop();
        }

        public void Restart()
        {
            stopwatch.Restart();

        }
        public TimeSpan Elapsed { get { return stopwatch.Elapsed; } }
    }

    internal class TimeInterval_DateTimeBased : ITimeInterval
    {
        private bool running;
        private DateTime start;

        public TimeSpan Elapsed { get; private set; }

        public TimeInterval_DateTimeBased()
        {
            running = false;
            Elapsed = TimeSpan.Zero;
        }

        public void Start()
        {
            if (!running)
            {
                start = DateTime.UtcNow;
                running = true;
                Elapsed = TimeSpan.Zero;
            }
        }

        public void Stop()
        {
            if (running)
            {
                DateTime end = DateTime.UtcNow;
                Elapsed += (end - start);
                running = false;
            }
        }

        public void Restart()
        {
            start = DateTime.UtcNow;
            running = true;
            Elapsed = TimeSpan.Zero;
        }
    }
}
