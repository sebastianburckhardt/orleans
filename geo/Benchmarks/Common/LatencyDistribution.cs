using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Web;

namespace GeoOrleans.Benchmarks.Common
{
    [Serializable]
    public class LatencyDistribution
    {

        public int[] Counts { get; set; }
        public int Total { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }

        

        // bucketization is chosen for human readability
        public static long[] buckets = { 2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45, 50, 75, 100, 150, 200, 250, 300, 350, 400, 450, 500, 1000, 2000, 5000, 
                                   10000, 20000, 30000, 60000, 120000, 300000, 600000, 1200000, 1800000, 
                                   3600000, 2*3600000, 5*3600000, 12*3600000, 
                                   1*24*3600000,  2*24*3600000, 5*24*3600000, 10*24*3600000};

        static int TimeoutValue = int.MaxValue;

        public void AddDataPoint(long msec)
        {
            if (msec < 1)
                throw new ArgumentException("msec parameter must be positive");

            if (Counts == null)
                Init();

            // find and increment bucket counter
            var pos = 0;
            while (pos < buckets.Length && buckets[pos] < msec)
                pos++;
            Counts[pos]++;

            // update total, min, max
            Total++;
            if (msec > Max) Max = msec;
            if (msec < Min) Min = msec;
        }

        public void MergeDistribution(LatencyDistribution d)
        {
            if (d.Counts == null)
                return;
            if (Counts == null)
                Init();

            // merge bucket counters
            for (int i = 0; i < Counts.Length; i++)
                Counts[i] += d.Counts[i];

            // update total, min, max
            Total += d.Total;
            if (d.Max > Max) Max = d.Max;
            if (d.Min < Min) Min = d.Min;
        }

        public void Init()
        {
            Counts = new int[buckets.Length + 1];
            Total = 0;
            Max = 0;
            Min = int.MaxValue;
        }

        public IEnumerable<string> GetStats()
        {
            if (Counts == null)
                yield break;

            yield return string.Format("nr={0}", Total);
            yield return string.Format("min={0}", FormatMsec(Min));

            int covered = 0;
            var pos = 0;
            while (covered < Total)
            {
                int goal = covered + (Total - covered + 1) / 2;
                while (covered < goal)
                    covered += Counts[pos++];
                if (pos - 1 < buckets.Length && (covered < Total))
                {
                    string perc = (covered * 100 / Total).ToString();
                    if (perc == "99")
                        perc = (covered * 100.0 / Total).ToString("G4");
                    yield return string.Format("{1}%<{0}", FormatMsec(buckets[pos - 1]), perc);
                }
            }
            yield return string.Format("max={0}", FormatMsec(Max));
        }

        public static string FormatMsec(long msec)
        {
            if (msec < 1000)
                return string.Format("{0}ms", msec);
            else if (msec < 60000)
                return string.Format("{0:G3}s", msec / 1000.0);
            else if (msec < 3600000)
                return string.Format("{0:G3}m", msec / 60000.0);
            else if (msec < 24 * 3600000)
                return string.Format("{0:G3}h", msec / 3600000.0);
            else if (msec < TimeoutValue)
                return string.Format("{0:G3}d", msec / (24 * 3600000.0));
            else
                return "TO";
        }

    }
}