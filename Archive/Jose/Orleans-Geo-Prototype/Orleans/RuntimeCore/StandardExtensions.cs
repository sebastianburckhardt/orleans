﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Security.Cryptography;
using System.Text;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Threading;


namespace Orleans
{
    /// <summary>
    /// The Utils class contains a variety of utility methods for use in application and grain code.
    /// </summary>
    internal static class StandardExtensions
    {
        public static TimeSpan Multiply(this TimeSpan timeSpan, double value)
        {
            double ticksD = checked((double)timeSpan.Ticks * value);
            long ticks = checked((long)ticksD);
            return TimeSpan.FromTicks(ticks);
        }

        public static TimeSpan Divide(this TimeSpan timeSpan, double value)
        {
            double ticksD = checked((double)timeSpan.Ticks / value);
            long ticks = checked((long)ticksD);
            return TimeSpan.FromTicks(ticks);
        }

        public static double Divide(this TimeSpan first, TimeSpan second)
        {
            double ticks1 = (double)first.Ticks;
            double ticks2 = (double)second.Ticks;
            return ticks1 / ticks2;
        }

        public static TimeSpan Max(TimeSpan first, TimeSpan second)
        {
            if (first >= second) return first;
            else return second;
        }

        public static TimeSpan Min(TimeSpan first, TimeSpan second)
        {
            if (first < second) return first;
            else return second;
        }

        public static TimeSpan NextTimeSpan(this SafeRandom random, TimeSpan timeSpan)
        {
            if (timeSpan <= TimeSpan.Zero) throw new ArgumentOutOfRangeException("timeSpan", timeSpan, "SafeRandom.NextTimeSpan timeSpan must be a positive number.");
            double ticksD = ((double)timeSpan.Ticks) * random.NextDouble();
            long ticks = checked((long)ticksD);
            return TimeSpan.FromTicks(ticks);
        }

        public static TimeSpan NextTimeSpan(this SafeRandom random, TimeSpan minValue, TimeSpan maxValue)
        {
            if (minValue <= TimeSpan.Zero) throw new ArgumentOutOfRangeException("minDelay", minValue, "SafeRandom.NextTimeSpan minValue must be a positive number.");
            if (minValue >= maxValue) throw new ArgumentOutOfRangeException("minValue", minValue, "SafeRandom.NextTimeSpan minValue must be greater than maxValue.");
            TimeSpan span = maxValue - minValue;
            return minValue + random.NextTimeSpan(span);
        }
    }
}
