using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans
{

    /// <summary>
    /// Class containing key names for the configurable LimitValues used by Orleans runtime.
    /// </summary>
    internal static class LimitNames
    {
        public const string Limit_MaxPendingItems = "MaxPendingItems";
        public const string Limit_MaxEnqueuedRequests = "MaxEnqueuedRequests";
        public const string Limit_MaxEnqueuedRequests_StatelessWorker = "MaxEnqueuedRequests_StatelessWorker";
    }

    /// <summary>
    /// Limits Manager
    /// </summary>
    internal static class LimitManager
    {
        private static ILimitsConfiguration limitsConfig;

        public static void Initialize(ILimitsConfiguration limitValues)
        {
            limitsConfig = limitValues;
        }

        public static void UnInitialize()
        {
            limitsConfig = null;
        }

        public static LimitValue GetLimit(string name)
        {
            if (limitsConfig == null) throw new InvalidOperationException("LimitsManager not yet initialized");

            return GetLimit(name, 0, 0);
        }

        public static LimitValue GetLimit(string name, int defaultSoftLimit)
        {
            if (limitsConfig == null) throw new InvalidOperationException("LimitsManager not yet initialized");

            return GetLimit(name, defaultSoftLimit, 0);
        }

        public static LimitValue GetLimit(string name, int defaultSoftLimit, int defaultHardLimit)
        {
            if (limitsConfig == null) throw new InvalidOperationException("LimitsManager not yet initialized");

            LimitValue limit = limitsConfig.GetLimit(name) 
                ?? new LimitValue { Name = name, SoftLimitThreshold = defaultSoftLimit, HardLimitThreshold = defaultHardLimit};
            return limit;
        }
    }
}
