using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Orleans.Counters
{
    internal interface ISiloPerformanceMetrics
    {
        long RequestQueueLength { get; }
        int ActivationCount { get; }
        int SendQueueLength { get; }
        int ReceiveQueueLength { get; }
        long SentMessages { get; }
        long ReceivedMessages { get; }
        float CpuUsage { get; }
        long MemoryUsage { get; }
        long ClientCount { get; }
        // More TBD

        bool IsOverloaded { get; }

        void LatchIsOverload(bool overloaded);
        void UnlatchIsOverloaded();
        void LatchCpuUsage(float value);
        void UnlatchCpuUsage();
    }

    internal interface IClientPerformanceMetrics
    {
        float CpuUsage { get; }
        long MemoryUsage { get; }
        int SendQueueLength { get; }
        int ReceiveQueueLength { get; }
        long SentMessages { get; }
        long ReceivedMessages { get; }
        long ConnectedGWCount { get; }
    }

    internal interface ISiloMetricsDataPublisher
    {
        Task ReportMetrics(ISiloPerformanceMetrics metricsData);
    }

    internal interface IClientMetricsDataPublisher
    {
        Task ReportMetrics(IClientPerformanceMetrics metricsData);
    }

    /// <summary>
    /// Snapshot of current runtime statistics for a silo
    /// </summary>
    [Serializable]
    public class SiloRuntimeStatistics
    {
        /// <summary>
        /// Number of activations in a silo.
        /// </summary>
        public int ActivationCount { get; internal set; }

        /// <summary>
        /// The size of the request queue.
        /// </summary>
        public long RequestQueueLength { get; internal set; }

        /// <summary>
        /// The size of the sending queue.
        /// </summary>
        public int SendQueueLength { get; internal set; }

        /// <summary>
        /// The size of the receiving queue.
        /// </summary>
        public int ReceiveQueueLength { get; internal set; }

        /// <summary>
        /// The CPU utilization.
        /// </summary>
        public float CpuUsage { get; internal set; }

        /// <summary>
        /// The used memory size.
        /// </summary>
        public long MemoryUsage { get; internal set; }

        /// <summary>
        /// Is this silo overloaded.
        /// </summary>
        public bool IsOverloaded { get; internal set; }

        /// <summary>
        /// The number of clients currently connected to that silo.
        /// </summary>
        public long ClientCount { get; internal set; }

        internal SiloRuntimeStatistics(ISiloPerformanceMetrics metrics)
        {
            ActivationCount = metrics.ActivationCount;
            RequestQueueLength = metrics.RequestQueueLength;
            SendQueueLength = metrics.SendQueueLength;
            ReceiveQueueLength = metrics.ReceiveQueueLength;
            CpuUsage = metrics.CpuUsage;
            MemoryUsage = metrics.MemoryUsage;
            IsOverloaded = metrics.IsOverloaded;
            ClientCount = metrics.ClientCount;
        }
    }

    /// <summary>
    /// Snapshot of current statistics for a given grain type.
    /// </summary>
    [Serializable]
    internal class GrainStatistic
    {
        /// <summary>
        /// The type of the grain for this GrainStatistic.
        /// </summary>
        public string GrainType { get; set; }

        /// <summary>
        /// Number of grains of a this type.
        /// </summary>
        public int GrainCount { get; set; }

        /// <summary>
        /// Number of activation of a agrain of this type.
        /// </summary>
        public int ActivationCount { get; set; }

        /// <summary>
        /// Number of silos that have activations of this grain type.
        /// </summary>
        public int SiloCount { get; set; }

        /// <summary>
        /// Returns the string representatio of this GrainStatistic.
        /// </summary>
        public override string ToString()
        {
            return string.Format("GrainStatistic: GrainType={0} NumSilos={1} NumGrains={2} NumActivations={3} ", GrainType, SiloCount, GrainCount, ActivationCount);
        }
    }

    /// <summary>
    /// Simple snapshot of current statistics for a given grain type on a given silo.
    /// </summary>
    [Serializable]
    public class SimpleGrainStatistic
    { 
        /// <summary>
        /// The type of the grain for this SimpleGrainStatistic.
        /// </summary>
        public string GrainType { get; set; }

        /// <summary>
        /// The silo address for this SimpleGrainStatistic.
        /// </summary>
        public SiloAddress SiloAddress { get; set; }

        /// <summary>
        /// The number of activations of this grain type on this given silo.
        /// </summary>
        public int ActivationCount { get; set; }

        /// <summary>
        /// Returns the string representatio of this SimpleGrainStatistic.
        /// </summary>
        public override string ToString()
        {
            return string.Format("SimpleGrainStatistic: GrainType={0} Silo={1} NumActivations={2} ", GrainType, SiloAddress, ActivationCount);
        }
    }

    [Serializable]
    internal class DetailedGrainReport
    {
        public GrainId Grain { get; set; } 
        public SiloAddress SiloAddress { get; set; } // silo on which these statistics come from
        public string SiloName { get; set; }        // silo on which these statistics come from
        public List<ActivationAddress> LocalCacheActivationAddresses { get; set; } // activation addresses in the local directory cache
        public List<ActivationAddress> LocalDirectoryActivationAddresses { get; set; } // activation addresses in the local directory.
        public SiloAddress PrimaryForGrain { get; set; } // primary silo for this grain
        public string GrainClassTypeName { get; set; }   // the name of the class that implements this grain.
        public List<string> LocalActivations { get; set; } // activations on this silo

        public override string ToString()
        {
            return string.Format("\n**DetailedGrainReport for grain {0} from silo {1} SiloAddress={2}\n" +
                                    "   LocalCacheActivationAddresses={4}\n   LocalDirectoryActivationAddresses={5}\n   PrimaryForGrain={6}\n" +
                                    "   GrainClassTypeName={7}\n    LocalActivations:\n{3}.\n",
                        Grain.ToDetailedString(), 
                        SiloName,
                        SiloAddress.ToLongString(),
                        Utils.IEnumerableToString(LocalCacheActivationAddresses),
                        Utils.IEnumerableToString(LocalDirectoryActivationAddresses),
                        PrimaryForGrain,
                        GrainClassTypeName, 
                        Utils.IEnumerableToString(LocalActivations, str => string.Format("      {0}", str), "\n"));
        }
    }
}
