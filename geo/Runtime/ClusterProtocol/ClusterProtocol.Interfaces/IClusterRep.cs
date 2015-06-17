using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace GeoOrleans.Runtime.ClusterProtocol.Interfaces
{
    public interface IClusterRep : Orleans.IGrainWithIntegerKey
    {
        // returns JSON containing current configuration knowledge
        Task<Dictionary<string, DeploymentInfo>> GetGlobalInfo();

        // post JSON gossip message containing configuration information
        Task<Dictionary<string, DeploymentInfo>> PostInfo(Dictionary<string, DeploymentInfo> globalinfo);

        // report instance info
        Task ReportActivity(string instance, InstanceInfo instanceinfo, Dictionary<string, ActivityCounts> counts);

    }



    [Serializable]
    public class DeploymentInfo
    {
        public string Deployment { get; set; }

        public DateTime Timestamp { get; set; }

        public Dictionary<string, InstanceInfo> Instances { get; set; }

        public Dictionary<string, string> ResourceAvailability { get; set; }

     }


    [Serializable]
    public class InstanceInfo
    {
        public DateTime Timestamp { get; set; }

        public string Address { get; set; }

    }




    [Serializable]
    public class ResourceInfo
    {
        public string Name { get; set; }

        public string Dictionary { get; set; }

        public string Join { get; set; }
    }

    [Serializable]
    public struct ActivityCounts : IEquatable<ActivityCounts>
    {

        public int Uses { get; set; }

        public int Fails { get; set; }

        public bool Equals(ActivityCounts other)
        {
            return this.Uses == other.Uses && this.Fails == other.Fails;
        }
    }


}
