using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ClusterProtocol.Interfaces;
using Newtonsoft.Json;

namespace ClusterProtocol.Grains
{
    public class ClusterRep : IClusterRep
    {

        public async Task<string> GetInfo()
        {
            return "";
        }


        string Deployment;
        
        

        Dictionary<string, DeploymentInfo> Info;

        InitializationInfo InitInfo;

        // post JSON gossip message containing configuration information
        public async Task PostInfo(string info)
        {

            Dictionary<string, DeploymentInfo> incoming = 
                JsonConvert.DeserializeObject<Dictionary<string, DeploymentInfo>>(info);

            foreach (var kvp in incoming)
                if (!Info.ContainsKey(kvp.Key) 
                    || Info[kvp.Key].Timestamp.CompareTo(kvp.Value.Timestamp) < 0)
                   Info[kvp.Key] = kvp.Value;   
 
            
        }

        // initialize with given JSON data
        public async Task Init(string info)
        {
            InitInfo = JsonConvert.DeserializeObject<InitializationInfo>(info);

            // TODO send message to self ; send message to join ; start broadcast timer
        }


        public async Task Broadcast()
        {
            foreach(var kvp in Info)
                if (kvp.Key != InitInfo.Deployment)
                {

                }
        }


    }
     

    [Serializable]
    public class DeploymentInfo
    {
        public string Deployment { get; set; }

        public DateTime Timestamp { get; set; }

        public bool Defunct { get; set; }

        public Dictionary<string, string> ResourceAvailability { get; set; }
    }


    [Serializable]
    public class InitializationInfo
    {
        public string Deployment { get; set; }

        public string Address { get; set; }

        public string Join { get; set; } 
    }

}