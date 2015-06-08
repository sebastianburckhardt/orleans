using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace ClusterProtocol.Interfaces
{
    public interface IClusterRep : IGrainWithIntegerKey
    {
        // returns JSON containing current configuration knowledge
        Task<string> GetInfo();

        // post JSON gossip message containing configuration information
        Task PostInfo(string info);
 
        // initialize with given JSON data
        Task Init(string info);


        
    }
}
