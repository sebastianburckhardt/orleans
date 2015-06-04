using System;
using System.Text;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Newtonsoft.Json.Linq;
using System.Net;
using Orleans;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;

namespace Common
{
    [Serializable]
    public class GrainEntity : TableEntity
    {
        // assume no . in partition key

        public IPAddress payload { get; set; }
        public int port { get; set; }

        public GrainEntity(Orleans.Grain pGrain, IPAddress pAddress, int pListeningPort, string pKey)
        {
            this.payload = pAddress;
            this.PartitionKey = pGrain.GetType().ToString();
            this.RowKey = pKey;
        }

        public GrainEntity()
        {

        }
    }

      





}
