using Microsoft.WindowsAzure.ServiceRuntime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure;
using System.Net;
using GeoOrleans.Runtime.Common;

namespace GeoOrleans.Benchmarks.Common
{
    public static class Util
    {

  
  
      
        public static string PrintStats(Dictionary<string, LatencyDistribution> stats)
        {
            var b = new StringBuilder();
            if (stats != null)
                foreach (var kvp in stats)
                {
                    b.AppendLine(kvp.Key);
                    b.Append("      ");
                    b.AppendLine(string.Join(" ", kvp.Value.GetStats()));
                }
            return b.ToString();
        }



        public static  string GRAIN_TABLE = "georepgrains";

       
        // todo change when more than one silo. 
        public static async Task<Tuple<IPAddress,int>> getGrainAddress(AzureEndpoints.ServiceDeployments pSiloEndpoint, Type pGrainType, string pGrainId)
        {
            string connectionKey = "";
            IPAddress grainIP = null;
            int grainPort = 0;
            GrainEntity registerEntity = null;

            switch (pSiloEndpoint)
            {
                case AzureEndpoints.ServiceDeployments.OrleansGeoUsWest:
                    connectionKey = StorageAccounts.GetConnectionString(StorageAccounts.Account.OrleansGeoUsWest);
                    break;
                case AzureEndpoints.ServiceDeployments.OrleansGeoEuropeWest:
                    connectionKey = StorageAccounts.GetConnectionString(StorageAccounts.Account.OrleansGeoEuropeWest);
                    break;
                default:
                    connectionKey = StorageAccounts.GetConnectionString(StorageAccounts.Account.DevStorage);
                    break;
            }
            CloudTableClient tableClient = AzureUtils.getTableClient(CloudStorageAccount.Parse(connectionKey));
            CloudTable table = tableClient.GetTableReference(GRAIN_TABLE);
            TableResult result = await AzureUtils.findEntity<GrainEntity>(tableClient,GRAIN_TABLE, pGrainType.ToString(), pGrainId);
            if (result == null)
            {
                throw new Exception("Failed to retrieve grain");
            }
            else
            {
                registerEntity = (GrainEntity) result.Result;
                grainIP = IPAddress.Parse(registerEntity.ipAddress);
                grainPort = registerEntity.port;
            }
            return new Tuple<IPAddress,int>(grainIP, grainPort);

        }

        public static async void register(Orleans.Grain pGrain, int port, string pKey)
        {
            IPHostEntry host;
            IPAddress localIp = null;
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIp = ip;
                    Console.WriteLine("IP is {0} ", localIp);
                }
            }
            GrainEntity grain = new GrainEntity(pGrain, localIp, port, pKey);
            CloudTableClient tableClient = AzureUtils.getTableClient();
            AzureUtils.createTable(tableClient, GRAIN_TABLE);
            await AzureUtils.updateEntity<GrainEntity>(tableClient, GRAIN_TABLE, grain);
        }


    }



}
