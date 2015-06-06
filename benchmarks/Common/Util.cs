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

namespace Common
{
    public static class Util
    {

        public static void Assert(bool condition, string message = null)
        {
            if (condition)
                return;
            if (!string.IsNullOrEmpty(message))
                throw new AssertionException(message);
            else
                throw new AssertionException();
        }

        public static void Fail(string message)
        {
            throw new AssertionException(message);
        }

        [Serializable()]
        public class AssertionException : Exception
        {
            public AssertionException() { }
            public AssertionException(string message) : base(message) { }
            protected AssertionException(System.Runtime.Serialization.SerializationInfo info,
                     System.Runtime.Serialization.StreamingContext context)
                : base(info, context) { }
        }

        private static string mydeploymentname;

        public static string MyDeploymentId
        {
            get
            {
                if (mydeploymentname == null)
                    try
                    {
                        mydeploymentname = Microsoft.WindowsAzure.ServiceRuntime.RoleEnvironment.DeploymentId;
                    }
                    catch (System.Runtime.InteropServices.SEHException)
                    {
                        // we are in a ASP.NET dev server
                        mydeploymentname = "localdeployment";
                    }
                    catch (System.InvalidOperationException)
                    {
                        // we are in a ASP.NET dev server
                        mydeploymentname = "localdeployment";
                    }
                    catch (System.TypeInitializationException)
                    {
                        // we are in a ASP.NET dev server
                        mydeploymentname = "localdeployment";
                    }

                return mydeploymentname;
            }
        }

    
        private static string myinstancename;

        public static string MyInstanceName
        {
            get
            {
                if (myinstancename == null)
                    try
                    {
                        myinstancename = Microsoft.WindowsAzure.ServiceRuntime.RoleEnvironment.CurrentRoleInstance.Id;
                    }
                    catch (System.Runtime.InteropServices.SEHException)
                    {
                        // we are in a ASP.NET dev server
                        myinstancename = "localsim";
                    }
                    catch (System.InvalidOperationException)
                    {
                        // we are in a ASP.NET dev server
                        myinstancename = "localsim";
                    }
                    catch (System.TypeInitializationException)
                    {
                        // we are in a ASP.NET dev server
                        myinstancename = "localsim";
                    }

                return myinstancename;
            }
        }




        public static bool RunningInAzureSimulator()
        {
            return Util.MyInstanceName.Contains("deployment");
        }


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

        public static Endpoints.ServiceDeployments GetRegion()
        {
            string region = RoleEnvironment.GetConfigurationSettingValue("region");
            if (region == null) throw new Exception("Region property not found");
            switch (region)
            {
                case "uswest":
                    return Endpoints.ServiceDeployments.OrleansGeoUsWest;
                case "europewest":
                    return Endpoints.ServiceDeployments.OrleansGeoEuropeWest;
                case "emulator":
                    return Endpoints.ServiceDeployments.Simulator;
                default:
                    throw new Exception("Unknown Region property");
            }
        }

        // todo change when more than one silo. 
        public static async Task<Tuple<IPAddress,int>> getGrainAddress(Endpoints.ServiceDeployments pSiloEndpoint, Type pGrainType, string pGrainId)
        {
            string connectionKey = "";
            IPAddress grainIP = null;
            int grainPort = 0;
            GrainEntity registerEntity = null;

            switch (pSiloEndpoint)
            {
                case Endpoints.ServiceDeployments.OrleansGeoUsWest:
                    connectionKey = StorageAccounts.GetConnectionString(StorageAccounts.Account.OrleansGeoUsWest);
                    break;
                case Endpoints.ServiceDeployments.OrleansGeoEuropeWest:
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
