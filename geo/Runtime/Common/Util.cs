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

namespace GeoOrleans.Runtime.Common
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
            set
            {
                mydeploymentname = value;
            }
        }

    
        private static string myinstancename;

        public static string MyInstanceName
        {
            get
            {
                // if this has not been set
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
            set
            {
                myinstancename = value;
            }
        }




        public static bool RunningInAzureSimulator()
        {
            return Util.MyInstanceName.Contains("deployment");
        }


     

   

        public static AzureEndpoints.ServiceDeployments GetRegion()
        {
            string region = RoleEnvironment.GetConfigurationSettingValue("region");
            if (region == null) throw new Exception("Region property not found");
            switch (region)
            {
                case "uswest":
                    return AzureEndpoints.ServiceDeployments.OrleansGeoUsWest;
                case "europewest":
                    return AzureEndpoints.ServiceDeployments.OrleansGeoEuropeWest;
                case "emulator":
                    return AzureEndpoints.ServiceDeployments.Simulator;
                default:
                    throw new Exception("Unknown Region property");
            }
        }

    

    }



}
