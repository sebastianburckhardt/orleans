using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class Endpoints
    {


        //----- URLs of endpoints

        public enum Conductor
        {
            OrleansGeoConductor
        }

        public enum ServiceDeployments
        {
            OrleansGeoUsWest,
            OrleansGeoEuropeWest
        }
        public enum ServiceStorage
        {
            OrleansGeoUsWest,    
            OrleansGeoEuropeWest
        }
        public enum LoadGenerators
        {
            OrleansGeoLoadUsWest,
            OrleansGeoLoadEuropeWest
        }


        //----------- code for picking them

        public static string GetService(int number)
        {
            if (RunningInAzureSimulator())
                // we are running in Azure simulator - use localhost
                return "localhost:81";
            else
            {
                var vals = Enum.GetValues(typeof(ServiceDeployments));
                return MakeServiceEndpoint(((ServiceDeployments)(number % vals.Length)).ToString());
            }
        }

        private static string MakeServiceEndpoint(string s)
        {
            return s.ToLower() + ".cloudapp.net";
        }


        public static string GetDefaultService()
        {
            if (RunningInAzureSimulator())
                // we are running in Azure simulator - use localhost
                return "localhost:81";
            else
                // we are running in Azure - use first service in list
                return GetService(0);
        }

        public static string GetConductor()
        {
            if (RunningInAzureSimulator())
                // we are running in Azure simulator - use localhost
                return "localhost:20473";
            else
                // we are running in Azure - use first service in list
                return MakeServiceEndpoint(((Conductor) 0).ToString());
        }

      

        public static bool RunningInAzureSimulator()
        {
            return MyLocation.Contains("deployment");
        }


        private static string myinstancename;

        public static string MyLocation
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


    }
}
