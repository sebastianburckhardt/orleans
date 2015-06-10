﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class Endpoints
    {


        //----- URLs of service endpoints

        public enum Conductor
        {
            OrleansGeoConductor
        }

        public enum ServiceDeployments
        {
            OrleansGeoUsWest,
            OrleansGeoEuropeWest,
            Simulator
        }
         public enum LoadGenerators
        {
            OrleansGeoLoadUsWest,
            OrleansGeoLoadEuropeWest
        }

    

        //----------- code for picking them

        public static string GetService(int number)
        {
            if (Util.RunningInAzureSimulator())
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
            if (Util.RunningInAzureSimulator())
                // we are running in Azure simulator - use localhost
                return "localhost:81";
            else
                // we are running in Azure - use first service in list
                return GetService(0);
        }

        public static string GetConductor()
        {
            if (Util.RunningInAzureSimulator())
                // we are running in Azure simulator - use localhost
                return "localhost:20473";
            else
                // we are running in Azure - use first service in list
                return MakeServiceEndpoint(((Conductor) 0).ToString());
        }


        public static IEnumerable<string> AllPublicEndpoints()
        {
            foreach (var sd in Enum.GetValues(typeof(ServiceDeployments)))
                if ((ServiceDeployments)sd == ServiceDeployments.Simulator)
                {
                    if (Util.RunningInAzureSimulator())
                        yield return "localhost";
                }
                else
                    yield return MakeServiceEndpoint(sd.ToString());
        }

      
       
    }
}
