using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    public static class AzureStorageLocations
    {
        /// <summary>
        /// Returns all of the datacenter locations in Windows Azure Storage.
        /// </summary>
        /// <returns>a list of location names</returns>
        public static List<string> AllSites()
        {
            List<string> locations = new List<string> 
            { "North Central US", "South Central US", "East US", "West US", "North Europe", "West Europe",
            "South East Asia", "East Asia", "East China", "North China", "East Japan", "West Japan", "Brazil" };
            return locations;
        }

        /// <summary>
        /// Returns the secodary location for an account in Windows Azure Storage.
        /// </summary>
        /// <returns>a location name</returns>
        public static string SecondarySite(string primary)
        {
            string secondary;
            switch (primary)
            {
                case "North Central US":
                    secondary = "South Central US";
                    break;
                case "South Central US":
                    secondary = "North Central US";
                    break;
                case "East US":
                    secondary = "West US";
                    break;
                case "West US":
                    secondary = "East US";
                    break;
                case "North Europe":
                    secondary = "West Europe";
                    break;
                case "West Europe":
                    secondary = "North Europe";
                    break;
                case "South East Asia":
                    secondary = "East Asia";
                    break;
                case "East Asia":
                    secondary = "South East Asia";
                    break;
                case "East China":
                    secondary = "North China";
                    break;
                case "North China":
                    secondary = "East China";
                    break;
                case "East Japan":
                    secondary = "West Japan";
                    break;
                case "West Japan":
                    secondary = "East Japan";
                    break;
                case "Brazil":
                    secondary = "South Central US";
                    break;
                default:
                    secondary = "unknown";
                    break;
            }
            return secondary;
        }
    }
}
