using Microsoft.WindowsAzure.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Runtime.Common
{
    public static class StorageAccounts
    {
       
        public enum Account
        {
            DevStorage,
            OrleansGeoSharedStorage,
            OrleansGeoUsWest,
            OrleansGeoEuropeWest
        }


 
        public static string GetConnectionString(Account account)
        {
             switch(account)
             {
                 case Account.OrleansGeoSharedStorage:
                     return "DefaultEndpointsProtocol=https;AccountName=orleansgeosharedstorage;AccountKey=xhWdo16SU3RkAwL/WEeFAaJhHQeNApHKL4j1FmBfKRUa7hfAGZylBql9a5BWHmzO6vdmyNG4h9B6nq6fBWQbvg==";
                 case Account.OrleansGeoUsWest:
                     return "DefaultEndpointsProtocol=https;AccountName=orleansgeouswest;AccountKey=pUIT+LRC/GohbXWMSAfKhuo3eLU8dz0LVUxHvb/X1BO7CZPs4aZ712vtwRPDxa388jbWJArz6hvsvIZTl96g0g==";
                 case Account.OrleansGeoEuropeWest:
                     return "DefaultEndpointsProtocol=https;AccountName=orleansgeoeuropewest;AccountKey=qZzwwZHfpBdoGnoPZ7Z3HS9kziEDdEZv16H4bbVIRPFOQkTGbtRoIJPRWn3oQWcQDa7n9hIhiaViCmTtlxYZew==";
                 default:
                     return "UseDevelopmentStorage=true";
             }
         }


        public static Account GetTracingAccount()
        {
            if (Util.RunningInAzureSimulator())
                return Account.DevStorage;
            else
                return Account.OrleansGeoSharedStorage;
        }

    }
}
