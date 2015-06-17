using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TechFestDemo

{
    class SimpleTestProgram
    {
        static void Main(string[] args)
        {
            Console.WriteLine("TechFest demo...");

            /*
            string accountName = "dbtsouthstorage";
            // string accountKey = "d0TDFITH4QMDmVQwtguVUPr7gjD4Pt3ZIhEFWXdflObcyWx7KXzGAYaP2cGzZKM7jAyi/D8t5PZH1XsQg+O3RA==";
            string accountKey = "frU39zZ7M01q9GMSz28KiE2G9lU0EXwC7Lnup1MDd5XlFM6JN6xlvvfKLqX8n2Rk0LIp+YQv2TkedJXSt8N46A==";
            */

   //         StorageCredentials creds = new StorageCredentials(accountName, accountKey);
  //          CloudStorageAccount configAccount = new CloudStorageAccount(creds, false);
            CloudStorageAccount configAccount = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
            CloudBlobClient configBlobClient = configAccount.CreateCloudBlobClient();
            CloudBlobContainer configContainer = configBlobClient.GetContainerReference("configurationdemocontainer");
            BlobRequestOptions options = new BlobRequestOptions();
            OperationContext context = new OperationContext();
            try
            {
                configContainer.CreateIfNotExists(/*options, context*/);
            }
            catch (StorageException ex)
            {
                Console.WriteLine("Storage Exception: " + ex.Message);
            }

            Console.WriteLine("Done. Hit enter to exit.");
            Console.ReadLine();
        }
    }
}
