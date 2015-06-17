using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    
    /*
     * DEPRECATED - DO NOT USE.
     */

    public class CapCloudStorageAccount
    {
        
        /// <summary>
        /// Initializes a new instance of the <see cref="CapCloudStorageAccount"/> class using the specified
        /// storage accounts.
        /// </summary>
        public CapCloudStorageAccount()
        {
        }

        /// <summary>
        /// Creates the Blob service client.
        /// </summary>
        /// <param name="slaEngine">The default consistency-based SLA to use when reading from this storage.</param>
        /// <returns>A client object that specifies the Blob service endpoint.</returns>
        public CapCloudBlobClient CreateCloudBlobClient(NumberOfClients numberOfClients)
        {
            return new CapCloudBlobClient(numberOfClients);
        }

        /// <summary>
        /// Creates the Blob service client.
        /// </summary>
        /// <returns>A client object that specifies the Blob service endpoint.</returns>
        public CapCloudBlobClient CreateCloudBlobClient()
        {
            return new CapCloudBlobClient();
        }

    }
}
