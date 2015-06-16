using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Threading;
using System.Net;

namespace Microsoft.WindowsAzure.Storage.Pileus
{

    /*
     * DEPRECATED - DO NOT USE.
     */

    /// <summary>
    /// Returns actual number of clients per tick divided by standard number of clients per tick.
    /// </summary>
    /// <returns>actual number of clients</returns>
    public delegate int NumberOfClients();

    /// <summary>
    /// Wrapper around CloudBlobClient in the context of Pileus.
    /// Hence, it returns CapCloudBlobContainer object instead of CloudBlobContainer.
    /// 
    /// </summary>
    public class CapCloudBlobClient
    {

        /// <summary>
        ///Maps container's name to the list of all slaEngines for that particular container. 
        ///For example, a client can call GetContainerReference several times with various SLAs.
        /// 
        public static Dictionary<string, List<ConsistencySLAEngine>> slaEngines = new Dictionary<string, List<ConsistencySLAEngine>>();

        /// <summary>
        ///Note: this is only for the emulation purposes. In real executions, this function should not be used. 
        ///Pointer to a delegate that returns the correct number of clients at a particular time. 
        ///This function is used for adjusting the hit and miss ratios because: in emulation, at each tick, and once a client exits, a new client is created to replace it. 
        ///Suppose there must be one client in the U.S. during 1 hour. Once the client finishes its execution, and system will create a new client, so to always keeps the total number of active clients equal to one. 
        ///Now, if the replica is next to the client, the client will execute very very fast, its hits will be way high (because of lots of clients creating one after another).
        ///Therefore, this delegate is used to get the real number of clients (which is one), and adjust the hit and miss ratios accordingly.
        /// </summary>
        public static NumberOfClients numberOfClients;

        /// <summary>
        /// 
        /// </summary>
        public CapCloudBlobClient(NumberOfClients numberOfClients)
        {
            this.Name = Guid.NewGuid().ToString();
            CapCloudBlobClient.numberOfClients = numberOfClients;
        }

        public static int ActualNumberOfClients()
        {
            return 1;
        }

        public CapCloudBlobClient(): this(ActualNumberOfClients)
        {
        }

        /// <summary>
        /// Name of the client.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Returns a reference to a <see cref="CapCloudBlobContainer"/> object with the specified name.
        /// </summary>
        /// <param name="containerName">The name of the container, or an absolute URI to the container.</param>
        /// <param name="engine">The SLA engine.</param>
        /// <returns>A reference to a container.</returns>
        public CapCloudBlobContainer GetContainerReference(string containerName, ConsistencySLAEngine slaEngine)
        {
            CapCloudBlobContainer result = null;
            if (!slaEngines.ContainsKey(containerName))
            {
                slaEngines[containerName] = new List<ConsistencySLAEngine>();
            }
            slaEngines[containerName].Add(slaEngine);

            //result = new CapCloudBlobContainer(containerName, slaEngine, this.Name);
            return result;
        }

        /// <summary>
        /// Returns a reference to a <see cref="CapCloudBlobContainer"/> object with the specified name.
        /// </summary>
        /// <param name="containerName">The name of the container, or an absolute URI to the container.</param>
        /// <returns>A reference to a container.</returns>
        public CapCloudBlobContainer GetContainerReference(string containerName)
        {
            CapCloudBlobContainer result = null;
            if (!slaEngines.ContainsKey(containerName))
            {
                slaEngines[containerName] = new List<ConsistencySLAEngine>();
            }
            // TODO: Create a default SLA and default configuration
            ConsistencySLAEngine slaEngine = new ConsistencySLAEngine(null, null);
            slaEngines[containerName].Add(slaEngine);

            //result = new CapCloudBlobContainer(containerName, slaEngine, this.Name);
            return result;
        }
       
    }

}
