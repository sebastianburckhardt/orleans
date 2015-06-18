using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions
{
    public enum ConfigurationActionSource
    {
        //states that this action is created beceause a configurator wants to improve SLA's utility.
        NonConstraint,
        //states that this action is created to address a constraint
        Constraint
    }

    /// <summary>
    /// Abstract class of a ConfigurationAction. Any configuration action should extend this class.
    /// </summary>
    public abstract class ConfigurationAction
    {
        

        protected int numberOfReads;
        protected int numberOfWrites;

        //computing number of blobs everytime is very long. Hence, we compute it only once.
        //TODO: it should be refreshed frequently. 
        //Sice we use YCSB for evaluation, and number of objects are fixed throughout the execution, refreshing is not implmeneted. 
        protected static int numberOfBlobs = 0;

        internal ConfigurationAction(string containerName, string serverName, float? utility, string SLAId=null, ConfigurationActionSource source=ConfigurationActionSource.NonConstraint, int numberOfReads=0, int numberOfWrites=0)
        {
            this.Configuration = ClientRegistry.GetConfiguration(containerName,false);
            this.ModifyingContainer = ClientRegistry.GetCloudBlobContainer(serverName, containerName);
            this.GainedUtility = utility ?? 0;
            OriginatingSLAs = new HashSet<string>();
            Clients = new HashSet<string>();
            if (SLAId!=null)
                OriginatingSLAs.Add(SLAId);

            this.Source = source;
            this.numberOfReads = numberOfReads;
            this.numberOfWrites = numberOfWrites;
            this.ServerName = serverName;

            if (numberOfBlobs == 0)
            {
                CloudBlobContainer primaryContainer = ClientRegistry.GetCloudBlobContainer(Configuration.PrimaryServers.First(), containerName);
                numberOfBlobs = primaryContainer.ListBlobs().Count();
            }

            Cost=ComputeCost();
        }

        // Container's configuration
        public ReplicaConfiguration Configuration { get; protected set; }

        // The container of the server that the action will be performed on it.
        public CloudBlobContainer ModifyingContainer { get; protected set; }

        public float GainedUtility { get; private set; }

        public HashSet<string> OriginatingSLAs{get; private set; }
        public HashSet<string> Clients { get; private set; }

        public ConfigurationActionSource Source { get; private set; }

        public string ServerName { get; set; }

        /// <summary>
        /// The cost of new configuration once this actions is applied.
        /// </summary>
        public double Cost { get; private set; }

        public string Id
        {
            get
            {
                
                return this.GetType().Name.ToString() + "_" + ServerName + "_" +this.ModifyingContainer.Name.ToString();
            }
        }

        public void AddUtility(float utility)
        {
            GainedUtility += utility;
        }

        public override bool Equals(System.Object obj)
        {
            if (obj == null)
                return false;

            ConfigurationAction p = (ConfigurationAction)obj;
            if (p == null)
                return false;

            return (this.Id == p.Id);

        }

        public string Logs { get; private set; }

        public override int GetHashCode()
        {
            return this.Id.GetHashCode();
        }

        public void Merge(ConfigurationAction action)
        {
            if (this.Id.Equals(action.Id))
            {
                this.GainedUtility += action.GainedUtility;
            }
            action.OriginatingSLAs.ToList().ForEach(s => this.OriginatingSLAs.Add(s));
        }

        /// <summary>
        /// Executes the action in Pileus, and performs the configuration.
        /// </summary>
        public abstract void Execute();

        /// <summary>
        /// returns a new configuration by applying the action to the input current configuration.
        /// </summary>
        /// <param name="current">The current configuration</param>
        /// <returns></returns>
        public abstract ReplicaConfiguration IfApply(ReplicaConfiguration current);

        /// <summary>
        /// returns the number of replicas that will be added upon the executed of this action.
        /// </summary>
        /// <returns>0 if no new replica will be added, 1 if one new replica will be added, and -1 if a replica will be removed.</returns>
        public abstract int NumberOfAddingReplica();

        /// <summary>
        /// computes the cost of applying this action.
        /// </summary>
        /// <returns></returns>
        public abstract double ComputeCost();

        public override string ToString()
        {
            return Id + ", Cost: " + Cost + " , GainedUtility: " + GainedUtility;
        }

        protected void AppendToLogger(string log)
        {
            long curSecond = DateTime.Now.ToUniversalTime().Ticks / 10000000;
            Logs += curSecond +  "," + log + "\n";
        }
    }

    public class ConfigurationActionComparer : IComparer<ConfigurationAction>
    {
        public int Compare(ConfigurationAction arg0, ConfigurationAction arg1)
        {
            //We assume the cost is 1 unit if it is zero to be able to perform the division.
            double cost0 = arg0.Cost == 0 ? 1 : arg0.Cost;
            double cost1 = arg1.Cost == 0 ? 1 : arg1.Cost;

            double v0 = arg0.GainedUtility / cost0;
            double v1=arg1.GainedUtility / cost1;

            if (v0 > v1)
                return 1;
            else if (v1 > v0)
                return -1;
            else 
                return 0;
        }
    } 
}
