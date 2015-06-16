using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.WindowsAzure.Storage.Pileus.Configuration
{
    
    public class SessionStateTableEntity : TableEntity
    {

        // TODO: There is no need to have separate lists for different classes of servers.  Just have one list of ServerState.
        private Dictionary<string, ServerState> secondaryReplicaServers;
        public byte[] secondaryByte { get; set; }

        private Dictionary<string, ServerState> primaryReplicaServers;
        public byte[] primaryByte { get; set; }

        private Dictionary<string, ServerState> nonReplicaServers;
        public byte[] nonReplicaByte { get; set; }

        public SessionStateTableEntity() { }

        public SessionStateTableEntity(SessionState state, ServerMonitor monitor, Dictionary<string, ServerState> nonReplicaServers, string containerName, string epochId, string clientName) {
            this.secondaryReplicaServers = new Dictionary<string, ServerState>();
            this.primaryReplicaServers = new Dictionary<string, ServerState>();
            this.nonReplicaServers = new Dictionary<string, ServerState>();

            this.PartitionKey = containerName + epochId;
            this.RowKey = clientName;

            // foreach (ServerState s in state.replicas.Values)
            foreach (ServerState s in monitor.GetAllServersState())
                {
                if (s.IsPrimary)
                {
                    primaryReplicaServers[s.Name] = s;
                }
                else
                {
                    secondaryReplicaServers[s.Name] = s;
                }
            }
            this.nonReplicaServers = nonReplicaServers;

            secondaryByte = ToBytes(secondaryReplicaServers);
            primaryByte = ToBytes(primaryReplicaServers);
            nonReplicaByte = ToBytes(nonReplicaServers);

            NumberOfReads = state.GetNumberOfReadsPerMonth();
            NumberOfWrites = state.GetNumberOfWritesPerMonth();
        }

        // TODO: remove these methods.  It should simply store data that is retrieved from a table.

        public Dictionary<string, ServerState> GetSecondaryReplicaServers()
        {
            if (secondaryReplicaServers == null && secondaryByte!=null)
                secondaryReplicaServers = FromBytes(secondaryByte);
            return secondaryReplicaServers;
        }

        public Dictionary<string, ServerState> GetPrimaryReplicaServers()
        {
            if (primaryReplicaServers== null)
                primaryReplicaServers = FromBytes(primaryByte);
            return primaryReplicaServers;
        }

        public Dictionary<string, ServerState> GetNonReplicaServers()
        {
            if (nonReplicaServers == null && nonReplicaByte!=null)
                nonReplicaServers = FromBytes(nonReplicaByte);
            return nonReplicaServers;
        }


        public int NumberOfReads { get; set; }
        public int NumberOfWrites { get; set; }

        public string ClientName {
            get
            {
                return this.RowKey;
            }
        }

        /// <summary>
        /// Returns union of servers that are secondary replica, along with those that do not replicate.
        /// </summary>
        /// <returns></returns>
        public List<ServerState> GetNonPrimaryServers()
        {
            List<ServerState> result = new List<ServerState>();

            secondaryReplicaServers.Values.ToList().ForEach(r => result.Add(r));

            nonReplicaServers.Values.ToList().ForEach(r => result.Add(r));

            return result;
        }

        /// <summary>
        /// Returns the union of servers that are either primary replica or secondary replica.
        /// </summary>
        /// <returns></returns>
        public List<ServerState> GetReplicaServers()
        {
            List<ServerState> result = new List<ServerState>();

            secondaryReplicaServers.Values.ToList().ForEach(r => result.Add(r));

            primaryReplicaServers.Values.ToList().ForEach(r => result.Add(r));

            return result;
        }

        private byte[] ToBytes(Dictionary<string, ServerState> state)
        {
            if (state == null)
                return null;

            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, state);
                stream.Position = 0;
                return stream.ToArray();
            }
        }

        private Dictionary<string, ServerState> FromBytes(byte[] stateByte)
        {
            using (MemoryStream stream = new MemoryStream())
            {
                stream.Write(stateByte, 0, stateByte.Count());
                stream.Position = 0;
                BinaryFormatter formatter = new BinaryFormatter();
                return (Dictionary<string, ServerState>)formatter.Deserialize(stream);
            }
        }

    }
}
