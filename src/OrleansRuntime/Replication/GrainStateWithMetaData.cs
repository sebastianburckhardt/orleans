using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.Replication
{
    [Serializable]
    public class GrainStateWithMetaData<TGrainState> : GrainState where TGrainState: GrainState,new()
    {
        /// <summary>
        /// The user-defined grain state
        /// </summary>
        public TGrainState GrainState { get; set; }

        /// <summary>
        /// A sequence number for the global state. Starts at 0, and gets incremented each time a replica commits a batch of updates.
        /// </summary>
        public long GlobalVersion { get; set; }

        /// <summary>
        /// A string representing a bit vector, with one bit per replica. 
        /// Bits are toggled when writing, so that the retry logic can avoid duplicating updates when retrying a failed update. 
        /// </summary>
        public string WriteVector { get; set; }

        public GrainStateWithMetaData()
        {
           GrainState = new TGrainState();
           GlobalVersion = 0;
           WriteVector = "";
        }

        public GrainStateWithMetaData(TGrainState initialstate)
        {
            this.GrainState = initialstate;
            GlobalVersion = 0;
            WriteVector = "";
        }
        
        // BitVector of replicas is implemented as a set of replica strings encoded within a string
        // The bitvector is represented as the set of replica ids whose bit is 1
        // This set is written as a string that contains the replica ids preceded by a comma each
        //
        // Assuming our replicas are named A, B, and BB, then
        // ""     represents    {}        represents 000 
        // ",A"   represents    {A}       represents 100 
        // ",A,B" represents    {A,B}     represents 110 
        // ",BB,A,B" represents {A,B,BB}  represents 111 

        public bool ContainsBit(string Replica)
        {
            var pos = WriteVector.IndexOf(Replica);
            return pos != -1 && WriteVector[pos-1] == ',';
        }

        /// <summary>
        /// toggle the bit and return the new value.
        /// </summary>
        /// <param name="Replica"></param>
        /// <returns></returns>
        public bool ToggleBit(string Replica)
        {
            var pos = WriteVector.IndexOf(Replica);
            if (pos != -1 && WriteVector[pos - 1] == ',')
            {
                var pos2 = WriteVector.IndexOf(',', pos + 1);
                if (pos2 == -1)
                    pos2 = WriteVector.Length;
                WriteVector = WriteVector.Remove(pos - 1, pos2 - pos + 1);
                return false;
            }
            else
            {
                WriteVector = string.Format(",{0}{1}", Replica, WriteVector);
                return true;
            }
        }


        public override IDictionary<string, object> AsDictionary()
        {
            var dictionary = GrainState.AsDictionary();
            dictionary.Add("GlobalVersion", GlobalVersion.ToString());
            dictionary.Add("WriteVector", WriteVector);
            return dictionary;
        }

        public override void SetAll(IDictionary<string, object> values)
        {
            if (values == null)
            {
                GlobalVersion = 0;
                WriteVector = "";
                GrainState.SetAll(null);
            }
            else
            {
                object versionstring;
                if (values.TryGetValue("GlobalVersion", out  versionstring))
                {
                    GlobalVersion = long.Parse((string)versionstring);
                    values.Remove("GlobalVersion");
                }

                object writevector;
                if (values.TryGetValue("WriteVector", out  writevector))
                {
                    WriteVector = (string)writevector;
                    values.Remove("WriteVector");
                }

                GrainState.SetAll(values);
            }
        }

        public override string ToString()
        {
            return string.Format("v{0} Flags={1} Data={2}", GlobalVersion, WriteVector, GrainState);
        }
    }
}
