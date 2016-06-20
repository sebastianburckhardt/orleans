using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.LogViews
{
    /// <summary>
    /// Used by StorageProviderLogViewAdaptor, which allows a plain storage provider to
    /// be wrapped by a log view provider, by adding meta data to the grain state.
    /// </summary>
    /// <typeparam name="TView">The view state</typeparam>
    [Serializable]
    public class GrainStateWithMetaDataAndETag<TView> : IGrainState where TView: class, new()
    {
        public GrainStateWithMetaData<TView> StateAndMetaData { get; set; }
       
        public string ETag { get; set; }

        object IGrainState.State
        {
            get
            {
                return StateAndMetaData;
            }
            set
            {
                StateAndMetaData = (GrainStateWithMetaData<TView>) value;
            }
        }

        public GrainStateWithMetaDataAndETag(TView initialview)
        {
            StateAndMetaData = new GrainStateWithMetaData<TView>(initialview);
        }
        public GrainStateWithMetaDataAndETag()
        {
            StateAndMetaData = new GrainStateWithMetaData<TView>();
        }

        public override string ToString()
        {
            return string.Format("v{0} Flags={1} ETag={2} Data={3}", StateAndMetaData.GlobalVersion, StateAndMetaData.WriteVector, ETag, StateAndMetaData.State);
        }
    }


    [Serializable]
    public class GrainStateWithMetaData<TView> where TView : class, new()
    {
        /// <summary>
        /// The stored view of the log
        /// </summary>
        public TView State { get; set; }

        /// <summary>
        /// The length of the log
        /// </summary>
        public int GlobalVersion { get; set; }


        /// <summary>
        /// Used to avoid duplicate appends.
        /// A string representing a bit vector, with one bit per replica. 
        /// Bits are toggled when writing, so that the retry logic can avoid appending an entry twice
        /// when retrying a failed append. 
        /// </summary>
        public string WriteVector { get; set; }

        public GrainStateWithMetaData()
        {
            State = new TView();
            GlobalVersion = 0;
            WriteVector = "";
        }

        public GrainStateWithMetaData(TView initialstate)
        {
            this.State = initialstate;
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
            return pos != -1 && WriteVector[pos - 1] == ',';
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

    }
}
