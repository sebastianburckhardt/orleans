using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    [Serializable()]
    public class ServiceLevelAgreement : IList<SubSLA>
    {

        /// <summary>
        /// Ordered list of SubSLAs that comprise the total SLA.
        /// </summary>
        private List<SubSLA> subSLAs;

        /// <summary>
        /// Constructs a new empty SLA.
        /// </summary>
        public ServiceLevelAgreement(string Id)
        {
            this.subSLAs = new List<SubSLA>();
            this.Id = Id;
        }

        /// <summary>
        /// Constructs a new SLA with a single subSLA.
        /// </summary>
        public ServiceLevelAgreement(string Id, SubSLA item)
        {
            this.subSLAs = new List<SubSLA>();
            this.subSLAs.Add(item);
            this.Id = Id;
        }

        /// <summary>
        /// SLAId used for merging similar SLAs with each other. 
        /// </summary>
        public string Id { get; private set; }

        #region IList

        public void Update(ServiceLevelAgreement sla)
        {
            for (int i = 0; i < subSLAs.Count; i++)
            {
                this[i].Update(sla[i]);
            }
        }

        public int IndexOf(SubSLA item)
        {
            return subSLAs.IndexOf(item);
        }

        public void Insert(int index, SubSLA item)
        {
            subSLAs.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            subSLAs.RemoveAt(index);
        }

        public SubSLA this[int index]
        {
            get
            {
                return subSLAs[index];
            }
            set
            {
                subSLAs[index] = value;
            }
        }

        public void Add(SubSLA item)
        {
            subSLAs.Add(item);
        }

        public void Clear()
        {
            subSLAs.Clear();
        }

        public bool Contains(SubSLA item)
        {
            return subSLAs.Contains(item);
        }

        public void CopyTo(SubSLA[] array, int arrayIndex)
        {
            subSLAs.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return subSLAs.Count; }
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }

        public bool Remove(SubSLA item)
        {
            return subSLAs.Remove(item);
        }

        public IEnumerator<SubSLA> GetEnumerator()
        {
            return subSLAs.GetEnumerator();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }

        #endregion

        #region Hits and Misses

        public float GetTotalMissedUtility()
        {
            float result = 0;
            foreach (SubSLA s in subSLAs)
            {
                result += s.NumberOfMisses * s.Utility;
            }
            return result;
        }

        public float GetTotalHitUtility()
        {
            float result = 0;
            foreach (SubSLA s in subSLAs)
            {
                result += s.NumberOfHits * s.Utility;
            }
            return result;
        }

        public float GetAverageDeliveredUtility()
        {
            float result = 0;
            int num = 0;
            int misses = 0;
            foreach (SubSLA s in subSLAs)
            {
                result += s.NumberOfHits * s.Utility;
                num += s.NumberOfHits;
                misses = s.NumberOfMisses;   // we only care about misses for the last subSLA
            }
            num += misses;
            if (num > 0)
            {
                result = result / num;
            }
            return result;            
        }

        /// <summary>
        /// Resets the number of hits and misses to zero.
        /// This operation is called after a reconfiguration happens in the system. 
        /// </summary>
        public void ResetHitsAndMisses()
        {
            foreach (SubSLA s in subSLAs){
                s.NumberOfMisses=0;
                s.NumberOfHits=0;
            }
        }
        /// <summary>
        /// This is only for the emulation purposes, and measurements.
        /// </summary>
        /// <param name="numberOfClients"></param>
        public void AdjustHitsAndMisses(int numberOfClients)
        {
            float sum = 1;
            foreach (SubSLA s in subSLAs)
            {
                sum += s.NumberOfHits + s.NumberOfMisses;
            }

            foreach (SubSLA s in subSLAs)
            {
                s.NumberOfHits =Convert.ToInt32( (s.NumberOfHits / sum) * numberOfClients*100);
                s.NumberOfMisses =Convert.ToInt32( (s.NumberOfMisses / sum) * numberOfClients*100);
            }
        }

        #endregion

        public byte[] ToBytes()
        {
            using (MemoryStream stream = new MemoryStream())
            {
                BinaryFormatter formatter = new BinaryFormatter();
                formatter.Serialize(stream, this);
                stream.Position = 0;
                return stream.ToArray();
            }
        }

    }

    
    /// <summary>
    /// A service level agreement consists of a list of desired latency/consistency pairs with associated utilities.  
    /// This class defines each of the subSLAs that are part of a complete SLA.
    /// </summary>
    [Serializable()]
    public class SubSLA
    {

        /// <summary>
        /// Desired latency in millseconds.
        /// </summary>
        private int latency;

        /// <summary>
        /// Desired consistency.
        /// </summary>
        private Consistency consistency;

        /// <summary>
        /// Time bound in seconds if bounded consistency is chosen.
        /// </summary>
        private int bound;

        /// <summary>
        /// An indication of the relative value of this subSLA to the client.
        /// </summary>
        private float utility;

        /// <summary>
        /// Constructs a new SubSLA.
        /// </summary>
        /// <param name="latency">desired latency (in milliseconds)</param>
        /// <param name="consistency">desired consistency</param>
        /// <param name="bound">time-bound for boudned staleness (in seconds)</param>
        /// <param name="utility">value of meeting this SubSLA</param>
        public SubSLA(int latency, Consistency consistency, int bound = 0, float utility = 1.0F)
        {
            this.latency = latency;
            this.consistency = consistency;
            this.bound = bound;
            this.utility = utility;

            this.NumberOfHits=0;
            this.NumberOfMisses=0;
        }

        public int Latency { get { return latency; } }
        public Consistency Consistency { get { return consistency; } }
        public int Bound { get { return bound; } }
        public float Utility { get { return utility; } }

        // Each subSLA records the hits and misses for read opeartions using its parent SLA
        // A "hit" indicates that the subSLA was met and no higher ranked subSLA was met
        // A "miss" indicates that the subSLA was not met and neither was a higher subSLA

        public int NumberOfHits { get; internal set; }
        public int NumberOfMisses { get; internal set; }

        public void Hit() { 
            NumberOfHits++; 
        }

        public void Miss() { 
            NumberOfMisses++; 
        }

        public void Update(SubSLA subSLA)
        {
            this.NumberOfHits = subSLA.NumberOfHits;
            this.NumberOfMisses = subSLA.NumberOfMisses;
        }

    }

    /// <summary>
    /// Orders two SLAs based on their missed utility.
    /// </summary>
    public class ServiceLevelAgreementComparer : IComparer<ServiceLevelAgreement>
    {
        public int Compare(ServiceLevelAgreement arg0, ServiceLevelAgreement arg1)
        {
            if (arg0 == null && arg1 != null)
                return 1;
            else if (arg0 != null && arg1 == null)
                return -1;
            else if (arg0 == null && arg1 == null)
                return 0;

            if (arg0.GetTotalMissedUtility() > arg1.GetTotalMissedUtility())
                return 1;
            else
                return -1;
        }
    }

}
