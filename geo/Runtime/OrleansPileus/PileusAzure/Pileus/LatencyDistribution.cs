using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Runtime.Serialization;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    [Serializable()]
    public class LatencyDistribution 
    {
        #region Locals
        // smallest latency value (in milliseconds)
        long m_min;

        // largest latency value (in milliseconds)
        long m_max;

        // interval length 
        long m_intervalLength;

        // how many intervals are there?
        // (m_max - m_min) / m_intervalLength
        int m_numIntervals;

        // number of entries in each interval
        int[] m_distribution;

        // sum(m_distribution[i]), for all i.
        int m_totalEntries;

        // we maintain a window of most recent
        // values to compute the average latency.
        // m_windowSize represents the number
        // of entries in the window.
        int m_windowSize;

        // a queue of most recent values
        //We do not serialize this, since this field is not improtant during reconfiguration, hence we won't write it in the configuratino blob.
        [NonSerialized()]
        Queue<long> m_queue;

        // sum of all the values
        long m_total;
        #endregion


        //
        // removes a value from the distribution
        //
        private
        void
        _Remove(
            long val
            )
        {
            Debug.Assert(!((val < m_min) || (val > m_max)), "This is not supported yet ...\n");
            int i = (int)((val - m_min) / m_intervalLength);

            m_distribution[i]--;
            m_totalEntries--;
        }


        public
        LatencyDistribution(
            long min,
            long max,
            long intervalLen,
            int windowSize = 0
            )
        {
            Debug.Assert((min <= max));

            m_min = min;
            m_max = max;
            m_intervalLength = intervalLen;
            m_numIntervals = (int)((m_max - m_min) / m_intervalLength);

            int diff = (int)(m_max - m_min) + 1;
            if (diff % (int)m_intervalLength != 0)
            {
                m_numIntervals++;
            }

            m_distribution = new int[m_numIntervals];
            for (int i = 0; i < m_numIntervals; i++)
            {
                m_distribution[i] = 0;
            }
            m_totalEntries = 0;

            m_windowSize = windowSize;
            if (m_windowSize > 0)
            {
                m_queue = new Queue<long>();
            }
            else
            {
                m_queue = null;
            }

            m_total = 0;
        }

        ~LatencyDistribution()
        {
        }


        //
        // Adds a latency value to the distribution.
        //
        public void
        Add(
            long val
            )
        {
            if (val > m_max)
            {
                val=m_max;
            }
            else if (val < m_min)
            {
                val = m_min;
            }

            Debug.Assert(!((val < m_min) || (val > m_max)), "This is not supported yet ...\n");

            m_total += val;

            // if we are maintaining a window of most recent
            // values, then add the new one and if the window
            // is full, then remove a old value.
            if (m_queue != null)
            {
                m_queue.Enqueue(val);
                if (m_queue.Count > m_windowSize)
                {
                    long oldVal = m_queue.Dequeue();
                    _Remove(oldVal);

                    Debug.Assert((m_total - oldVal) >= 0);
                    m_total -= oldVal;
                }
            }

            // include the new value into the distribution
            int i = (int)((val - m_min) / m_intervalLength);

            m_distribution[i]++;
            m_totalEntries++;

            if (m_queue != null)
            {
                Debug.Assert(m_queue.Count == m_totalEntries);
            }
        }


        //
        // Returns the probability that a RTT from a server
        // is within the given value.
        //
        public
        float
        ProbabilityOfFindingValueLessThanGiven(
            long val, bool optimistic=true
            )
        {
            float prob = 0;

            if (m_totalEntries <= 0 && optimistic)
            {
                // no data so assume that the server is fast enough until we learn otherwise
                return 1;
            }
            else if (m_totalEntries <= 0 && !optimistic)
            {
                return 0;
            }

            if (val > m_max)
            {
                prob = 1;
            }

            if ((val >= m_min) && (val <= m_max))
            {
                int bucket = (int)((val - m_min) / m_intervalLength);
                float numEntries = 0;
                for (int i = 0; i < bucket; i++)
                {
                    numEntries += m_distribution[i];
                }

                prob = numEntries / m_totalEntries;
            }

            return prob;
        }

        public int GetTotalEntries()
        {
            return m_totalEntries;
        }
        
        //
        // Returns the average RTT to a server
        //
        public double FindAverage()
        {
            double result = (m_total * 1.0) / m_totalEntries;
            return result;
        }

    }
}
