using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections.Concurrent;

namespace PileusApp.Utils
{
    public class MeasurementType
    {
        public MeasurementType()
        {
            Values = new ConcurrentBag<float>();
        }

        public ConcurrentBag<float> Values { get; set; }

        public float GetAverage()
        {
            float total=0;
            foreach (float t in Values)
            {
                total += t;
            }
            return total / Values.Count();
        }

        public float GetTotal()
        {
            float total = 0;
            foreach (float t in Values)
            {
                total += t;
            }
            return total;
        }
    }
}
