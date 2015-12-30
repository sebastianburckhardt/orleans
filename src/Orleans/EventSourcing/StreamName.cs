using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.EventSourcing
{
    static class StreamName
    {
        public static string GetName(string grainType, GrainReference grainReference, ICustomStreamName customName)
        {
            return customName?.GetStreamName() ?? GetDefaultStreamName(grainType, grainReference);
        }

        private static string GetDefaultStreamName(string grainType, GrainReference grainReference)
        {
            return string.Format("{0}-{1}", grainType, grainReference.ToKeyString());
        }
    }
}
