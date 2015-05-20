using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Orleans.Serialization
{
    internal class DeserializationContext
    {
        [ThreadStatic]
        private static DeserializationContext ctx;

        internal static DeserializationContext Current
        {
            get
            {
                if (ctx == null)
                {
                    ctx = new DeserializationContext();
                }
                return ctx;
            }
        }

        private readonly Dictionary<int, object> taggedObjects;

        private DeserializationContext()
        {
            taggedObjects = new Dictionary<int, object>();
        }

        internal void Reset()
        {
            taggedObjects.Clear();
        }

        internal void RecordObject(int offset, object obj)
        {
            taggedObjects[offset] = obj;
        }

        internal object FetchReferencedObject(int offset)
        {
            object result;
            if (!taggedObjects.TryGetValue(offset, out result))
            {
                throw new SerializationException("Reference with no referred object");
            }
            return result;
        }
    }
}
