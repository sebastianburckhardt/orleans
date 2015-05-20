using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
    public static class Util
    {

        public static void Assert(bool condition, string message = null)
        {
            if (condition)
                return;
            if (!string.IsNullOrEmpty(message))
                throw new AssertionException(message);
            else
                throw new AssertionException();
        }

        public static void Fail(string message)
        {
            throw new AssertionException(message);
        }

        [Serializable()]
        public class AssertionException : Exception
        {
            public AssertionException() { }
            public AssertionException(string message) : base(message) { }
            protected AssertionException(System.Runtime.Serialization.SerializationInfo info,
                     System.Runtime.Serialization.StreamingContext context)
                : base(info, context) { }
        }
    }
}
