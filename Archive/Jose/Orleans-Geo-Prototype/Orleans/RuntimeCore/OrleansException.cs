using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization;

namespace Orleans
{
    /// <summary>
    /// An exception class used by the Orleans runtime for reporting errors.
    /// </summary>
    /// <remarks>
    /// This is also the base class for any more specific exceptions 
    /// raised by the Orleans runtime.
    /// </remarks>
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1058:TypesShouldNotExtendCertainBaseTypes")]
    public class OrleansException : ApplicationException
    {
        public OrleansException() : base("Unexpected error.") { }

        public OrleansException(string message) : base(message) { }

        public OrleansException(string message, Exception innerException) : base(message, innerException) { }

        protected OrleansException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// Signifies that a gateway silo is currently in overloaded / load shedding state 
    /// and is unable to currently accept this message being sent.
    /// </summary>
    /// <remarks>
    /// This situation is usaully a transient condition.
    /// The message is likely to be accepted by this or another gateway if it is retransmitted at a later time.
    /// </remarks>
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors")]
    public class OrleansGatewayTooBusyException : OrleansException
    {
        public OrleansGatewayTooBusyException() : base("Gateway too busy") { }

        protected OrleansGatewayTooBusyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// Signifies that a silo is in an overloaded state where some 
    /// runtime limit setting is currently being exceeded, 
    /// and so that silo is unable to currently accept this message being sent.
    /// </summary>
    /// <remarks>
    /// This situation is often a transient condition.
    /// The message is likely to be accepted by this or another silo if it is retransmitted at a later time.
    /// </remarks>
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors")]
    public class OrleansLimitExceededException : OrleansException
    {
        public OrleansLimitExceededException() : base("Limit exceeded") { }

        public OrleansLimitExceededException(string limitName, int current, int threshold, object extraInfo) 
            : base(string.Format("Limit exceeded {0} Current={1} Threshold={2} {3}", limitName, current, threshold, extraInfo)) { }

        public OrleansLimitExceededException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
        }
    }

    /// <summary>
    /// Signifies that a silo has detected a deadlock / loop in a call graph.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Deadlock detection is not enabled by default in Orleans silos, 
    /// because it introduces some extra overhead in call handling.
    /// </para>
    /// <para>
    /// There are some constraints on the types of deadlock that can currently be detected 
    /// by Orleans silos.
    /// </para>
    /// </remarks>
    [Serializable]
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1032:ImplementStandardExceptionConstructors")]
    public class OrleansDeadlockException : OrleansException
    {
        internal IEnumerable<Tuple<GrainId, int, int>> CallChain { get; private set; }

        public OrleansDeadlockException() : base("Deadlock between grain calls") {}

        internal OrleansDeadlockException(IEnumerable<Tuple<GrainId, int, int>> callChain)
            : base(String.Format("Deadlock Exception for grain call chain {0}.", Utils.IEnumerableToString(callChain, 
                            (Tuple<GrainId, int, int> elem) => String.Format("{0}.{1}.{2}", elem.Item1, elem.Item2, elem.Item3)))) 
        {
            CallChain = callChain;
        }

        protected OrleansDeadlockException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {
            if (info != null)
            {
                this.CallChain = (IEnumerable<Tuple<GrainId, int, int>>)info.GetValue("CallChain", typeof(IEnumerable<Tuple<GrainId, int, int>>));
            }
        }

        public override void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info != null)
            {
                info.AddValue("CallChain", this.CallChain, typeof(IEnumerable<Tuple<GrainId, int, int>>));
            }

            base.GetObjectData(info, context);
        }
    }
}

