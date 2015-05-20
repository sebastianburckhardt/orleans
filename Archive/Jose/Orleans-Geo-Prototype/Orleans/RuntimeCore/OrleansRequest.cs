using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;


namespace Orleans
{
    /// <summary>
    /// Data object holding metadata associated with a grain Invoke request.
    /// </summary>
    [Serializable]
    public sealed class InvokeMethodRequest
    {
        // TODO ageller: maybe this should implement IJsonSerializable?

        /// <summary> InterfaceId for this Invoke request. </summary>
        public int InterfaceId      { get; private set; }
        /// <summary> MethodId for this Invoke request. </summary>
        public int MethodId         { get; private set; }
        /// <summary> Arguments for this Invoke request. </summary>
        public object[] Arguments   { get; private set; }

        internal InvokeMethodRequest(int interfaceId, int methodId, object[] arguments)
        {
            InterfaceId = interfaceId;
            MethodId = methodId;
            Arguments = arguments;
        }

        /// <summary> 
        /// String representation for this Invoke request. 
        /// </summary>
        /// <remarks>
        /// Note: This is not the serialized wire form of this Invoke request.
        /// </remarks>
        public override string ToString()
        {
            return String.Format("InvokeMethodRequest {0}:{1}", InterfaceId, MethodId);
        }
    }

    /// <summary>
    /// Invoke options for an <c>InvokeMethodRequest</c>
    /// </summary>
    /// <remarks>
    /// These flag values are used in Orleans generated invoker code, and should not be altered. </remarks>
    [Flags]
    public enum InvokeMethodOptions
    {
        None = 0,

        OneWay = 0x04,
        ReadOnly = 0x08,
        Unordered = 0x10,
        DelayForConsistency = 0x20,
        AlwaysInterleave = 0x100, // AlwaysInterleave is a requst type that can interleave with any other request type, including write request.

        // NOTE: The values below are currently unused. 
        // Leave here for backward compat until we next regerenerate the Halo Presence grains used in the Nightly LoadTest.
        // NOTE: ensure these stay in sync with TaskType enum values
        TaskTypeRequires = 0x00,
        TaskTypeSupports = 0x01,
        TaskTypeDoesNotSupport = 0x02,
        TaskTypeRequiresNew = 0x03,
        TaskTypeMask = 0x03,
    }

    // used for tracking request invocation history for deadlock detection.
    [Serializable]
    internal sealed class RequestInvocationHistory
    {
        public GrainId GrainId { get; private set; }
        public ActivationId ActivationId { get; private set; }
        public int InterfaceId { get; private set; }
        public int MethodId { get; private set; }

        internal RequestInvocationHistory(Message message)
        {
            GrainId = message.TargetGrain;
            ActivationId = message.TargetActivation;
            InterfaceId = message.InterfaceId;
            MethodId = message.MethodId;
        }

        public override string ToString()
        {
            return String.Format("RequestInvocationHistory {0}:{1}:{2}:{3}", GrainId, ActivationId, InterfaceId, MethodId);
        }
    }
}
