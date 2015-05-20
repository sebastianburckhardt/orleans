using System;

namespace Orleans
{
    internal enum SchedulingContextType
    {
        Activation,
        SystemTarget,
        SystemThread
    }

    internal interface ISchedulingContext : IEquatable<ISchedulingContext>
    {
        SchedulingContextType ContextType { get; }
        string Name { get; }
    }

    internal static class SchedulingUtils
    {
        // AddressableContext is the one that can send messages (Activation and SystemTarget)
        // null context and SystemThread and not addressable.
        internal static bool IsAddressableContext(ISchedulingContext context)
        {
            return context != null && context.ContextType != SchedulingContextType.SystemThread;
        }

        internal static bool IsSystemContext(ISchedulingContext context)
        {
            // System Context are either associated with the (null) context, system target or system thread.
            // Both System targets, system thread and normal grains have OrleansContext instances, of the appropriate type (based on SchedulingContext.ContextType).
            return context == null || context.ContextType != SchedulingContextType.Activation;
        }
    }
}
