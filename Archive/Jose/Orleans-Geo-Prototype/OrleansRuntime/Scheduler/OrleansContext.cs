using System;

using Orleans.Scheduler;
using Orleans.Runtime.Coordination;

namespace Orleans.Runtime.Scheduler
{
    internal class OrleansContext : ISchedulingContext
    {
        public ActivationData Activation { get; private set; }

        public SystemTarget SystemTarget { get; private set; }

        public int DispatcherTarget { get; private set; }

        public SchedulingContextType ContextType { get; private set; }

        public OrleansContext(ActivationData activation)
        {
            Activation = activation;
            ContextType = SchedulingContextType.Activation;
        }

        internal OrleansContext(SystemTarget systemTarget)
        {
            SystemTarget = systemTarget;
            ContextType = SchedulingContextType.SystemTarget;
        }

        internal OrleansContext(int dispatcherTarget)
        {
            DispatcherTarget = dispatcherTarget;
            ContextType = SchedulingContextType.SystemThread;
        }

        #region IEquatable<ISchedulingContext> Members

        public bool Equals(ISchedulingContext other)
        {
            return AreSame(other);
        }

        #endregion

        public override bool Equals(object obj)
        {
            return AreSame(obj);
        }

        private bool AreSame(object obj)
        {
            var other = obj as OrleansContext;
            if (ContextType == SchedulingContextType.Activation)
                return other != null && Activation.Equals(other.Activation);
            else if (ContextType == SchedulingContextType.SystemTarget)
                return other != null && SystemTarget.Equals(other.SystemTarget);
            else if (ContextType == SchedulingContextType.SystemThread)
                return other != null && DispatcherTarget.Equals(other.DispatcherTarget);
            else
                return false;
        }

        public override int GetHashCode()
        {
            if (ContextType == SchedulingContextType.Activation)
                return Activation.ActivationId.Key.GetHashCode();
            else if (ContextType == SchedulingContextType.SystemTarget)
                return SystemTarget.ActivationId.Key.GetHashCode();
            else if (ContextType == SchedulingContextType.SystemThread)
                return DispatcherTarget;
            else
                return 0;
        }

        public override string ToString()
        {
            if (ContextType == SchedulingContextType.Activation)
                return Activation.ToString();
            else if (ContextType == SchedulingContextType.SystemTarget)
                return SystemTarget.ToString();
            else if (ContextType == SchedulingContextType.SystemThread)
                return String.Format("DispatcherTarget{0}", DispatcherTarget);
            else
                return "";
        }

        public string Name 
        {
            get
            {
                if (ContextType == SchedulingContextType.Activation)
                    return Activation.Name;
                else if (ContextType == SchedulingContextType.SystemTarget)
                    //return SystemTarget.ToString();
                    return SystemTarget.Grain.ToString();
                else if (ContextType == SchedulingContextType.SystemThread)
                    return String.Format("DispatcherTarget{0}", DispatcherTarget);
                else
                    return "";
            } 
        }
    }
}
