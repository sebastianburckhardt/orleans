using System;

using Orleans.Runtime.Scheduler;

namespace Orleans.Runtime
{
    internal abstract class SystemTarget : ISystemTarget, ISystemTargetBase, IInvokable
    {
        private IGrainMethodInvoker _lastInvoker;

        protected SystemTarget(GrainId grain, SiloAddress currentSilo)
        {
            Grain = grain;
            CurrentSilo = currentSilo;
            ActivationId = ActivationId.GetSystemActivation(grain, currentSilo);
            schedulingContext = new OrleansContext(this);
        }

        public SiloAddress CurrentSilo { get; private set; }

        public GrainId Grain { get; private set; }

        public ActivationId ActivationId { get; set; }

        private readonly OrleansContext schedulingContext;
        internal OrleansContext SchedulingContext { get { return schedulingContext; } }

        public IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null)
        {
            if (_lastInvoker != null && interfaceId == _lastInvoker.InterfaceId)
                return _lastInvoker;

            var invoker = GrainTypeManager.Instance.GetInvoker(interfaceId);
            _lastInvoker = invoker;
            
            return _lastInvoker;
        }

        public void HandleNewRequest(Message request)
        {
            InsideGrainClient.Current.Invoke(this, this, request).Ignore();
        }

        public void HandleResponse(Message response)
        {
            GrainClient.InternalCurrent.ReceiveResponse(response);
        }

        public override string ToString()
        {
            return String.Format("[SystemTarget: {0}{1}{2}]",
             CurrentSilo,
             Grain,
             ActivationId);
        }
    }
}
