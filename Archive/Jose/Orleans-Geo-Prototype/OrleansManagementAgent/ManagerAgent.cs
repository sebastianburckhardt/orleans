using System;
using ManagementFramework.Events;
using Orleans.Management.Events;

namespace Orleans.Management.Agents
{
    public class ManagerAgent : OrleansManagementAgent
    {
        public ManagerAgent()
            : base("ManagerAgent")
        {
            AddSubscriptionType(typeof(LifecycleCommandEvent), this.ProcessLifecycleCommandEvent);
        }

        /// <summary>
        /// Processes a LifecycleCommandEvent.
        /// </summary>
        /// <param name="eventType">We're expecting a LifecycleCommandEvent.</param>
        /// <param name="ae">We're delivered a LifecycleCommandEvent but we'll have to cast it from the AbstractEvent.</param>
        private void ProcessLifecycleCommandEvent(Guid eventType, AbstractEvent ae)
        {
            LifecycleCommandEvent req = ae as LifecycleCommandEvent;

            if (req == null) return; // Ignore - not for us

            logger.Info("Received management event: EvtGuid={0} Event Contents={1}", eventType, ae.ToString());

            // Processing of lifecycle commands
            if (req.Command == LifecycleActions.Stop) {
                // Shutdown
                Silo.Stop();
            }
            else if (req.Command == LifecycleActions.Start) {
                // Run
                Silo.Start();
            }
            else {
                SendReply(
                    CreateCommandAcknowledgement(
                        req, 
                        new InvalidOperationException("LifecycleActions=" + Enum.GetName(typeof(LifecycleActions), req.Command) + " not supported")));
            }

            SendReply(CreateCommandAcknowledgement(req, null));
        }
    }
}
