using System;
using System.IO;
using System.Threading;
using ManagementFramework.Common;
using ManagementFramework.Events;
using Orleans.Management.Events;
using Orleans.Runtime.Counters;
using Orleans.RuntimeCore;

namespace Orleans.Management.Agents
{
    public class StatusAgent : OrleansManagementAgent
    {
        private const int OneSecond = 1000;

        private Timer timer;
        private int frequency;

        public StatusAgent()
            : base("StatusAgent")
        {
            AddSubscriptionType(typeof(GetStatusRequestEvent), this.ProcessGetStatusRequestEvent);
            AddPublishType(typeof(HealthEvent));
        }

        /// <summary>
        /// Processes a GetStatusRequestEvent.
        /// </summary>
        /// <param name="eventType">We're expecting a GetStatusRequestEvent.</param>
        /// <param name="ae">We're delivered a GetStatusRequestEvent but we'll have to cast it from the AbstractEvent.</param>
        private void ProcessGetStatusRequestEvent(Guid eventType, AbstractEvent ae)
        {
            GetStatusRequestEvent req = ae as GetStatusRequestEvent;

            if (req == null) return; // Ignore - not for us

            logger.Info("Received management event: EvtGuid={0} Event Contents={1}", eventType, ae.ToString());

            var response = CreateHeartbeatEvent(req);

            SendReply(response);
        }

        public override void OnStart()
        {
            base.OnStart();

            if (this.DeploymentGroupId != default(Guid))
            {
                this.frequency = OneSecond;
                TimeSpan freqPeriod = TimeSpan.FromMilliseconds(frequency);
                logger.Info("Starting status heartbeat with frequency={0}", freqPeriod);
                this.timer = new Timer(Heartbeat, null, Timeout.Infinite, Timeout.Infinite);
                this.timer.Change(this.frequency, Timeout.Infinite); // Start the timer running

            }
        }

        /// <summary>
        /// Stop  execution.
        /// </summary>
        public override void OnStop()
        {
            base.OnStop();

            DisposeTimer();
        }

        /// <summary>
        /// Periodic task - Send regular heartbeat event
        /// </summary>
        /// <param name="state"></param>
        private void Heartbeat(object state)
        {
            var heartbeat = CreateHeartbeatEvent(null);

            if (this.DeploymentGroupId != default(Guid))
            {
                SendMessage(heartbeat);
            }

            // Queue next timer callback
            this.timer.Change(this.frequency, Timeout.Infinite);
        }

        /// <summary>
        /// Create and intialiaze a new heartbeat / health event to send.
        /// </summary>
        /// <returns>Newly constructed heartbeat / health event</returns>
        private HealthEvent CreateHeartbeatEvent(GetStatusRequestEvent requestEvent)
        {
            var heartbeat = new HealthEvent();
            InitializeManagementEvent(heartbeat, requestEvent);
            heartbeat.Status = GetSystemState();
            heartbeat.StatusDetails = GetStatusDetails();
            heartbeat.Recipient = this.DeploymentGroupId;
            heartbeat.Timestamp = DateTime.Now;
            return heartbeat;
        }

        private LifecycleState GetSystemState()
        {
            SystemStatus s = SystemStatus.Current;

            if (s == SystemStatus.Starting) return LifecycleState.Prepared;
            else if (s == SystemStatus.Pausing || s == SystemStatus.Paused) return LifecycleState.Ready;
            else if (s == SystemStatus.Running) return LifecycleState.Started;
            else if (s == SystemStatus.Stopping) return LifecycleState.Concluded;
            else if (s == SystemStatus.Stopped) return LifecycleState.Stopped;
            else if (s == SystemStatus.Error) return LifecycleState.Unknown;
            else return LifecycleState.Unknown;
        }

        private string GetStatusDetails()
        {
            using (StringWriter s = new StringWriter())
            {
                s.Write("StatusDetails[ ");
                s.Write("SystemStatus="); s.WriteLine(SystemStatus.Current.ToString());
                OrleansCounterBase.DumpAllCounters(s);
                s.Write(" ]");
                return s.ToString();
            }
        }


        protected override void Dispose(bool disposing)
        {
            base.Dispose(disposing);

            if (disposing)
            {
                DisposeTimer();
            }
        }

        private void DisposeTimer()
        {

            if (this.timer != null)
            {
                this.timer.Change(0, Timeout.Infinite);
                this.timer.Dispose();
                this.timer = null;
            }
        }
    }
}
