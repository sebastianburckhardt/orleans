using System;
using System.Collections.Generic;
using ManagementFramework.Communications.Common;
using ManagementFramework.Events;
using Orleans.Runtime;
using Orleans.RuntimeCore;
using Orleans.Management.Events;
using System.Net;

namespace Orleans.Management.Agents
{
    public abstract class OrleansManagementAgent : IDisposable
    {
        internal protected string Name { get; internal set; }

        internal protected Guid RuntimeInstanceId { get; internal set; }

        internal protected Guid DeploymentGroupId { get; internal set; }

        internal protected Silo Silo { get; internal set; }

        internal protected string SiloName { get; internal set; }

        internal protected string Host { get; internal set; }

        protected Logger logger { get; private set; }

        private EventChannelAdapter EventChannel { get; set; }

        private readonly Dictionary<Type, Action<Guid, AbstractEvent>> eventHandlers = new Dictionary<Type, Action<Guid, AbstractEvent>>();
        private readonly List<Type> publicationTypes = new List<Type>();

        /// <summary>
        /// Initializes a new instance of this agent class.
        /// </summary>
        /// <param name="name">The name of this agent.</param>
        public OrleansManagementAgent(string name)
        {
            this.logger = new Logger(name, Logger.LoggerType.Runtime);
            this.Name = name;
            this.Host = Dns.GetHostName();
        }

        internal void SetEventChannel(EventChannelAdapter eventChannel)
        {
            this.EventChannel = eventChannel;
        }



        #region ILifecycle Members

        /// <summary>
        /// Prepare for execution.
        /// </summary>
        public virtual void OnPrepare()
        {
            if (this.EventChannel == null) 
                throw new ArgumentNullException("EventChannel not set up for management agent " + this.Name);

            logger.Info(
                "{0} - name: {1}, eventChannelAdapter.Name: {2}, eventChannelAdapter.Type: {3}",
                this.GetType().Name,
                this.Name,
                this.EventChannel.Name,
                this.EventChannel.GetType().Name);

            // Register event types to be published
            foreach (Type eventType in publicationTypes)
            {
                EventChannel.PublicationTypes.Add(eventType);
            }

            // Register event subscriptions
            foreach (Type eventType in eventHandlers.Keys)
            {
                EventChannel.SubscriptionTypes.Add(eventType);
            }
        }

        /// <summary>
        /// Start execution.
        /// </summary>
        public virtual void OnStart()
        {
            logger.Info("{0}, OnStart() called.", Name);

            // Register event subscriptions
            foreach (Type eventType in eventHandlers.Keys) {
                EventChannel.Subscribe(eventType, eventHandlers[eventType]);
                EventChannel.Subscribe(eventType, eventHandlers[eventType], this.RuntimeInstanceId);
            }
        }

        /// <summary>
        /// Stop  execution.
        /// </summary>
        public virtual void OnStop()
        {
            logger.Info("{0}, OnStop() called.", Name);
            EventChannel.UnsubscribeAll();
        }

        /// <summary>
        /// Conclude, i.e., terminate execution.
        /// </summary>
        public virtual void OnConclude()
        {
            logger.Info("{0}, OnConclude() called.", Name);
        }

        #endregion

        protected void AddSubscriptionType(Type eventType, Action<Guid, AbstractEvent> handler)
        {
            eventHandlers[eventType] = handler;
        }

        protected void AddPublishType(Type eventType)
        {
            publicationTypes.Add(eventType);
        }

        /// <summary>
        /// Initialize an Orleans management event with some common data fields
        /// </summary>
        /// <param name="replyEvent"></param>
        protected void InitializeManagementEvent(OrleansManagementEvent replyEvent, OrleansManagementEvent requestEvent)
        {
            replyEvent.SiloName = this.SiloName;
            replyEvent.RuntimeInstanceId = this.RuntimeInstanceId;
            replyEvent.DeploymentGroupId = this.DeploymentGroupId;
            replyEvent.MachineName = this.Host;
        }

        /// <summary>
        /// Initialize an Orleans management event with some common data fields
        /// </summary>
        /// <param name="replyEvent"></param>
        protected CommandAcknowledgeEvent CreateCommandAcknowledgement(OrleansManagementEvent requestEvent, Exception error)
        {
            CommandAcknowledgeEvent ack = new CommandAcknowledgeEvent();
            InitializeManagementEvent(ack, requestEvent);
            ack.Recipient = this.DeploymentGroupId;
            ack.ProcessedEvent = requestEvent;
            ack.ProcessedOk = (error == null);
            if (error != null) ack.ErrorMessage = error.ToString();
            return ack;
        }

        protected void SendReply(OrleansManagementEvent reply)
        {
            try
            {
                reply.Recipient = this.DeploymentGroupId;

                if (logger.IsVerbose2) logger.Verbose2("Sending management event reply {0} to deployment group {1}", reply, this.DeploymentGroupId);

                this.EventChannel.Reply(reply);
            }
            catch (Exception exc)
            {
                logger.Error("Problem sending reply event back to manager. Ignoring exception={0}", exc);
            }
        }

        protected void SendMessage(OrleansManagementEvent msg)
        {
            try
            {
                if (msg.Recipient != default(Guid))
                {
                    if (logger.IsVerbose2) logger.Verbose2("Sending management event {0} to deployment group {1}", msg, msg.DeploymentGroupId);

                    this.EventChannel.Reply(msg);
                }
                else
                {
                    if (logger.IsVerbose2) logger.Verbose2("Publishing management event {0} to everybody", msg);

                    this.EventChannel.Publish(msg);
                }
            }
            catch (Exception exc)
            {
                logger.Error("Problem publishing management event to event channel. Ignoring exception={0}", exc);
            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
        }
    }
}
