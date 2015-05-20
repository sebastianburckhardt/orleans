using System;
using System.Collections.Generic;
using System.Net;
using System.Reflection;
using ManagementFramework.Communications.Common;
using ManagementFramework.Communications.EventBusAdapters.WspAdapter;
using Orleans.Management.Events;
using Orleans.Runtime;
using Orleans.RuntimeCore;

namespace Orleans.Management.Agents
{
    public class ManagementBusConnector 
    {
        public EventChannelAdapter EventChannel { get; private set; }

        public Guid ProcessInstanceId { get; private set; }
        public Guid DeploymentGroupId { get; private set; }
        public string SiloName { get; private set; }

        internal Silo Silo { get; private set; }

        private readonly Dictionary<string, OrleansManagementAgent> agents = new Dictionary<string, OrleansManagementAgent>();

        public const string LoggerAgentName = "LoggerAgent";
        public const string ManagerAgentName = "ManagerAgent";
        public const string StatusAgentName = "StatusAgent";
        public const string TracerAgentName = "TracerAgent";

        private static string[] agentNames = { 
            LoggerAgentName,
            ManagerAgentName,
            StatusAgentName,
            TracerAgentName
        };

        private Logger logger;

        public ManagementBusConnector(Guid deploymentGroup)
        {
            this.ProcessInstanceId = Guid.NewGuid();
            this.DeploymentGroupId = deploymentGroup;
            this.logger = Logger.GetLogger("ManagementBusConnector", Logger.LoggerType.Runtime);
        }

        public void Init(string siloName, Silo silo)
        {
            this.SiloName = siloName;
            this.Silo = silo;

            if (this.DeploymentGroupId == default(Guid))
            {
                logger.Info("Silo {0} is not running as part of a deployment group", siloName);
                return;
            }

            //bool ok = CheckEventChannelEnvSetup();

            //if (!ok) {
            //    throw new NotSupportedException("Cannot find event channel runtime environment");
            //}

            // Connect to event channel
            this.EventChannel = WspAdapter.Default;

            foreach (string agentName in agentNames)
            {
                this.AddAgent(agentName);
            }
        }

        public void Run()
        {
            if (this.DeploymentGroupId == default(Guid)) return;

            this.EventChannel.OnStart();

            foreach (string agentName in agents.Keys)
            {
                StartAgent(agentName, this.SiloName, this.Silo);
            }

            // Phone home and say Hello if running in a deployment group
            AnnounceToDeploymentGroup(ManagementFramework.Common.LifecycleState.Prepared);
        }

        public void Shutdown()
        {
            if (this.DeploymentGroupId == default(Guid)) return;

            foreach (string agentName in agents.Keys)
            {
                StopAgent(agentName);
            }

            // Phone home and say Goodbye if running in a deployment group
            AnnounceToDeploymentGroup(ManagementFramework.Common.LifecycleState.Stopped);

            this.EventChannel.OnStop();
            this.EventChannel = null;
        }

        private void AddAgent(string agentName)
        {
            logger.Info("Adding agent: {0}", agentName);
            OrleansManagementAgent agent;
            switch (agentName)
            {
                case LoggerAgentName: agent = new LoggerAgent(); break;
                case ManagerAgentName: agent = new ManagerAgent(); break;
                case StatusAgentName: agent = new StatusAgent(); break;
                case TracerAgentName: agent = new TracerAgent(); break;
                default: throw new ArgumentException("Unknown management agent: " + agentName);
            }
            
            agents.Add(agentName, agent);
        }

        private void StartAgent(string agentName, string siloName, Silo silo)
        {
            var agent = agents[agentName];

            //logger.Info("Preparing managemment agent: {0}", agentName);

            agent.Silo = silo;
            agent.SiloName = siloName;
            agent.RuntimeInstanceId = this.ProcessInstanceId;
            agent.DeploymentGroupId = this.DeploymentGroupId;
            agent.SetEventChannel(this.EventChannel);

            agent.OnPrepare();

            logger.Info("Starting managemment agent: {0}", agentName);

            agent.OnStart();
        }

        private void StopAgent(string agentName)
        {
            var agent = agents[agentName];

            logger.Info("Stopping managemment agent: {0}", agentName);

            agent.OnStop();
        }

        public OrleansManagementAgent GetAgent(string agentName)
        {
            return agents[agentName];
        }

        public static bool CheckEventChannelEnvSetup()
        {
            bool found = false;
            
            string dll = Environment.Is64BitProcess ? "SharedMemoryMgrx64" : "SharedMemoryMgrx86";

            try {
                AssemblyName assyName = new AssemblyName(dll);
                Assembly assy = Assembly.Load(assyName);
                if (assy != null) {
                    found = true;
                }
            }
            catch (Exception ex) {
                Logger log = Logger.GetLogger("ManagementBusConnector", Logger.LoggerType.Runtime);
                log.Verbose( "Unable to load event channel DLL {0} - Ignoring error: {1}", dll, ex.Message);
            }
            return found;
        }

        /// <summary>
        /// "E.T. phone home!" 
        /// If this host instance is a part of a deployment group, then send a message back to the launcher / manager
        /// </summary>
        /// <param name="deploymentGroupId">Deployment group id we are running in</param>
        /// <param name="myInstanceId">Unique id for this process instance</param>
        /// <param name="siloName">Name of this silo</param>
        private void AnnounceToDeploymentGroup(ManagementFramework.Common.LifecycleState state)
        {
            Guid myInstanceId = this.ProcessInstanceId;
            Guid myDeploymentGroup = this.DeploymentGroupId;
            string name = this.SiloName;

            logger.Info("Dialing home to deployment group={0} with silo={1}, instance={2}, state={3}", myDeploymentGroup, name, myInstanceId, state);

            var dialHomeEvent = new HealthEvent();
            dialHomeEvent.Id = myInstanceId;
            dialHomeEvent.RuntimeInstanceId = myInstanceId;
            dialHomeEvent.Recipient = myDeploymentGroup;
            dialHomeEvent.DeploymentGroupId = myDeploymentGroup;
            dialHomeEvent.SiloName = this.SiloName; 
            dialHomeEvent.MachineName = Dns.GetHostName();
            dialHomeEvent.Status = state;

            this.EventChannel.Reply(dialHomeEvent);
        }

    }
}
