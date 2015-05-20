using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Web;
using Microsoft.WindowsAzure.ServiceRuntime;

namespace Orleans.Host.Azure.Utils
{
    /// <summary>
    /// Various utility functions to make it easier to access and handle Azure configuration information.
    /// </summary>
    public static class AzureConfigUtils
    {
        /// <summary>
        /// Try to determine the base location for the Azure app directory we are being run from
        /// </summary>
        /// <returns>App directory this library is being run from</returns>
        /// <exception cref="InvalidOperationException">If unable to determine our app directory location</exception>
        public static DirectoryInfo AzureAppDirectory
        {
            get
            {
                // App directory locations:
                // Worker Role code:            {RoleRoot}\approot
                // WebRole – Role startup code: {RoleRoot}\approot\bin
                // WebRole - IIS web app code:  {ServerRoot}

                string appRoot;
                string roleRootDir = Environment.GetEnvironmentVariable("RoleRoot");
                if (roleRootDir == null)
                {
                    // RoleRoot not defined for IIS web apps running as Azure WebRoles - try using Server.MapPath to resolve
                    appRoot = HttpContext.Current.Server.MapPath(@"~\");
                }
                else
                {
                    // Being called from Role startup code - either Azure WorkerRole or WebRole
                    Assembly assy = Assembly.GetExecutingAssembly();
                    appRoot = Path.GetDirectoryName(assy.Location);
                }
                if (appRoot == null)
                {
                    throw new InvalidOperationException(
                        "Could not determine Azure approot location from either RoleRoot or HttpContext.Current.Server.MapPath");
                }
                var appDir = new DirectoryInfo(appRoot);
                if (!appDir.Exists)
                {
                    throw new FileNotFoundException(
                        "Cannot find Azure approot directory at " + appDir.FullName, 
                        appDir.FullName);
                }
                return appDir;
            }
        }

        ///<summary>
        /// Return the default file location for the Orleans client config file (ClientConfiguration.xml)
        ///</summary>
        ///<exception cref="FileNotFoundException">If client config file cannot be located</exception>
        public static FileInfo ClientConfigFileLocation
        {
            get
            {
                const string cfgFileName = "ClientConfiguration.xml";
                var loc1 = new FileInfo(Path.Combine(AzureAppDirectory.FullName, cfgFileName));
                if (!loc1.Exists)
                {
                    var loc2 = new FileInfo(Path.Combine(".", cfgFileName)); // try current directory
                    if (loc2.Exists) return loc2;
                    // Report error using first (expected) search location
                    throw new FileNotFoundException(
                        String.Format("Cannot find Orleans client config file at {0} or {1}", loc1.FullName, loc2.FullName), 
                        loc1.FullName);
                }
                return loc1;
            }
        }

        ///<summary>
        /// Return the default file location for the Orleans silo config file (OrleansConfiguration.xml)
        ///</summary>
        ///<exception cref="FileNotFoundException">If silo config file cannot be located</exception>
        public static FileInfo SiloConfigFileLocation
        {
            get
            {
                const string cfgFileName = "OrleansConfiguration.xml";
                var loc1 = new FileInfo(Path.Combine(AzureAppDirectory.FullName, cfgFileName));
                if (!loc1.Exists)
                {
                    var loc2 = new FileInfo(Path.Combine(".", cfgFileName)); // try current directory
                    if (loc2.Exists) return loc2;
                    // Report error using first (expected) search location
                    throw new FileNotFoundException(
                        String.Format("Cannot find Orleans silo config file at {0} or {1}", loc1.FullName, loc2.FullName),
                        loc1.FullName);
                }
                return loc1;
            }
        }

        /// <summary>
        /// Get the instance named for the specified Azure role instance
        /// </summary>
        /// <param name="deploymentId">Azure Deployment Id for this service</param>
        /// <param name="roleInstance">Azure role instance information</param>
        /// <returns>Instance name for this role</returns>
        public static string GetInstanceName(string deploymentId, RoleInstance roleInstance)
        {
            string instanceId = roleInstance.Id;

            if (instanceId.Length > deploymentId.Length)
            {
                return instanceId.Substring(deploymentId.Length + 1);
            }
            else
            {
                return instanceId;
            }
        }

        /// <summary>
        /// Get the index number for the current Azure role instance. 
        /// </summary>
        /// <returns>Index number for the current role instance [zero-based]</returns>
        public static int GetMyInstanceIndex()
        {
            const char charSep = '_';

            string instanceName = AzureConfigUtils.GetInstanceName(RoleEnvironment.DeploymentId, RoleEnvironment.CurrentRoleInstance);

            if (instanceName.Contains(charSep))
            {
                // Parse role instance name to extract real instance number
                string indexPart = instanceName.Split(new[] {charSep}).Last();
                return int.Parse(indexPart);
            }
            else
            {
                // Use update domain bucket as approximation - this is about the best we can do without any extra info available
                return RoleEnvironment.CurrentRoleInstance.UpdateDomain;
            }
        }

        /// <summary>
        /// Get the instance name for the current Azure role instance
        /// </summary>
        /// <returns>Instance name for the current role instance</returns>
        public static string GetMyInstanceName()
        {
            return AzureConfigUtils.GetInstanceName(RoleEnvironment.DeploymentId, RoleEnvironment.CurrentRoleInstance);
        }

        /// <summary>
        /// List instance details of the specified roles
        /// </summary>
        /// <param name="roles">Dictionary contining the roles to be listed, indexed by instance name</param>
        public static void ListAllRoleDetails(IDictionary<string, Role> roles)
        {
            if (roles == null) throw new ArgumentNullException("roles", "No roles dictionary provided");

            foreach (string name in roles.Keys)
            {
                Role r = roles[name];
                foreach (RoleInstance instance in r.Instances)
                {
                    ListRoleInstanceDetails(instance);
                }
            }
        }

        /// <summary>
        /// List details of the specified role instance
        /// </summary>
        /// <param name="instance">role instance to be listed</param>
        public static void ListRoleInstanceDetails(RoleInstance instance)
        {
            if (instance == null) throw new ArgumentNullException("instance", "No RoleInstance data provided");

            Trace.TraceInformation("Role={0} Instance: Id={1} FaultDomain={2} UpdateDomain={3}",
                instance.Role.Name, instance.Id, instance.FaultDomain, instance.UpdateDomain);

            ListRoleInstanceEndpoints(instance);
        }

        /// <summary>
        /// List endpoint details of the specified role instance
        /// </summary>
        /// <param name="instance">role instance to be listed</param>
        public static void ListRoleInstanceEndpoints(RoleInstance instance)
        {
            if (instance == null) throw new ArgumentNullException("instance", "No RoleInstance data provided");

            foreach (string endpointName in instance.InstanceEndpoints.Keys)
            {
                Trace.TraceInformation("Role={0} Instance={1} EndpointName={2}", 
                    instance.Role.Name, instance.Id, endpointName);

                ListEndpointDetails(instance.InstanceEndpoints[endpointName]);
            }
        }

        internal static void ListEndpointDetails(RoleInstanceEndpoint endpoint)
        {
            if (endpoint == null) throw new ArgumentNullException("endpoint", "No RoleInstanceEndpoint data provided");

            Trace.TraceInformation("Role={0} Instance={1} Address={2} Port={3}",
                endpoint.RoleInstance.Role.Name, endpoint.RoleInstance.Id, endpoint.IPEndpoint.Address, endpoint.IPEndpoint.Port);
        }

        /// <summary>
        /// Get the endpoint details of the specified role
        /// </summary>
        /// <param name="role">role to be inspected</param>
        /// <returns>The list of <c>RoleInstanceEndpoint</c> data associated with the specified Azure role.</returns>
        public static List<RoleInstanceEndpoint> GetRoleEndpoints(Role role)
        {
            if (role == null) throw new ArgumentNullException("role", "No Role data provided");

            List<RoleInstanceEndpoint> endpoints = new List<RoleInstanceEndpoint>();
            foreach (RoleInstance instance in role.Instances)
            {
                //ListRoleInstanceEndpoints(instance);
                endpoints.AddRange(instance.InstanceEndpoints.Values);
            }
            return endpoints;
        }

        /// <summary>
        /// Get the endpoint IP address details of the specified role
        /// </summary>
        /// <param name="roleName">Name of the role to be inspected</param>
        /// <param name="endpointName">Name of the endpoint to be inspected</param>
        /// <returns>The list of <c>IPEndPoint</c> data for the specified endpoint associated with the specified Azure role name.</returns>
        public static List<IPEndPoint> GetRoleInstanceEndpoints(string roleName, string endpointName)
        {
            List<IPEndPoint> endpoints = new List<IPEndPoint>();

            if (RoleEnvironment.Roles.ContainsKey(roleName))
            {
                foreach (RoleInstance inst in RoleEnvironment.Roles[roleName].Instances)
                {
                    if (inst.InstanceEndpoints.ContainsKey(endpointName))
                    {
                        RoleInstanceEndpoint instEndpoint = inst.InstanceEndpoints[endpointName];
                        if (instEndpoint != null)
                        {
                            endpoints.Add(instEndpoint.IPEndpoint);
                        }
                    }
                }
            }

            return endpoints;
        }

        //public static List<string> FindRoleInstances(string roleName)
        //{
        //    List<string> roleInstances = new List<string>();

        //    string deploymentId = RoleEnvironment.DeploymentId;

        //    // Create a CloudStorageAccount using the credentials for disgnostics storage
        //    CloudStorageAccount csa = CloudStorageAccount.FromConfigurationSetting(OrleansAzureConstants.DiagnosticsStorageAccountConfigurationSettingName);

        //    // Create a new DeploymentDiagnosticManager for a given deployment ID
        //    DeploymentDiagnosticManager ddm = new DeploymentDiagnosticManager(csa, deploymentId);

        //    // Get the role instance diagnostics manager for all instance of the a role
        //    IEnumerable<RoleInstanceDiagnosticManager> ridmList = ddm.GetRoleInstanceDiagnosticManagersForRole(roleName);

        //    foreach (RoleInstanceDiagnosticManager ridm in ridmList)
        //    {
        //        DiagnosticMonitorConfiguration dmc = ridm.GetCurrentConfiguration();

        //        string instanceName = GetInstanceName(deploymentId, ridm.RoleInstanceId);

        //        Trace.WriteLine(string.Format("Found: Deployment={0} Instance Name={1} ", deploymentId, instanceName));

        //        roleInstances.Add(instanceName);
        //    }

        //    return roleInstances;
        //}

        //public static void ConfigureRoleInstances(string deploymentId, string roleName)
        //{
        //    //Create a CloudStorageAccount using the credentials for development storage
        //    CloudStorageAccount csa = CloudStorageAccount.FromConfigurationSetting(OrleansAzureConstants.DiagnosticsStorageAccountConfigurationSettingName);

        //    //Create a new DeploymentDiagnosticManager for a given deployment ID
        //    DeploymentDiagnosticManager ddm = new DeploymentDiagnosticManager(csa, deploymentId);

        //    //Get the role instance diagnostics manager for all instance of the a role
        //    IEnumerable<RoleInstanceDiagnosticManager> ridmList = ddm.GetRoleInstanceDiagnosticManagersForRole(roleName);

        //    //Create a performance counter for processor time
        //    PerformanceCounterConfiguration pccCPU = new PerformanceCounterConfiguration();
        //    pccCPU.CounterSpecifier = @"\Processor(_Total)\% Processor Time";
        //    pccCPU.SampleRate = TimeSpan.FromSeconds(5);

        //    //Create a performance counter for available memory
        //    PerformanceCounterConfiguration pccMemory = new PerformanceCounterConfiguration();
        //    pccMemory.CounterSpecifier = @"\Memory\Available Mbytes";
        //    pccMemory.SampleRate = TimeSpan.FromSeconds(5);

        //    //Set the new diagnostic monitor configuration for each instance of the role 
        //    foreach (RoleInstanceDiagnosticManager ridm in ridmList)
        //    {
        //        DiagnosticMonitorConfiguration dmc = ridm.GetCurrentConfiguration();
        //        //Add the new performance counters to the configuration 
        //        dmc.PerformanceCounters.DataSources.Add(pccCPU);
        //        dmc.PerformanceCounters.DataSources.Add(pccMemory);

        //        //Update the configuration
        //        ridm.SetCurrentConfiguration(dmc);
        //    }

        //}
    }
}
