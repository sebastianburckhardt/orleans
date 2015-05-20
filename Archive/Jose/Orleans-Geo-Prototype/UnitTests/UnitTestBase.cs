using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Management;
using System.Management.Automation;
using System.Management.Automation.Runspaces;
using System.Net;
using System.Reflection;
using System.Runtime.Remoting;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.AzureUtils;
using Orleans.Runtime;
using Orleans.Runtime.MembershipService;
using Orleans.RuntimeCore.Configuration;
using Orleans.Serialization;

namespace UnitTests
{
    [Serializable]
    public class SiloHandle
    {
        public Silo Silo { get; set; }
        public AppDomain AppDomain { get; set; }
        public Options Options { get; set; }
        public bool IsInProcess { get; set; }
        public string Name { get; set; }
        public Process Process { get; set; }
        public string MachineName { get; set; }
        private IPEndPoint endpoint;
        public IPEndPoint Endpoint
        {
            get
            {
                // watch it! In OutOfProcess case the IPEndPoint may not be correct, 
                // as the port is sometimes allocated inside the silo, so this endpoint variable will have a zero port.
                return endpoint;
            }

            set
            {
                endpoint = value;
            }
        }
        public override string ToString()
        {
            return String.Format("SiloHandle:{0}", Endpoint);
        }
    }
    [Serializable]
    public class UnitTestBase
    {
        protected static AppDomain SharedMemoryDomain;
        internal static SiloHandle Primary = null;
        protected static SiloHandle Secondary = null;
        private static readonly List<SiloHandle> additionalSilos = new List<SiloHandle>();
        protected static bool cleanedFileStore = false;
        protected bool startFresh;
        public Logger logger;

        private Options SiloOptions;
        private ClientOptions ClientOptions;
        protected static GlobalConfiguration Globals = null;
        private static ClientConfiguration clientConfig = null;
        protected static string DeploymentId = null;
        public static string DeploymentIdPrefix = null;

        public const int BasePort = 11111;
        private static int InstanceCounter = 0;
        private static readonly Random rand = new Random();

        public UnitTestBase()
            : this(new Options())
        {
        }

        public UnitTestBase(bool startFreshOrleans)
            : this(new Options { StartFreshOrleans = startFreshOrleans })
        {
        }

        public UnitTestBase(Options siloOptions)
            : this(siloOptions, null)
        {
        }

        public UnitTestBase(Options siloOptions, ClientOptions clientOptions)
        {
            // Only show time in test logs, not date+time.
            Logger.ShowDate = false;

            this.SiloOptions = siloOptions;
            this.ClientOptions = clientOptions;

            logger = Logger.GetLogger("UnitTestBase-" + this.GetType().Name, Logger.LoggerType.Application);

            AppDomain.CurrentDomain.UnhandledException += ReportUnobservedException;
            InitializeRuntime(this.GetType().FullName);
        }

        private void InitializeRuntime(string testName)
        {

            try
            {
                Initialize(SiloOptions, ClientOptions);
                string startMsg = "----------------------------- STARTING NEW UNIT TEST : " + testName + " -------------------------------------";
                logger.Info(0, startMsg);
                Console.WriteLine(startMsg);
            }
            catch (TimeoutException te)
            {
                throw new TimeoutException("Timeout during test initialization", te);
            }
            catch (Exception ex)
            {
                Exception baseExc = ex.GetBaseException();
                if (baseExc is TimeoutException)
                {
                    throw new TimeoutException("Timeout during test initialization", ex);
                }
                throw new AggregateException(
                    string.Format("Exception during test initialization: {0}",
                        Logger.PrintException(ex)), ex);
            }
        }

        private static void ReportUnobservedException(object sender, UnhandledExceptionEventArgs eventArgs)
        {
            Exception exception = (Exception) eventArgs.ExceptionObject;
            Console.WriteLine("Unobserved exception: {0}", exception);
            Assert.Fail("Unobserved exception: {0}", exception);
        }

        protected void WaitForLivenessToStabilize(bool softKill = true)
        {
            TimeSpan stabilizationTime = TimeSpan.Zero;
            if(!softKill)
            {
                // in case  of hard kill (kill and not Stop), we should give silos time to detect failures first.
                stabilizationTime = Globals.ProbeTimeout.Multiply(Globals.NumMissedProbesLimit);
            }
            if (Globals.UseLivenessGossip)
            {
                stabilizationTime += TimeSpan.FromSeconds(5);
            }
            else
            {
                stabilizationTime += Globals.TableRefreshTimeout.Multiply(2);
            }
            logger.Info("\n\nWaitForLivenessToStabilize is about to sleep for {0}", stabilizationTime);
            Thread.Sleep(stabilizationTime);
            logger.Info("WaitForLivenessToStabilize is done sleeping");
        }

        public static void CheckForUnobservedPromises()
        {
            var unobservedPromises = AsyncCompletion.GetUnobservedPromises();

            if (unobservedPromises.Count > 0)
            {
                var sb = new StringBuilder();
                sb.Append("Unobserved promises have been left behind:").AppendLine();
                foreach (var unobservedPromise in unobservedPromises)
                {
                    sb.Append(unobservedPromise).AppendLine().Append("==============================").AppendLine();
                }
                Assert.Fail(sb.ToString());
            }
        }

        public static void Initialize(Options options, ClientOptions clientOptions = null)
        {
            bool doStartPrimary = false;
            bool doStartSecondary = false;

            AsyncCompletion.TrackObservations = true;
            CheckForUnobservedPromises();

            if (!cleanedFileStore)
            {
                cleanedFileStore = true;
                EmptyFileStore();
                EmptyMembershipTable(); // first time
            }
            if (options.StartFreshOrleans)
            {
                // the previous test was !startFresh, so we need to cleanup after it.
                if (Primary != null || Secondary != null || GrainClient.Current != null)
                {
                    ResetDefaultRuntimes();
                }

                ResetAllAdditionalRuntimes();

                if (options.StartPrimary)
                {
                    doStartPrimary = true;
                }
                if (options.StartSecondary)
                {
                    doStartSecondary = true;
                }
            }
            else
            {
                if (options.StartPrimary && Primary == null)
                {
                    // first time.
                    doStartPrimary = true;
                }
                if (options.StartSecondary && Secondary == null)
                {
                    doStartSecondary = true;
                }
            }
            if (options.PickNewDeploymentId && String.IsNullOrEmpty(DeploymentId))
            {
                string prefix = DeploymentIdPrefix != null ? DeploymentIdPrefix : "depid-";
                //DeploymentId = prefix + Guid.NewGuid().ToString();
                DateTime now = DateTime.UtcNow;
                string DateTimeFormat = "yyyy-MM-dd-hh-mm-ss-fff";
                int randomSuffix = (new Random()).Next(1000);
                DeploymentId = prefix + now.ToString(DateTimeFormat, CultureInfo.InvariantCulture) + "-" + randomSuffix;
            }

            if (doStartPrimary)
            {
                Primary = StartOrleansRuntime(Silo.SiloType.Primary, options);
            }
            if (doStartSecondary)
            {
                Secondary = StartOrleansRuntime(Silo.SiloType.Secondary, options);
            }

            if (GrainClient.Current == null && options.StartClient)
            {
                if (clientOptions != null && clientOptions.ClientConfigFile != null)
                {
                    clientConfig = ClientConfiguration.LoadFromFile(clientOptions.ClientConfigFile.FullName);
                }
                else
                {
                    clientConfig = ClientConfiguration.StandardLoad();
                }

                if (clientOptions != null)
                {
                    clientConfig.ResponseTimeout = clientOptions.ResponseTimeout;
                    if (clientOptions.ProxiedGateway && clientOptions.Gateways != null)
                    {
                        clientConfig.Gateways = clientOptions.Gateways;
                        if (clientOptions.PreferedGatewayIndex >= 0)
                            clientConfig.PreferedGatewayIndex = clientOptions.PreferedGatewayIndex;
                    }
                    clientConfig.PropagateActivityId = clientOptions.PropagateActivityId;
                    if (!String.IsNullOrEmpty(DeploymentId))
                        clientConfig.DeploymentId = DeploymentId;
                }
                if (System.Diagnostics.Debugger.IsAttached)
                {
                    // Test is running inside debugger - Make timeout ~= infinite
                    clientConfig.ResponseTimeout = TimeSpan.FromMilliseconds(1000000);
                }
                if (options.LargeMessageWarningThreshold > 0)
                {
                    clientConfig.LargeMessageWarningThreshold = options.LargeMessageWarningThreshold;

                }
                clientConfig.AdjustConfiguration();
                UnobservedExceptionsHandlerClass.ResetUnobservedExceptionHandler();
                if (!OrleansClient.IsInitialized)
                {
                    OrleansClient.Initialize(clientConfig);
                }
            }
        }

        //public static bool ClientUnobservedPromiseHandler(Exception ex)
        //{
        //    var logger = new Logger("UnitTestBase", Logger.LoggerType.Application);
        //    logger.Error("Unobserved promise was broken with exception: ", ex);
        //    Assert.Fail("Unobserved promise was broken with exception: " + ex.Message + " at " + ex.StackTrace);
        //    return true;
        //}

        protected static void StartSecondGrainClient()
        {
            var grainClient = new OutsideGrainClient(cfg: clientConfig, secondary: true);
            GrainClient.Current = grainClient;
            grainClient.StartInternal();
        }

        static SiloHandle StartOrleansRuntime(Silo.SiloType type, Options options, AppDomain shared = null)
        {
            SiloHandle retValue = new SiloHandle();
            StartOrleansRuntime(retValue,type,options,shared);
            return retValue;
        }
        static SiloHandle StartOrleansRuntime(SiloHandle retValue, Silo.SiloType type, Options options, AppDomain shared = null)
        {
            retValue.Options = options;
            retValue.IsInProcess = !options.StartOutOfProcess;
            OrleansConfiguration config = new OrleansConfiguration();
            if (options.SiloConfigFile == null)
            {
                config.StandardLoad();
            }
            else
            {
                config.LoadFromFile(options.SiloConfigFile.FullName);
            }
            // GK: why would anyone ever want to uncomment this line? It hard-overwrittes the config setting, so we can't use any other trace level aside INFO.
            // IMPORTNAT: Do NOT uncomment this line! I keep deleting it and it keeps re-appearing.
            //config.Defaults.DefaultTraceLevel = OrleansLogger.Severity.Info;
            if (config.Globals.SeedNodes.Count > 0 && options.BasePort < 0)
            {
                config.PrimaryNode = config.Globals.SeedNodes[0];
            }
            else
            {
                config.PrimaryNode = new IPEndPoint(IPAddress.Loopback,
                                                    options.BasePort >= 0 ? options.BasePort : BasePort);
            }
            config.Globals.SeedNodes.Clear();
            config.Globals.SeedNodes.Add(config.PrimaryNode);

            if (!String.IsNullOrEmpty(DeploymentId))
                config.Globals.DeploymentId = DeploymentId;
            config.Defaults.PropagateActivityId = options.PropagateActivityId;
            if (options.LargeMessageWarningThreshold > 0) config.Defaults.LargeMessageWarningThreshold = options.LargeMessageWarningThreshold;

            if (options.DirectoryReplicationFactor > 0) config.Globals.DirectoryReplicationFactor = options.DirectoryReplicationFactor;
            if (options.LivenessType != GlobalConfiguration.LivenessProviderType.NotSpecified)
            {
                config.Globals.LivenessType = options.LivenessType;
            }
            if (!String.IsNullOrEmpty(options.AzureDataConnectionString))
            {
                config.Globals.DataConnectionString = options.AzureDataConnectionString;
            }
            if (options.ReminderServiceType != GlobalConfiguration.ReminderServiceProviderType.NotSpecified)
            {
                config.Globals.ReminderServiceType = options.ReminderServiceType;
            }

            Globals = config.Globals;
            config.IsRunningAsUnitTest = true;

            string domainName = options.DomainName;
            if (domainName == null)
            {
                switch (type)
                {
                    case Silo.SiloType.Primary:
                        domainName = "Primary";
                        break;
                    default:
                        domainName = "Secondary_" + InstanceCounter.ToString(CultureInfo.InvariantCulture);
                        break;
                }
            }

            NodeConfiguration nodeConfig = null;
            if (options.OverrideConfig)
            {
                nodeConfig = config.GetConfigurationForNode(domainName);
                nodeConfig.HostNameOrIPAddress = "loopback";
                int port = options.BasePort < 0 ? BasePort : options.BasePort;
                nodeConfig.Port = port + InstanceCounter;
                nodeConfig.DefaultTraceLevel = config.Defaults.DefaultTraceLevel;
                nodeConfig.PropagateActivityId = config.Defaults.PropagateActivityId;
                nodeConfig.BulkMessageLimit = config.Defaults.BulkMessageLimit;
                nodeConfig.MaxActiveThreads = options.MaxActiveThreads;
                if (options.SiloGenerationNumber > 0)
                {
                    nodeConfig.Generation = options.SiloGenerationNumber;
                }
                config.Globals.MaxForwardCount = options.MaxForwardCount;
                if (options.performDeadlockDetection != BooleanEnum.None) // use only if was explicitly specified.
                    config.Globals.PerformDeadlockDetection = options.PerformDeadlockDetection;
                SerializationManager.Initialize(config.Globals.UseStandardSerializer);

                if (config.Globals.ExpectedClusterSize_CV.IsDefaultValue) // overwrite only if was not explicitly set.
                    config.Globals.ExpectedClusterSize = 2;

                config.Globals.CollectionQuantum = options.CollectionQuantum;
                config.Globals.Application.SetDefaultCollectionAgeLimit(options.DefaultCollectionAgeLimit);

                InstanceCounter++;

                config.Overrides[domainName] = nodeConfig;
                config.AdjustConfiguration();
            }

            if (options.StartOutOfProcess)
            {
                //save the config so that you can pass it can be passed to the OrleansHost process.
                //launch the process
                if (nodeConfig != null)
                {
                    retValue.Endpoint = nodeConfig.Endpoint;
                }
                else
                {
                    retValue.Endpoint = config.Overrides[domainName].Endpoint;
                }
                string fileName = Path.Combine(Path.GetDirectoryName(typeof(UnitTestBase).Assembly.Location), "UnitTestConfig" + DateTime.UtcNow.Ticks);
                WriteConfigFile(fileName, config);
                retValue.MachineName = options.MachineName;
                string processName = "OrleansHost.exe";
                string imagePath = Path.Combine(Path.GetDirectoryName(typeof(UnitTestBase).Assembly.Location), processName);
                retValue.Process = StartProcess(retValue.MachineName, imagePath, domainName, fileName);
            }
            else
            {
                //var logger = new Logger("UnitTestBase");
                //logger.Info("Starting a new silo in app domain {0} with config {1}", domainName, config.ToString(domainName));
                var setup = new AppDomainSetup { ApplicationBase = Environment.CurrentDirectory };
                AppDomain outDomain = AppDomain.CreateDomain(domainName, null, setup);
                var args = new object[] { domainName, type, config };
                var result = (Silo)outDomain.CreateInstanceFromAndUnwrap(
                    "OrleansRuntime.dll", typeof(Silo).FullName, false,
                    BindingFlags.Default, null, args, CultureInfo.CurrentCulture,
                    new object[] { });


                result.Start();
                retValue.Silo = result;
                retValue.Endpoint = result.SiloAddress.Endpoint;
                retValue.AppDomain = outDomain;
                retValue.AppDomain.UnhandledException += ReportUnobservedException;

            }
            retValue.Name = domainName;
            return retValue;
        }

        protected SiloHandle StartAdditionalOrleans(Options siloOptions, Silo.SiloType type)
        {
            SiloHandle instance = StartOrleansRuntime(type, siloOptions);
            additionalSilos.Add(instance);
            return instance;
        }

        
        protected SiloHandle StartAdditionalOrleans()
        {
            SiloHandle instance = StartOrleansRuntime(
                Silo.SiloType.Secondary,
                this.SiloOptions);
            additionalSilos.Add(instance);
            return instance;
        }

        protected IEnumerable<SiloHandle> GetActiveSilos()
        {
            logger.Info("GetActiveSilos: Primary={0} Secondary={1} + {2} Additional={3}",
                Primary, Secondary, additionalSilos.Count, Utils.IEnumerableToString(additionalSilos));

            if (null != Primary && Primary.Silo != null) yield return Primary;
            if (null != Secondary && Secondary.Silo != null) yield return Secondary;
            if (additionalSilos.Count > 0)
                foreach (var s in additionalSilos)
                    if (null != s && s.Silo != null)
                        yield return s;
        }

        protected SiloHandle GetSiloForAddress(SiloAddress siloAddress)
        {
            var ret = GetActiveSilos().Where(s => s.Silo.SiloAddress.Equals(siloAddress)).FirstOrDefault();
            return ret;
        }

        protected List<SiloHandle> StartAdditionalOrleansRuntimes(int nRuntimes)
        {
            List<SiloHandle> instances = new List<SiloHandle>();
            for (int i = 0; i < nRuntimes; i++)
            {
                SiloHandle instance = StartAdditionalOrleans();
                instances.Add(instance);
            }
            return instances;
        }

        public static void ResetAllAdditionalRuntimes()
        {
            foreach (SiloHandle instance in additionalSilos)
            {
                ResetRuntime(instance);
            }
            additionalSilos.Clear();
        }

        public static void ResetDefaultRuntimes()
        {
            try
            {
                OrleansClient.Uninitialize();
            }
            catch (Exception exc) { Console.WriteLine(exc); }

            OrleansTask.Reset();

            ResetRuntime(Secondary);
            ResetRuntime(Primary);
            Secondary = null;
            Primary = null;
            InstanceCounter = 0;
            DeploymentId = null;
            EmptyMembershipTable();
        }

        private static void DoStopSilo(SiloHandle instance, bool kill)
        {
            if (instance.IsInProcess)
            {
                if (!kill)
                {
                    try { if (instance.Silo != null) instance.Silo.Stop(); }
                    catch (RemotingException re) { Console.WriteLine(re); /* Ignore error */ }
                    catch (Exception exc) { Console.WriteLine(exc); throw; }
                }

                try
                {
                    if (instance.AppDomain != null)
                    {
                        instance.AppDomain.UnhandledException -= ReportUnobservedException;
                        AppDomain.Unload(instance.AppDomain);
                    }
                }
                catch (Exception exc) { Console.WriteLine(exc); throw; }
            }
            else
            {
                try { if (instance.Process != null) instance.Process.Kill(); }
                catch (Exception exc) { Console.WriteLine(exc); throw; }
            }
            instance.AppDomain = null;
            instance.Silo = null;
            instance.Process = null;
        }

        public static void StopRuntime(SiloHandle instance)
        {
            if (instance != null)
            {
                if (!instance.IsInProcess)
                {
                    throw new NotSupportedException(
                        "Cannot stop silo when running out of processes, can only kill silo.");
                }
                else
                {
                    DoStopSilo(instance, false);
                }
            }
        }

        public static void KillRuntime(SiloHandle instance)
        {
            if (instance != null)
            {
                // do NOT stop, just kill directly, to simulate crash.
                DoStopSilo(instance, true);
            }
        }

        public static void ResetRuntime(SiloHandle instance)
        {
            if (instance != null)
            {
                DoStopSilo(instance, false);
            }
        }

        public static SiloHandle RestartRuntime(SiloHandle instance, bool kill = false)
        {
            if (instance != null)
            {
                var options = instance.Options;
                var type = instance.Silo.Type;
                DoStopSilo(instance, kill);
                StartOrleansRuntime(instance, type, options);
                return instance;
            }
            return null;
        }

        protected void RestartDefaultSilosButKeepCurrentClient(string msg)
        {
            logger.Info("Restarting all silos - Old Primary={0} Secondary={1} Others={2}",
                Primary.Silo.SiloAddress,
                Secondary.Silo.SiloAddress,
                Utils.IEnumerableToString(additionalSilos, s => s.Silo.SiloAddress.ToString()));

            ResetAllAdditionalRuntimes();
            //ResetDefaultRuntimes();

            ResetRuntime(Secondary);
            ResetRuntime(Primary);
            Secondary = null;
            Primary = null;
            InstanceCounter = 0;
            DeploymentId = null;
            EmptyMembershipTable();

            InitializeRuntime(msg);

            logger.Info("After restarting silos - New Primary={0} Secondary={1} Others={2}",
                Primary.Silo.SiloAddress,
                Secondary.Silo.SiloAddress,
                Utils.IEnumerableToString(additionalSilos, s => s.Silo.SiloAddress.ToString()));
        }

        protected void LogMetric(string metricName, object metricValue, string categories = null)
        {
            try
            {
                XmlDocument xmlDoc = new XmlDocument();
                if (File.Exists("performanceLog.xml"))
                {
                    xmlDoc.Load("performanceLog.xml");
                }
                else
                {
                    XmlElement root = xmlDoc.CreateElement("PerformanceLog");
                    xmlDoc.AppendChild(root);
                    XmlAttribute attrib = xmlDoc.CreateAttribute("DateTime");
                    attrib.Value = DateTime.UtcNow.ToString(CultureInfo.InvariantCulture);
                    root.Attributes.Append(attrib);
                    string machineName = System.Environment.MachineName;
                    attrib = xmlDoc.CreateAttribute("MachineName");
                    attrib.Value = machineName;
                    root.Attributes.Append(attrib);
                    attrib = xmlDoc.CreateAttribute("BuildId");
                    attrib.Value = "BUILD_ID";
                    root.Attributes.Append(attrib);
                }

                WriteRecord(xmlDoc, metricName, metricValue, categories);
                xmlDoc.Save("performanceLog.xml");
            }
            catch (Exception ex)
            {
                logger.Error(0, "Unable to log metric", ex);
            }
        }

        private void WriteRecord(XmlDocument doc, string metricName, object metricValue, string categories)
        {
            StackTrace stackTrace = new StackTrace();
            StackFrame stackFrame = stackTrace.GetFrame(2);
            MethodBase methodBase = stackFrame.GetMethod();
            string methodName = methodBase.Name;
            Type methodType = methodBase.ReflectedType;

            XmlNode root = doc.SelectSingleNode("/PerformanceLog");
            XmlElement newElement = doc.CreateElement("LogEntry");
            root.AppendChild(newElement);
            XmlAttribute attrib = doc.CreateAttribute("ClassName");
            attrib.Value = methodType.Name;
            newElement.Attributes.Append(attrib);
            attrib = doc.CreateAttribute("MethodName");
            attrib.Value = methodName;
            newElement.Attributes.Append(attrib);
            attrib = doc.CreateAttribute("MetricName");
            attrib.Value = metricName;
            newElement.Attributes.Append(attrib);
            attrib = doc.CreateAttribute("MetricValue");
            attrib.Value = metricValue.ToString();
            newElement.Attributes.Append(attrib);

            if (categories != null)
            {
                foreach (string category in categories.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                {
                    XmlElement element = doc.CreateElement("Category");
                    element.InnerText = category.Trim();
                    newElement.AppendChild(element);
                }
            }
        }

        protected SiloAddress[] GetRuntimesIds(List<Silo> instances)
        {
            SiloAddress[] ids = new SiloAddress[instances.Count];
            for (int i = 0; i < instances.Count; i++)
            {
                ids[i] = instances[i].SiloAddress;
            }
            return ids;
        }

        protected static void EmptyFileStore()
        {
            //ServerConfigManager configManager = ServerConfigManager.LoadConfigManager();
            // todo: clear StoreManager
        }

        protected static void EmptyMembershipTable()
        {
            try
            {
                OrleansConfiguration config = new OrleansConfiguration();
                config.StandardLoad();
                //if (config.Globals.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.File))
                //{
                //    FileBasedMembershipTable.DeleteMembershipTableFile(config.Globals.LivenessFileDirectory);
                //}
                //else 
                if (config.Globals.LivenessType.Equals(GlobalConfiguration.LivenessProviderType.AzureTable))
                {
                    AzureBasedMembershipTable table = AzureBasedMembershipTable.GetAzureBasedMembershipTable(config.Globals.DeploymentId, config.Globals.DataConnectionString).WithTimeout(AzureTableDefaultPolicies.TableOperation_TIMEOUT).Result;
                    table.DeleteAzureMembershipTableEntries(config.Globals.DeploymentId).Wait(AzureTableDefaultPolicies.TableOperation_TIMEOUT);
                }
            }
            catch (Exception) { }
        }

#if !DISABLE_STREAMS
        internal void DeleteAllAzureQueues(IEnumerable<Orleans.Streams.QueueId> allQueues, string deploymentId, string storageConnectionString)
        {
            DeleteAllAzureQueues(allQueues, deploymentId, storageConnectionString, this.logger);
        }

        internal static void DeleteAllAzureQueues(IEnumerable<Orleans.Streams.QueueId> allQueues, string deploymentId, string storageConnectionString, Logger log)
        {
            if (deploymentId != null)
            {
                if (log != null) log.Info("About to delete all {0} Stream Queues\n", allQueues.Count());
                foreach (var queueId in allQueues)
                {
                    AzureQueueDataManager manager = new AzureQueueDataManager(queueId.ToString(), deploymentId, storageConnectionString);
                    manager.DeleteQueue().Wait();
                }
            }
        }
#endif

        public static void RemoveFromRemoteMachine(string machineName, string path)
        {
            Directory.Delete(@"\\" + machineName + @"\" + path.Replace(":", "$"), true);
        }

        public static void CopyToRemoteMachine(string machineName, string from, string to, string[] exclude = null)
        {
            ProcessStartInfo startInfo = new ProcessStartInfo();
            startInfo.FileName = @"C:\Windows\System32\xcopy.exe";
            startInfo.CreateNoWindow = true;
            startInfo.UseShellExecute = false;
            startInfo.WorkingDirectory = Path.GetDirectoryName(typeof(UnitTestBase).Assembly.Location);
            StringBuilder args = new StringBuilder();
            args.AppendFormat(" {0} {1}", from, @"\\" + machineName + @"\" + to.Replace(":", "$"));
            if (null != exclude && exclude.Length > 0)
            {
                args.AppendFormat(" /EXCLUDE:{0}", string.Join("+", exclude));
            }
            args.AppendFormat(" /I /V /E /Y /Z");
            startInfo.Arguments = args.ToString();
            Process xcopy = Process.Start(startInfo);
            xcopy.WaitForExit();
        }

        public static Process StartProcess(string machineName, string processPath, params string[] parameters)
        {
            StringBuilder args = new StringBuilder();
            foreach (string s in parameters)
            {
                args.AppendFormat(" \"{0}\" ", s);
            }
            if (machineName == "." || machineName == "localhost")
            {
                ProcessStartInfo startInfo = new ProcessStartInfo();
                startInfo.FileName = processPath;
                startInfo.CreateNoWindow = true;
                startInfo.WorkingDirectory = Path.GetDirectoryName(processPath);
                startInfo.UseShellExecute = false;
                startInfo.Arguments = args.ToString();
                Process retValue = Process.Start(startInfo);
                string startupEventName = parameters[0];
                bool createdNew;
                EventWaitHandle startupEvent = new EventWaitHandle(false, EventResetMode.ManualReset, startupEventName, out createdNew);
                if (!createdNew) startupEvent.Reset();
                bool b = startupEvent.WaitOne(15000);
                Assert.IsTrue(b);
                return retValue;
            }
            else
            {
                string commandline = string.Format("{0} {1}", processPath, args);
                // connect
                ConnectionOptions connOpt = new ConnectionOptions();
                connOpt.Impersonation = ImpersonationLevel.Impersonate;
                connOpt.EnablePrivileges = true;
                ManagementScope scope = new ManagementScope(String.Format(@"\\{0}\ROOT\CIMV2", machineName), connOpt);
                scope.Connect();

                ObjectGetOptions objectGetOptions = new ObjectGetOptions();
                ManagementPath managementPath = new ManagementPath("Win32_Process");
                ManagementClass processClass = new ManagementClass(scope, managementPath, objectGetOptions);
                ManagementBaseObject inParams = processClass.GetMethodParameters("Create");
                inParams["CommandLine"] = commandline;
                inParams["CurrentDirectory"] = Path.GetDirectoryName(processPath);

                ManagementBaseObject outParams = processClass.InvokeMethod("Create", inParams, null);
                int processId = int.Parse(outParams["processId"].ToString());
                return Process.GetProcessById(processId, machineName);
            }
        }
        static public void WriteConfigFile(string fileName, OrleansConfiguration config)
        {
            StringBuilder content = new StringBuilder();
            content.AppendFormat(@"<?xml version=""1.0"" encoding=""utf-8""?>
<OrleansConfiguration xmlns=""urn:orleans"">
  <Deployment>
    <Silo Name=""Primary"" HostName=""localhost"" />
    <Silo Name=""Node2"" HostName=""localhost"" />
    <Silo Name=""Node3"" HostName=""localhost"" />
  </Deployment>
  <Globals>
    <SeedNode Address=""localhost"" Port=""11111"" />
    <Tasks Disabled=""true""/>
    <Messaging ResponseTimeout=""3000"" SiloSenderQueues=""5"" MaxResendCount=""0""/>
    {0}
  </Globals>
  <Defaults>
    <Networking Address=""localhost"" Port=""0"" />
    <Scheduler MaxActiveThreads=""0"" />
    <Tracing DefaultTraceLevel=""Info"" TraceToConsole=""true"" TraceToFile=""{{0}}-{{1}}.log"" WriteMessagingTraces=""false""/>
  </Defaults>
  <Override Node=""Primary"">
      <Networking Port=""11111"" />
      <ProxyingGateway Address=""localhost"" Port=""30000"" />
  </Override>
  <Override Node=""Node2"">
    <Networking Port=""11113"" />
  </Override>
  <Override Node=""Node3"">
    <Networking Port=""11114"" />
  </Override>
</OrleansConfiguration>",
                        ToXmlString(config.Globals.Application)
                        );

            using (StreamWriter writer = new StreamWriter(fileName))
            {
                writer.WriteLine(content.ToString());
                writer.Flush();
            }
        }
        private static string ToXmlString(ApplicationConfiguration appConfig)
        {
            StringBuilder result = new StringBuilder();
            result.AppendFormat("            <Application>");
            result.AppendFormat("                <Defaults>");
            result.AppendFormat("                    <Deactivation AgeLimit=\"{0}\"/>", (long)appConfig.DefaultCollectionAgeLimit.TotalSeconds);
            result.AppendFormat("                </Defaults>");
            foreach (GrainTypeConfiguration classConfig in appConfig.ClassSpecific)
            {
                if (classConfig.CollectionAgeLimit.HasValue)
                {
                    result.AppendFormat("                <GrainType Type=\"{0}\">", classConfig.Type.FullName);
                    result.AppendFormat("                    <Deactivation AgeLimit=\"{0}\"/>", (long)classConfig.CollectionAgeLimit.Value.TotalSeconds);
                    result.AppendFormat("                </GrainType>");
                }
            }
            result.AppendFormat("            </Application>");
            return result.ToString();
        }

        public static void RunScript(string scriptPath, params string[] options)
        {
            Command command = new Command(scriptPath + " " + string.Join(" ", options), true);

            RunspaceConfiguration runspaceConfiguration = RunspaceConfiguration.Create();

            Runspace runspace = RunspaceFactory.CreateRunspace(runspaceConfiguration);
            runspace.Open();

            RunspaceInvoke scriptInvoker = new RunspaceInvoke(runspace);

            using (Pipeline pipeline = runspace.CreatePipeline())
            {
                pipeline.Commands.Add(command);

                try
                {
                    var results = pipeline.Invoke();
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: " + e);
                }
            }
        }

        public static TimeSpan TimeRun(int numIterations, TimeSpan baseline, string what, Action action)
        {
            var stopwatch = new Stopwatch();

            long startMem = GC.GetTotalMemory(true);
            stopwatch.Start();

            action();

            stopwatch.Stop();
            long stopMem = GC.GetTotalMemory(false);
            long memUsed = stopMem - startMem;
            TimeSpan duration = stopwatch.Elapsed;

            string timeDeltaStr = "";
            if (baseline > TimeSpan.Zero)
            {
                double delta = (duration - baseline).TotalMilliseconds / baseline.TotalMilliseconds;
                timeDeltaStr = String.Format("-- Change = {0}%", 100.0 * delta);
            }
            Console.WriteLine("Time for {0} loops doing {1} = {2} {3} Memory used={4}", numIterations, what, duration, timeDeltaStr, memUsed);
            return duration;
        }

        public static int GetRandomGrainId()
        {
            return rand.Next();
        }


        public static void ConfigureClientThreadPoolSettingsForStorageTests(int NumDotNetPoolThreads = 200)
        {
            ThreadPool.SetMinThreads(NumDotNetPoolThreads, NumDotNetPoolThreads);
            ServicePointManager.Expect100Continue = false;
            ServicePointManager.DefaultConnectionLimit = NumDotNetPoolThreads; // 1000;
            ServicePointManager.UseNagleAlgorithm = false;
        }

        public static async Task WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan timeout)
        {
            bool keepGoing = true;
            int numLoops = 0;
            // ReSharper disable AccessToModifiedClosure
            Func<Task> loop =
                async () =>
                {
                    do
                    {
                        numLoops++;
                        // need to wait a bit to before re-checking the condition.
                        await Task.Delay(TimeSpan.FromSeconds(1));
                    }
                    while (!await predicate() && keepGoing);
                };
            // ReSharper restore AccessToModifiedClosure

            var task = loop();
            try
            {
                await Task.WhenAny(new Task[] { task, Task.Delay(timeout) });
            }
            finally
            {
                keepGoing = false;
            }
            Assert.IsTrue(task.IsCompleted, "The test completed {0} loops then timed out after {1}", numLoops, timeout);
        }

        public void SuppressFastKillInHandleProcessExit()
        {
            foreach (var silo in GetActiveSilos())
            {
                if (silo != null && silo.Silo != null && silo.Silo.TestHookup != null)
                {
                    silo.Silo.TestHookup.SuppressFastKillInHandleProcessExit();
                }
            }
        }


        public static double CalibrateTimings()
        {
            const int NumLoops = 10000;
            TimeSpan baseline = TimeSpan.FromTicks(80); // Baseline from jthelin03D
            int n;
            var sw = Stopwatch.StartNew();
            for (int i = 0; i < NumLoops; i++)
            {
                n = i;
            }
            sw.Stop();
            double multiple = 1.0 * sw.ElapsedTicks / baseline.Ticks;
            Console.WriteLine("CalibrateTimings: {0} [{1} Ticks] vs {2} [{3} Ticks] = x{4}",
                sw.Elapsed, sw.ElapsedTicks,
                baseline, baseline.Ticks,
                multiple);
            return multiple > 1.0 ? multiple : 1.0;
        }
    }

    public enum BooleanEnum
    {
        None = 0,
        True = 1,
        False = 2
    }

    public class Options
    {
        public Options()
        {
            // all defaults except:
            DomainName = null;
            OverrideConfig = true;
            StartFreshOrleans = true;
            StartPrimary = true;
            StartSecondary = true;
            StartClient = true;
            PickNewDeploymentId = false;
            AzureDataConnectionString = null;
            ReminderServiceType = GlobalConfiguration.ReminderServiceProviderType.NotSpecified;
            MaxActiveThreads = 5;
            PropagateActivityId = Constants.DEFAULT_PROPAGATE_E2E_ACTIVITY_ID;
            BasePort = -1; // use default from configuration file
            MaxForwardCount = MessagingConfiguration.DEFAULT_MAX_FORWARD_COUNT;
            MachineName = ".";
            StartOutOfProcess = GetConfigFlag("StartOutOfProcess", false);
            SiloGenerationNumber = -1;
            performDeadlockDetection = BooleanEnum.None;
            LargeMessageWarningThreshold = 0;
            LivenessType = GlobalConfiguration.LivenessProviderType.NotSpecified;
            //StartOutOfProcess = GetConfigFlag("StartOutOfProcess",true);
            //UseMockTable = true;
            //UseMockOracle = true;
            CollectionQuantum = GlobalConfiguration.DEFAULT_COLLECTION_QUANTUM;

        }
        private bool GetConfigFlag(string name, bool defaultValue)
        {
            bool result;
            if (bool.TryParse(ConfigurationManager.AppSettings[name], out result))
                return result;
            else return defaultValue;
        }
        public Options Copy()
        {
            return new Options
            {
                DomainName = DomainName,
                OverrideConfig = OverrideConfig,
                StartOutOfProcess = StartOutOfProcess,
                StartFreshOrleans = StartFreshOrleans,
                StartPrimary = StartPrimary,
                StartSecondary = StartSecondary,
                StartClient = StartClient,
                SiloConfigFile = SiloConfigFile,
                PickNewDeploymentId = PickNewDeploymentId,
                AzureDataConnectionString = AzureDataConnectionString,
                ReminderServiceType = ReminderServiceType,
                MaxActiveThreads = MaxActiveThreads,
                BasePort = BasePort,
                MaxForwardCount = MaxForwardCount,
                MachineName = MachineName,
                LargeMessageWarningThreshold = LargeMessageWarningThreshold,

                CollectionTotalMemoryLimit = CollectionTotalMemoryLimit,
                DefaultCollectionAgeLimit = DefaultCollectionAgeLimit,
                CollectionQuantum = CollectionQuantum,
                SiloGenerationNumber = SiloGenerationNumber,

                DirectoryReplicationFactor = DirectoryReplicationFactor,
                PerformDeadlockDetection = PerformDeadlockDetection,
                PropagateActivityId = PropagateActivityId,
                LivenessType = LivenessType
            };
        }
        public string DomainName { get; set; }
        public bool OverrideConfig { get; set; }
        public bool StartOutOfProcess { get; set; }
        public bool StartFreshOrleans { get; set; }
        public bool StartPrimary { get; set; }
        public bool StartSecondary { get; set; }
        public bool StartClient { get; set; }

        public FileInfo SiloConfigFile { get; set; }
        public bool PickNewDeploymentId { get; set; }
        public string AzureDataConnectionString { get; set; }
        public GlobalConfiguration.ReminderServiceProviderType ReminderServiceType { get; set; }

        public int MaxActiveThreads { get; set; }
        public bool PropagateActivityId { get; set; }
        public int BasePort { get; set; }
        public int MaxForwardCount { get; set; }
        public string MachineName { get; set; }
        public int LargeMessageWarningThreshold { get; set; }

        public int CollectionTotalMemoryLimit { get; set; }
        public TimeSpan DefaultCollectionAgeLimit { get; set; }
        public TimeSpan CollectionQuantum { get; set; }
        public int DirectoryReplicationFactor { get; set; }
        public int SiloGenerationNumber { get; set; }
        public GlobalConfiguration.LivenessProviderType LivenessType { get; set; }

        internal BooleanEnum performDeadlockDetection;
        public bool PerformDeadlockDetection
        {
            get { return performDeadlockDetection == BooleanEnum.True; }
            set { performDeadlockDetection = (value ? BooleanEnum.True : BooleanEnum.False); }
        }
    }

    public class ClientOptions
    {
        public ClientOptions()
        {
            // all defaults except:
            ResponseTimeout = Constants.DEFAULT_RESPONSE_TIMEOUT;
            ProxiedGateway = false;
            Gateways = null;
            PreferedGatewayIndex = -1;
            PropagateActivityId = Constants.DEFAULT_PROPAGATE_E2E_ACTIVITY_ID;
            ClientConfigFile = null;
        }

        public ClientOptions Copy()
        {
            return new ClientOptions
            {
                ResponseTimeout = ResponseTimeout,
                ProxiedGateway = ProxiedGateway,
                Gateways = Gateways,
                PreferedGatewayIndex = PreferedGatewayIndex,
                PropagateActivityId = PropagateActivityId,
                ClientConfigFile = ClientConfigFile,
            };
        }
        public TimeSpan ResponseTimeout { get; set; }
        public bool ProxiedGateway { get; set; }
        public List<IPEndPoint> Gateways { get; set; }
        public int PreferedGatewayIndex { get; set; }
        public bool PropagateActivityId { get; set; }
        public FileInfo ClientConfigFile { get; set; }
    }
}
