using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Runtime.Remoting.Channels.Ipc;
using System.Text;

namespace Orleans.Host
{
    public class AppDomainHost : MarshalByRefObject, IAppDomainHost
    {
        public static readonly int BaseServerPort = 8085;
        public const string ServiceName = "AppDomainHost";

        internal static readonly bool UseIpcChannel = false;
        internal static readonly bool UseSecureChannel = false;

        private static readonly Dictionary<string, Type> loadedTypes = new Dictionary<string, Type>();
        private static bool initServerDone;
        private static bool initClientDone;

        static void Main(string[] args)
        {
            Log("Main");

            int port = BaseServerPort;
            if (args != null && args.Length > 0)
            {
                if (Int32.TryParse(args[0], out port))
                {
                    Console.WriteLine("Setting listen port to {0}", port);
                }
            }

            InitServer(port);

            PressAnyKey();
        }

        public AppDomainHost()
        {
            Log("Constructor in AppDomain={0}", AppDomain.CurrentDomain.FriendlyName);
        }

        public static void InitClient()
        {
            if (initClientDone) return;

            Log("InitClient in AppDomain={0}", AppDomain.CurrentDomain.FriendlyName);
            IChannel channel = UseIpcChannel ? (IChannel) new IpcChannel() : new TcpChannel();
            ChannelServices.RegisterChannel(channel, UseSecureChannel);
            initClientDone = true;
        }

        public static void InitServer(int port)
        {
            if (initServerDone) return;

            Log("InitServer in AppDomain={0}", AppDomain.CurrentDomain.FriendlyName);

            const string remotingUrl = ServiceName;
            string protocol = UseIpcChannel ? "ipc" : "tcp";

            string portName = UseIpcChannel ? GetIpcPortName(port) : port.ToString();
            IChannel channel = UseIpcChannel ? (IChannel) new IpcChannel(portName) : new TcpChannel(port);
            ChannelServices.RegisterChannel(channel, UseSecureChannel);
            RemotingConfiguration.ApplicationName = ServiceName;

            Log("Creating {0} server port={1} url={2}", protocol, portName, remotingUrl);
            const WellKnownObjectMode hostMode = WellKnownObjectMode.SingleCall;
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(AppDomainHost), remotingUrl, hostMode);

            Log("Registered {0} remoting channel at {1} with url={2}", protocol, portName, remotingUrl);

            initServerDone = true;
        }
        internal static string GetIpcPortName(int port)
        {
            return string.Format("{0}-{1}", ServiceName, port);
        }

        internal static string GetIpcUrl(int port)
        {
            return string.Format("ipc://{0}/{1}", GetIpcPortName(port), ServiceName);
        }

        internal static string GetTcpUrl(int port)
        {
            return string.Format("tcp://localhost:{0}/{1}", port, ServiceName);
        }

        internal static string GetHostRemotingUrl(int port)
        {
            return UseIpcChannel ? GetIpcUrl(port) : GetTcpUrl(port);
        }

        public static MarshalByRefObject GetRemoteObject(Type remotableType, int port, object[] args, out Process process)
        {
            if (!initClientDone) InitClient();

            string portName = UseIpcChannel ? GetIpcPortName(port) : port.ToString();

            Log("GetRemoteObject remotableType={0} port={1} in AppDomain={2}", remotableType.FullName, portName, AppDomain.CurrentDomain.FriendlyName);

            string exePath = Assembly.GetExecutingAssembly().CodeBase; // "AppDomainHost.exe"

            process = SpawnProcess(exePath, port);

            string remotingUrl = GetHostRemotingUrl(port);
            Log("Connecting to remoting URL = {0}", remotingUrl);

            // Connect to the remote loader in the spawned process
            AppDomainHost host = (AppDomainHost) RemotingServices.Connect(typeof(AppDomainHost), remotingUrl);

            // Make sure we can talk to the remote loader process
            host.Ping();

            // Load the specified remotable type into the remote loader process
            MarshalByRefObject remote = host.Load(
                remotableType.FullName,
                remotableType.Assembly.FullName,
                args);

            Log("Created host process for {0} on port={1} at url {2}", remotableType.FullName, portName, remotingUrl);

            return remote;
        }

        public static void KillHostProcess(Process process)
        {
            if (process != null)
            {
                string processName = process.ProcessName;
                try
                {
                    Log("Killing host process {0}", processName);
                    process.Kill();
                    Log("Killed host process {0}", processName);
                }
                catch (Exception exc)
                {
                    Log("Ignoring error killing process {0} Exception = {1}", processName, exc);
                }
            }
        }

        private static Process SpawnProcess(string exePath, int port)
        {
            Log("Spawning host process for {0} with port id={1}", exePath, port);
            Process p = Process.Start(exePath, port.ToString());
            return p;
        }

        // Unless we override this method, our AppDomains will time out after 5m.  By returning null, we allow this AppDomain to live indefinitely.
        // See http://social.msdn.microsoft.com/forums/en-us/clr/thread/2D8918FB-08AB-4D52-933B-E472744D1E53
        public override object InitializeLifetimeService() { return null; }

        #region IAppDomainHost methods

        public void Ping()
        {
            Log("Ping");
        }

        public MarshalByRefObject Load(string className, string assemblyName, object[] args = null)
        {
            Log("Trying to load class {0} from assembly {1}", className, assemblyName);
            try
            {
                Type loadType;
                lock (loadedTypes)
                {
                    if (loadedTypes.ContainsKey(className))
                    {
                        loadType = loadedTypes[className];
                    }
                    else
                    {
                        Assembly assy = Assembly.Load(assemblyName);
                        loadType = assy.GetType(className);
                        loadedTypes.Add(className, loadType);
                        Log("Loaded class {0} from assembly {1}", className, assemblyName);
                        RemotingConfiguration.RegisterActivatedServiceType(loadType);
                        Log("Registered remoting type {0}", className);
                    }
                }

                MarshalByRefObject mbro = (MarshalByRefObject) Activator.CreateInstance(loadType, args);
                ObjRef objRef = RemotingServices.Marshal(mbro);
                Log("Load returning {0} with uri = {1}", mbro, objRef.URI);
                return mbro;
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error loading class {0} from assembly {1} - {2}", className, assemblyName, exc);
                throw;
            }
        }

        #endregion

        public static void PressAnyKey()
        {
            Console.WriteLine("Press any key to exit...");
            ConsoleKeyInfo key = Console.ReadKey();
        }

        private static void Log(string msg)
        {
            Console.WriteLine(ServiceName + " - " + msg);
        }

        private static void Log(string fmt, params object[] args)
        {
            Console.WriteLine(ServiceName + " - " + fmt, args);
        }
    }
}
