using System;
using System.IO;
using System.Net;
using System.Reflection;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Orleans
{
    /// <summary>
    /// Configures client runtime for connecting to Orleans system
    /// </summary>
    public static class OrleansClient
    {
        /// <summary>
        /// Whether the client runtime has already been initialized
        /// </summary>
        /// <returns><c>true</c> if client runtime is already initialized</returns>
        public static bool IsInitialized { get { return isFullyInitialized && GrainClient.Current != null; } }

        private static bool isFullyInitialized = false;

        private static OutsideGrainClient outsideGrainClient;

        private static readonly object initLock = new Object();

        /// <summary>
        /// Initializes the client runtime from the standard client configuration file.
        /// </summary>
        public static void Initialize()
        {
            ClientConfiguration config = ClientConfiguration.StandardLoad();
            if (config == null)
            {
                Console.WriteLine("Error loading standard client configuration file.");
                throw new ArgumentException("Error loading standard client configuration file");
            }
            InternalInitialize(config);
        }

        /// <summary>
        /// Initializes the client runtime from the provided client configuration file.
        /// If an error occurs reading the specified configuration file, the initialization fails.
        /// </summary>
        /// <param name="configFilePath">A relative or absolute pathname for the client configuration file.</param>
        public static void Initialize(string configFilePath)
        {
            ClientConfiguration config;
            try
            {
                config = ClientConfiguration.LoadFromFile(configFilePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error loading client configuration file {0}: {1}", configFilePath, ex);
                throw;
            }
            if (config == null)
            {
                Console.WriteLine("Error loading client configuration file {0}:", configFilePath);
                throw new ArgumentException(String.Format("Error loading client configuration file {0}:", configFilePath), "configFilePath");
            }
            InternalInitialize(config);
        }

        /// <summary>
        /// Initializes the client runtime from the provided client configuration file.
        /// If an error occurs reading the specified configuration file, the initialization fails.
        /// </summary>
        /// <param name="configFile">The client configuration file.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1011:ConsiderPassingBaseTypesAsParameters")]
        public static void Initialize(FileInfo configFile)
        {
            ClientConfiguration config;
            try
            {
                config = ClientConfiguration.LoadFromFile(configFile.FullName);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error loading client configuration file {0}: {1}", configFile.FullName, ex);
                throw;
            }
            if (config == null)
            {
                Console.WriteLine("Error loading client configuration file {0}:", configFile.FullName);
                throw new ArgumentException(String.Format("Error loading client configuration file {0}:", configFile.FullName), "configFile");
            }
            InternalInitialize(config);
        }

        /// <summary>
        /// Initializes the client runtime from the provided client configuration object. 
        /// If the configuration object is null, the initialization fails. 
        /// </summary>
        /// <param name="config">A ClientConfiguration object.</param>
        public static void Initialize(ClientConfiguration config)
        {
            if (config == null)
            {
                Console.WriteLine("Initialize was called with null ClientConfiguration object.");
                throw new ArgumentException("Initialize was called with null ClientConfiguration object.", "config");
            }
            InternalInitialize(config);
        }

        /// <summary>
        /// Initializes the client runtime from the standard client configuration file using the provided gateway address.
        /// Any gateway addresses specified in the config file will be ignored and the provided gateway address wil be used instead. 
        /// </summary>
        /// <param name="gatewayAddress">IP address and port of the gateway silo</param>
        /// <param name="overrideConfig">Whether the specified gateway endpoint should override / replace the values from config file, or be additive</param>
        public static void Initialize(IPEndPoint gatewayAddress, bool overrideConfig = true)
        {
            ClientConfiguration config = ClientConfiguration.StandardLoad();
            if (config == null)
            {
                Console.WriteLine("Error loading standard client configuration file.");
                throw new ArgumentException("Error loading standard client configuration file");
            }
            if (overrideConfig)
            {
                config.Gateways = new List<IPEndPoint>(new[] { gatewayAddress });
            }
            else if (!config.Gateways.Contains(gatewayAddress))
            {
                config.Gateways.Add(gatewayAddress);
            }
            config.PreferedGatewayIndex = config.Gateways.IndexOf(gatewayAddress);
            InternalInitialize(config);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void InternalInitialize(ClientConfiguration config, OutsideGrainClient grainClient = null)
        {
            // We deliberately want to run this initialization code on .NET thread pool thread to escape any 
            // TPL execution environment and avoid any conflicts with client's synchronization context
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>();
            WaitCallback DoInit = state =>
            {
                try
                {
                    DoInternalInitialize(config, grainClient);
                    tcs.SetResult(true); // Resolve promise
                }
                catch (Exception exc)
                {
                    tcs.SetException(exc); // Break promise
                }
            };
            // Queue Init call to thread pool thread
            ThreadPool.QueueUserWorkItem(DoInit, null);
            tcs.Task.Wait(); // Wait for Init to finish
        }

        /// <summary>
        /// Initializes client runtime from client configuration object.
        /// </summary>
        private static void DoInternalInitialize(ClientConfiguration config, OutsideGrainClient grainClient = null)
        {
            if (IsInitialized)
                return;

            lock (initLock)
            {
                if (!IsInitialized)
                {
                    try
                    {
                        // this is probably overkill, but this ensures isFullyInitialized false
                        // before we make a call that makes GrainClient.Current not null
                        isFullyInitialized = false;

                        //Console.WriteLine(string.Format("Connecting to Orleans gateway address = {0}", gateway.Endpoint.ToString()));

                        if (grainClient == null)
                        {
                            grainClient = new OutsideGrainClient(config, false);
                        }
                        outsideGrainClient = grainClient;  // Keep reference, to avoid GC problems
                        outsideGrainClient.Start();

                        LimitManager.Initialize(config);

                        // this needs to be the last successful step inside the lock so 
                        // IsInitialized doesn't return true until we're fully initialized
                        isFullyInitialized = true;
                    }
                    catch (Exception exc)
                    {
                        // just make sure to fully Uninitialize what we managed to partially initialize, so we don't end up in inconsistent state and can later on re-initialize.
                        Console.WriteLine("Initialization failed. {0}", exc);
                        InternalUninitialize();
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Uninitializes client runtime.
        /// </summary>
        public static void Uninitialize()
        {
            lock (initLock)
            {
                InternalUninitialize();
            }
        }

        /// <summary>
        /// This is the lock free version of uninitilize so we can share 
        /// it between the public method and error paths inside initialize.
        /// This should only be called inside a lock(initLock) block.
        /// </summary>
        private static void InternalUninitialize()
        {
            // Update this first so IsInitialized immediately begins returning
            // false.  Since this method should be protected externally by 
            // a lock(initLock) we should be able to reset everything else 
            // before the next init attempt.
            isFullyInitialized = false;

            LimitManager.UnInitialize();
            
            if (GrainClient.Current != null)
            {
                try
                {
                    GrainClient.InternalCurrent.Reset();
                }
                catch (Exception) { }

                GrainClient.Current = null;
            }
            outsideGrainClient = null;
        }

        /// <summary>
        /// Check that the runtime is intialized correctly, and throw InvalidOperationException if not
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown if Orleans runtime is not correctly initialized before this call.</exception>
        private static void CheckInitialized()
        {
            if (!IsInitialized)
                throw new InvalidOperationException("Runtime is not initialized. Call OrleansClient.Initialize method to initialize the runtime.");
        }

        /// <summary>
        /// Provides logging facility for applications.
        /// </summary>
        /// <exception cref="InvalidOperationException">Thrown if Orleans runtime is not correctly initialized before this call.</exception>
        public static OrleansLogger Logger
        {
            get
            {
                CheckInitialized();
                return GrainClient.Current.AppLogger;
            }
        }

        /// <summary>
        /// Set a timeout for responses on this Orleans client.
        /// </summary>
        /// <param name="timeout"></param>
        /// <exception cref="InvalidOperationException">Thrown if Orleans runtime is not correctly initialized before this call.</exception>
        public static void SetResponseTimeout(TimeSpan timeout)
        {
            CheckInitialized();
            GrainClient.Current.SetResponseTimeout(timeout);
        }

        /// <summary>
        /// Get a timeout of responses on this Orleans client.
        /// </summary>
        /// <returns>The response timeout.</returns>
        /// <exception cref="InvalidOperationException">Thrown if Orleans runtime is not correctly initialized before this call.</exception>
        public static TimeSpan GetResponseTimeout()
        {
            CheckInitialized();
            return GrainClient.Current.GetResponseTimeout();
        }

#if !DISABLE_STREAMS
        public static IEnumerable<Streams.IStreamProvider> GetStreamProviders()
        {
            return GrainClient.InternalCurrent.CurrentStreamProviderManager.GetStreamProviders();
        }

        public static Streams.IStreamProvider GetStreamProvider(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentNullException("name");
            return GrainClient.InternalCurrent.CurrentStreamProviderManager.GetProvider(name) as Streams.IStreamProvider;
        }
#endif

        internal static List<IPEndPoint> Gateways
        {
            get
            {
                CheckInitialized();
                return outsideGrainClient.Gateways;
            }
        }

        internal static MethodInfo GetStaticMethodThroughReflection(string assemblyName, string className, string methodName, Type[] argumentTypes)
        {
            Assembly asm = Assembly.Load(assemblyName);
            if (asm == null) 
                throw new InvalidOperationException(string.Format("Cannot find assembly {0}", assemblyName));

            Type cl = asm.GetType(className);
            if (cl == null) 
                throw new InvalidOperationException(string.Format("Cannot find class {0} in assembly {1}", className, assemblyName));

            MethodInfo method;
            if(argumentTypes == null)
                method = cl.GetMethod(methodName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);
            else
                method = cl.GetMethod(methodName, BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static, null, argumentTypes, null);
            
            if (method == null) 
                throw new InvalidOperationException(string.Format("Cannot find static method {0} of class {1} in assembly {2}", methodName, className, assemblyName));

            return method;
        }

        internal static object InvokeStaticMethodThroughReflection(string assemblyName, string className, string methodName, Type[] argumentTypes, object[] arguments)
        {
            MethodInfo method = GetStaticMethodThroughReflection(assemblyName, className, methodName, argumentTypes);
            return method.Invoke(null, arguments);
        }

        internal static Type LoadTypeThroughReflection(string assemblyName, string className)
        {
            Assembly asm = Assembly.Load(assemblyName);
            if (asm == null) throw new InvalidOperationException(string.Format("Cannot find assembly {0}", assemblyName));

            Type cl = asm.GetType(className);
            if (cl == null) throw new InvalidOperationException(string.Format("Cannot find class {0} in assembly {1}", className, assemblyName));

            return cl;
        }
    }
}
