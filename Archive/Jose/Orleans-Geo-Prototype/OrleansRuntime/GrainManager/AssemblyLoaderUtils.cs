using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

using System.Diagnostics;
using System.IO;

namespace Orleans.Runtime
{
    internal abstract class AssemblyLoadLogEntry
    {
        /// <summary>
        /// The name of the item to resolve.
        /// </summary>
        public string AssemblyName { get; internal set; }

        public StackTrace StackTrace { get; internal set; }
    }

    internal class AssemblyLoadRequestDetails : AssemblyLoadLogEntry
    {
        public AssemblyLoadRequestDetails()
        {
            this.StackTrace = new StackTrace(2); // Stack trace for our callers-caller
        }
        public AssemblyLoadRequestDetails(ResolveEventArgs assemblyResolveEvent)
        {
            this.StackTrace = new StackTrace(3); // Omit event handler callback from the stack trace
            this.AssemblyName = assemblyResolveEvent.Name;
            this.RequestingAssembly = assemblyResolveEvent.RequestingAssembly;
        }

        /// <summary>
        /// Gets the assembly whose dependency is being resolved.
        /// </summary>
        public Assembly RequestingAssembly { get; internal set; }
    }

    internal class AssemblyLoadCompleteDetails : AssemblyLoadLogEntry
    {
        public AssemblyLoadCompleteDetails()
        {
            this.StackTrace = new StackTrace(2); // Stack trace for our callers-caller
        }
        public AssemblyLoadCompleteDetails(AssemblyLoadEventArgs assemblyLoadEvent)
        {
            this.StackTrace = new StackTrace(3); // Omit event handler callback from the stack trace
            this.LoadedAssembly = assemblyLoadEvent.LoadedAssembly;
            this.AssemblyName = this.LoadedAssembly.FullName;
            if (!LoadedAssembly.IsDynamic)
            {
                try { this.AssemblyLocation = this.LoadedAssembly.Location; }
                catch { }
            }
        }

        /// <summary>
        /// Gets the assembly which has just been loaded.
        /// </summary>
        public Assembly LoadedAssembly { get; internal set; }

        /// <summary>
        /// Gets the assembly which has just been loaded from.
        /// </summary>
        public string AssemblyLocation { get; internal set; }
    }

    internal class AssemblyLoaderUtils
    {
        private static bool TraceLoadRequests { get; set; }
        private static List<AssemblyLoadLogEntry> AssemblyLoadLogEntries { get; set; }

        private static Logger logger = Logger.GetLogger("AssemblyLoadTracer");

        public static void EnableAssemblyLoadTracing()
        {
            EnableAssemblyLoadTracing(false);
        }
        public static void EnableAssemblyLoadTracing(bool realtimeTrace)
        {
            logger.Info("Starting assembly load request tracing");

            ClearLog();

            TraceLoadRequests = realtimeTrace;

            AppDomain.CurrentDomain.AssemblyResolve += AssemblyResolveTracer;
            AppDomain.CurrentDomain.AssemblyLoad += AssemblyLoadTracer;
        }

        public static void DisableAssemblyLoadTracing()
        {
            logger.Info("Stopping assembly load request tracing");

            TraceLoadRequests = false;

            AppDomain.CurrentDomain.AssemblyResolve -= AssemblyResolveTracer;
            AppDomain.CurrentDomain.AssemblyLoad -= AssemblyLoadTracer;
        }

        public static void AddLog(AssemblyLoadLogEntry lr)
        {
            if (AssemblyLoadLogEntries == null)
            {
                AssemblyLoadLogEntries = new List<AssemblyLoadLogEntry>();
            }

            AssemblyLoadLogEntries.Add(lr);

            if (TraceLoadRequests)
            {
                DumpLogEntry(lr);
            }
        }

        public static void ClearLog()
        {
            if (AssemblyLoadLogEntries == null)
            {
                AssemblyLoadLogEntries = new List<AssemblyLoadLogEntry>();
            }
            else
            {
                AssemblyLoadLogEntries.Clear();
            }
        }

        public static void DumpLog()
        {
            if (AssemblyLoadLogEntries != null)
            {
                logger.Info("Dumping Assembly load request log.");
                AssemblyLoadLogEntries.ForEach( (lr) => DumpLogEntry(lr) );
            }
            else
            {
                logger.Info("Assembly load request tracing was not enabled.");
            }
        }

        private static void DumpLogEntry(AssemblyLoadLogEntry loadLogEntry)
        {
            var lc = loadLogEntry as AssemblyLoadCompleteDetails;
            if (lc != null)
            {
                logger.Info("Assembly loaded successfully: Assembly={0} from Location={1} {2}",
                   lc.AssemblyName, lc.AssemblyLocation, lc.StackTrace);
            }
            else
            {
                var lr = loadLogEntry as AssemblyLoadRequestDetails;
                logger.Info("Assembly load request: Assembly={0} requested by Assembly={1} {2}",
                   lr.AssemblyName, lr.RequestingAssembly, lr.StackTrace);
            }
        }

        /// <see cref="ResolveEventHandler"/>
        public static Assembly AssemblyResolveTracer(object sender, ResolveEventArgs args)
        {
            AssemblyLoadRequestDetails lr = new AssemblyLoadRequestDetails(args);
            AssemblyLoaderUtils.AddLog(lr);
            return null;
        }

        /// <see cref="AssemblyLoadEventHandler"/>
        public static void AssemblyLoadTracer(object sender, AssemblyLoadEventArgs args)
        {
            AssemblyLoadCompleteDetails lc = new AssemblyLoadCompleteDetails(args);
            AssemblyLoaderUtils.AddLog(lc);
        }

        /// <see cref="ResolveEventHandler"/>
        public static Assembly EmbeddedAssemblyResolver(object sender, ResolveEventArgs args)
        {
            String resourceName = "AssemblyLoadingAndReflection." + new AssemblyName(args.Name).Name + ".dll";

            using (var stream = Assembly.GetExecutingAssembly().GetManifestResourceStream(resourceName)) {
                Byte[] assemblyData = new Byte[stream.Length];
                stream.Read(assemblyData, 0, assemblyData.Length);
                return Assembly.Load(assemblyData);
            }
        }

        public static string GetLocationSafe(Assembly a)
        {
            if (a.IsDynamic)
            {
                return "dynamic";
            }
            else
            {
                try
                {
                    return a.Location;
                }
                catch (Exception)
                {
                    return "unknown";
                }
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2001:AvoidCallingProblematicMethods", MessageId = "System.Reflection.Assembly.LoadFrom")]
        public static Assembly GetActivationAssembly(Assembly grainAssembly)
        {
            Assembly retAsm = null;

            string grainDllLocation = GetLocationSafe(grainAssembly);
            string activationDllPath = Path.Combine(
                Path.GetDirectoryName(grainDllLocation),
                Path.GetFileNameWithoutExtension(grainDllLocation) + GrainClientGenerator.GrainInterfaceData.ActivationDllSuffix + Path.GetExtension(grainDllLocation));

            if (File.Exists(activationDllPath))
            {
                // It is okay to use LoadFrom here because we are loading application assemblies deployed to the specific directory.
                // Such application assemblies should not be deployed somewhere else, e.g. GAC, so this is safe.
                retAsm = Assembly.LoadFrom(activationDllPath);
            }
            return retAsm;
        }
    }
}
