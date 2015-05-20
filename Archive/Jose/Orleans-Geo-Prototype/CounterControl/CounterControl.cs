using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Security.Principal;
using Orleans.Runtime.Counters;


namespace Orleans.Counter.Control
{
    /// <summary>
    /// Control Orleans Counters - Register or Unregister the Orleans counter set
    /// </summary>
    internal class CounterControl
    {
        public bool Unregister { get; private set; }
        public bool BruteForce { get; private set; }
        public bool NeedRunAsAdministrator { get; private set; }
        public bool IsRunningAsAdministrator { get; private set; }
        public bool PauseAtEnd { get; private set; }

        public CounterControl()
        {
            // Check user is Administrator and has granted UAC elevation permission to run this app
            WindowsIdentity userIdent = WindowsIdentity.GetCurrent();
            WindowsPrincipal userPrincipal = new WindowsPrincipal(userIdent);
            this.IsRunningAsAdministrator = userPrincipal.IsInRole(WindowsBuiltInRole.Administrator);
        }

        public void PrintUsage()
        {
            using (StringWriter usageStr = new StringWriter())
            {
                usageStr.WriteLine(Assembly.GetExecutingAssembly().GetName().Name + ".exe {command}");
                usageStr.WriteLine("Where commands are:");
                usageStr.WriteLine(" /? or /help       = Display usage info");
                usageStr.WriteLine(" /r or /register   = Register Windows performance counters for Orleans [default]");
                usageStr.WriteLine(" /u or /unregister = Unregister Windows performance counters for Orleans");
                usageStr.WriteLine(" /f or /force      = Use brute force, if necessary");
                usageStr.WriteLine(" /pause            = Pause for user key press after operation");

                ConsoleText.WriteUsage(usageStr.ToString());
            }
        }

        public bool ParseArguments(string[] args)
        {
            bool ok = true;
            this.NeedRunAsAdministrator = true;
            this.Unregister = false;

            foreach (string arg in args)
            {
                if (arg.StartsWith("/") || arg.StartsWith("-"))
                {
                    var a = arg.ToLowerInvariant().Substring(1);
                    switch (a)
                    {
                        case "r":
                        case "register":
                            this.Unregister = false;
                            break;
                        case "u":
                        case "unregister":
                            this.Unregister = true;
                            break;
                        case "f":
                        case "force":
                            this.BruteForce = true;
                            break;
                        case "pause":
                            this.PauseAtEnd = true;
                            break;
                        case "?":
                        case "help":
                            this.NeedRunAsAdministrator = false;
                            ok = false;
                            break;
                        default:
                            this.NeedRunAsAdministrator = false;
                            ok = false;
                            break;
                    }
                }
                else
                {
                    ConsoleText.WriteError("Unrecognised command line option: " + arg);
                    ok = false;
                }
            }

            return ok;
        }

        public int Run()
        {
            if (this.NeedRunAsAdministrator && !this.IsRunningAsAdministrator)
            {
                ConsoleText.WriteError("Need to be running in Administrator role to perform the requested operations.");
                return 1;
            }

            InitConsoleLogging();

            try
            {
                if (this.Unregister) 
                {
                    ConsoleText.WriteStatus("Unregistering Orleans performance counters with Windows");

                    UnregisterWindowsPerfCounters(this.BruteForce);
                }
                else 
                {
                    ConsoleText.WriteStatus("Registering Orleans performance counters with Windows");

                    RegisterWindowsPerfCounters(true); // Always reinitialize counter registrations, even if already existed
                }

                ConsoleText.WriteStatus("Operation completed successfully.");
                return 0;
            }
            catch (Exception exc) {
                ConsoleText.WriteError("Error running CounterControl.exe", exc);

                if (this.BruteForce) {
                    ConsoleText.WriteStatus("Ignoring error due to brute-force mode");
                    return 0;
                }
                else {
                    return 2;
                }
            }
        }

        /// <summary>
        /// Initialize log infrastrtucture for Orleans runtime sub-components
        /// </summary>
        private static void InitConsoleLogging()
        {
            Trace.Listeners.Clear();
            NodeConfiguration cfg = new NodeConfiguration();
            cfg.TraceFilePattern = null;
            cfg.TraceToConsole = false;
            Logger.Initialize(cfg);
            var logWriter = new LogWriterToConsole(true, true); // Use compact console output & no timestamps / log message metadata
            Logger.LogConsumers.Add(logWriter);
        }

        /// <summary>
        /// Create the set of Orleans counters, if they do not already exist
        /// </summary>
        /// <param name="useBruteForce">Use brute force, if necessary</param>
        /// <remarks>Note: Program needs to be running as Administrator to be able to register Windows perf counters.</remarks>
        private static void RegisterWindowsPerfCounters(bool useBruteForce)
        {
            try 
            {
                if (OrleansPerfCounterManager.AreWindowsPerfCountersAvailable())
                {
                    if (!useBruteForce)
                    {
                        ConsoleText.WriteStatus("Orleans counters are already registered -- Use brute-force mode to re-initialize");
                        return;
                    }
                    else
                    {
                        // Delete any old perf counters
                        UnregisterWindowsPerfCounters(true);
                    }
                }

                // Register perf counters
                OrleansPerfCounterManager.InstallCounters();

                if (OrleansPerfCounterManager.AreWindowsPerfCountersAvailable()) 
                {
                    ConsoleText.WriteStatus("Orleans counters registered successfully");
                }
                else
                {
                    ConsoleText.WriteError("Orleans counters are NOT registered");
                }
            }
            catch (Exception exc) {
                ConsoleText.WriteError("Error registering Orleans counters - {0}" + exc);
                throw;
            }
        }

        /// <summary>
        /// Remove the set of Orleans counters, if they already exist
        /// </summary>
        /// <param name="useBruteForce">Use brute force, if necessary</param>
        /// <remarks>Note: Program needs to be running as Administrator to be able to unregister Windows perf counters.</remarks>
        private static void UnregisterWindowsPerfCounters(bool useBruteForce)
        {
            if (!OrleansPerfCounterManager.AreWindowsPerfCountersAvailable())
            {
                ConsoleText.WriteStatus("Orleans counters are already unregistered");
            }
            else {
                // Delete any old perf counters
                try
                {
                    OrleansPerfCounterManager.DeleteCounters();
                }
                catch (Exception exc)
                {
                    ConsoleText.WriteError("Error deleting old Orleans counters - {0}" + exc);
                    if (useBruteForce)
                    {
                        ConsoleText.WriteStatus("Ignoring error deleting Orleans counters due to brute-force mode");
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }
    }
}
