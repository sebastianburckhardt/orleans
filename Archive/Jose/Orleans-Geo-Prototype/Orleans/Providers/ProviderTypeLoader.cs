using System;
using System.Collections.Generic;
using System.Reflection;


namespace Orleans.Providers
{
    internal class ProviderTypeLoader
    {
        private readonly Func<Type, bool> _condition;
        private readonly Action<Type> _callback;
        private readonly HashSet<Type> _alreadyProcessed;
        public bool IsActive { get; set; }

        private static readonly List<ProviderTypeLoader> managers;

        private static readonly Logger logger = Logger.GetLogger("ProviderTypeLoader", Logger.LoggerType.Runtime);

        static ProviderTypeLoader()
        {
            managers = new List<ProviderTypeLoader>();

            AppDomain.CurrentDomain.AssemblyLoad += ProcessNewAssembly;
        }

        public ProviderTypeLoader(Func<Type, bool> condition, Action<Type> action)
        {
            this._condition = condition;
            _callback = action;
            _alreadyProcessed = new HashSet<Type>();
            IsActive = true;
         }


        public static void AddProviderTypeManager(Func<Type, bool> condition, Action<Type> action)
        {
            var manager = new ProviderTypeLoader(condition, action);

            lock (managers)
            {
                managers.Add(manager);
            }

            manager.ProcessLoadedAssemblies();
        }

        private void ProcessLoadedAssemblies()
        {
            lock (managers)
            {
                // Walk through already-loaded assemblies. 
                // We do this under the lock to avoid race conditions when an assembly is added 
                // while a type manager is initializing.
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    ProcessAssemblyLocally(assembly);
                }
            }
        }

        private void ProcessType(Type type)
        {
            if (!_alreadyProcessed.Contains(type) && !type.IsInterface && !type.IsAbstract && _condition(type))
            {
                _alreadyProcessed.Add(type);
                _callback(type);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void ProcessAssemblyLocally(Assembly assembly)
        {
            if (IsActive)
            {
                try
                {
                    foreach (var type in assembly.DefinedTypes)
                    {
                        ProcessType(type);
                    }
                }
                catch (Exception exc)
                {
                    logger.Warn(ErrorCode.Provider_AssemblyLoadError,
                        "Error searching for providers in assembly {0} -- ignoring this assembly. Error = {1}", assembly.FullName, exc);
                }
            }
        }

        private static void ProcessNewAssembly(object sender, AssemblyLoadEventArgs args)
        {
            // We do this under the lock to avoid race conditions when an assembly is added 
            // while a type manager is initializing.
            lock (managers)
            {
                // We assume that it's better to fetch and iterate through the list of types once,
                // and the list of TypeManagers many times, rather than the other way around.
                // Certainly it can't be *less* efficient to do it this way.
                foreach (var type in args.LoadedAssembly.DefinedTypes)
                {
                    foreach (var mgr in managers)
                    {
                        if (mgr.IsActive)
                        {
                            mgr.ProcessType(type);
                        }
                    }
                }
            }
        }
    }
}