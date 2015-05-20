using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Orleans.Serialization;

namespace Orleans.Runtime
{
    internal class GrainAssemblyLoader
    {
        private readonly Dictionary<string, Assembly> grainAssemblies;
        private readonly Dictionary<AssemblyName, string> loadedAssemblies;
        private readonly Logger logger;
        private readonly Dictionary<string, string> applicationAssemblies;
        private readonly Dictionary<string, GrainTypeData> installedGrainTypes;
        private bool grainAssembliesLoaded;
        private static GrainAssemblyLoader instance;

        // binaries to skip while looking for grain assemblies
        private static readonly string[] SkipGrainSearchInTheseBinaries = new[] { 
            // Note: Names MUST be in all lowercase
            "appdomainhost.exe",
            "bungie.blf.dll",
            "clientgenerator.exe",
            "countercontrol.exe",
            "fsharp.core.dll",
            "ilmerge.exe",
            "microsoft.windowsazure.configuration.dll",
            "microsoft.windowsazure.diagnostics.dll",
            "microsoft.windowsazure.diagnostics.storageutility.dll",
            "microsoft.windowsazure.serviceruntime.dll",
            "microsoft.windowsazure.storage.dll",
            "microsoft.windowsazure.storageclient.dll",
            "msshrtmi.dll",
            "orleansazureutils.dll", 
            "orleanshost.exe",
            "orleansmanager.exe",
            "orleanssilohost.dll",
            "system.web.mvc.dll",
            "xceed.compression.dll",
            "xceed.compression.formats.dll",
        };

        public static GrainAssemblyLoader Instance
        {
            get { return instance ?? (instance = new GrainAssemblyLoader()); }
        }

        private GrainAssemblyLoader()
        {
            grainAssemblies = new Dictionary<string, Assembly>();
            loadedAssemblies = new Dictionary<AssemblyName, string>();
            applicationAssemblies = new Dictionary<string, string>();
            installedGrainTypes = new Dictionary<string, GrainTypeData>();
            logger = Logger.GetLogger("GrainAssemblyLoader");
            string currentDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            FindApplicationAssemblies(currentDirectory, false);
            FindApplicationAssemblies(Path.Combine(currentDirectory, "Applications"), true);
            ProcessLoadedAssemblies();
        }

        private void FindApplicationAssemblies(string dir, bool includeSubdirectories)
        {
            if (Directory.Exists(dir))
            {
                foreach (var entry in Directory.EnumerateFiles(dir, "*.dll"))
                {
                    // [todo] the application of Path.Combine in the following line of code will
                    // *not* produce the desired result if 'dir' is a relative path.
                    applicationAssemblies.Add(entry.Substring(0, entry.LastIndexOf('.')), Path.Combine(dir, entry));
                }
                if (includeSubdirectories)
                {
                    foreach (var folder in Directory.EnumerateDirectories(dir))
                    {
                        FindApplicationAssemblies(folder, true);
                    }
                }
            }
        }

        public void Clear()
        {
            grainAssemblies.Clear();
            loadedAssemblies.Clear();
        }

        /// <summary>
        /// Because we want to be able to load grain assemblies from arbitrary folders while the standard Assembly.Load method is limited to subfolders of the main app directory, 
        /// we have to manage assembly loading process ourselves by setting CurrentDomain_AssemblyResolve as a handler for CurrentDomain.AssemblyResolve.
        /// We load grain assemblies into the app domain as part of scanning for grain classes and keep references to grain assmeblies in the grainAssemblies dictionary.
        /// CurrentDomain_AssemblyResolve simply looks up a requested assmelby in the dictionary.
        /// In the future we should consider using separate app domains for different apps and perfoming the scanning process in a temporary app domain,
        /// which we should unload at the end of the scanning process in order to not keep all grain assemblies loaded all the time.
        /// Currently the path for grain assemblies is hardcoded to [OrleansPath]\Applications. We need to move folder path to the config file eventually.
        /// </summary>
        public Dictionary<string, GrainTypeData> LoadGrainAssemblies()
        {
            if (grainAssembliesLoaded)
            {
                return installedGrainTypes;
            }

            AppDomain.CurrentDomain.AssemblyResolve += CurrentDomain_AssemblyResolve;
            try
            {
                string basePath = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                string appPath = basePath + "\\Applications"; // TODO: Need to move directory path to the config file

                logger.Info(ErrorCode.Loader_LoadingFromDir, "Searching for grains in system folder {0}", basePath);
                LoadGrainAssemblies(basePath, false);

                if (Directory.Exists(appPath))
                {
                    logger.Info(ErrorCode.Loader_LoadingFromDir, "Searching for grains in app folder {0}", appPath);
                    LoadGrainAssemblies(appPath, true);
                }
                //else
                //{
                //    logger.Info(ErrorCode.Loader_DirNotFound, "Application directory \"{0}\" not found. Skipping loading of application grain assemblies.", appPath);
                //}

                // Load serialization info for all loaded assemblies
                AppDomain.CurrentDomain.AssemblyLoad += NewAssemblyHandler;
                foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                {
                    if (!assembly.ReflectionOnly)
                    {
                        if (logger.IsVerbose3) logger.Verbose3(ErrorCode.Loader_LoadingSerInfo, "Loading serialization info for grain assembly {0}", AssemblyLoaderUtils.GetLocationSafe(assembly));
                        SerializationManager.FindSerializationInfo(assembly);
                    }
                }

                grainAssembliesLoaded = true;
                if (logger.IsVerbose) logger.Verbose("SerializationManager Registered type count is now {0}", SerializationManager.RegisteredTypesCount);

                var sb = new StringBuilder();
                sb.AppendLine(String.Format("Loaded grain type summary for {0} types: ", installedGrainTypes.Count));
                foreach (var grainType in installedGrainTypes.Values.OrderBy(gtd => gtd.Type.Name))
                {
                    // Skip system targets and Orleans grains
                    var assemblyName = grainType.Type.Assembly.FullName.Split(',')[0];
                    if (!typeof(ISystemTarget).IsAssignableFrom(grainType.Type)) // && !assemblyName.Equals("orleansruntime", StringComparison.CurrentCultureIgnoreCase))
                    {
                        int grainClassTypeCode = GrainClientGenerator.GrainInterfaceData.GetGrainClassTypeCode(grainType.Type);
                        sb.AppendFormat("Grain class {0} [{1} (0x{2})] from {3}.dll implementing interfaces: ", 
                            TypeUtils.GetTemplatedName(grainType.Type),
                            grainClassTypeCode,
                            grainClassTypeCode.ToString("X"),
                            assemblyName);
                        var first = true;
                        foreach (var iface in grainType.ServiceInterfaceTypes)
                        {
                            if (!first)
                            {
                                sb.Append(", ");
                            }
                            sb.Append(iface.Namespace).Append(".").Append(TypeUtils.GetTemplatedName(iface));
                            if (GrainClientGenerator.GrainInterfaceData.IsGrainType(iface))
                            {
                                int ifaceTypeCode = GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(iface);
                                sb.AppendFormat(" [{0} (0x{1})]", ifaceTypeCode, ifaceTypeCode.ToString("X"));
                            }
                            first = false;
                        }
                        sb.AppendLine();
                    }
                }
                logger.LogWithoutBulkingAndTruncating(OrleansLogger.Severity.Info, ErrorCode.Loader_GrainTypeFullList, sb.ToString());

                return installedGrainTypes;
            }
            finally
            {
                AppDomain.CurrentDomain.AssemblyResolve -= CurrentDomain_AssemblyResolve;
            }
        }

        private static void NewAssemblyHandler(object sender, AssemblyLoadEventArgs args)
        {
            SerializationManager.FindSerializationInfo(args.LoadedAssembly);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2001:AvoidCallingProblematicMethods", MessageId = "System.Reflection.Assembly.LoadFile")]
        private Assembly CurrentDomain_AssemblyResolve(object sender, ResolveEventArgs args)
        {
            // See if the assembly is already loaded
            if (grainAssemblies.ContainsKey(args.Name))
            {
                if (logger.IsVerbose3) logger.Verbose3(ErrorCode.Loader_AssemblyLookupResolved, "Resolved lookup for assembly {0}", args.Name);
                return grainAssemblies[args.Name];
            }

            // See if we know where the assembly is to be found
            string path;
            if (applicationAssemblies.TryGetValue(args.Name, out path))
            {
                // It is okay to use LoadFile here because we are loading application assemblies deployed to the specific directory.
                // Such application assemblies should not be deployed somewhere else, e.g. GAC, so this is safe.
                return Assembly.LoadFile(path);
            }

            if (!args.Name.EndsWith(".resources", StringComparison.OrdinalIgnoreCase))
            {
                StackTrace stackTrace = new StackTrace(true);
                logger.Warn(ErrorCode.Loader_AssemblyLookupFailed, "Failed lookup for assembly {0}; StackTrace follows:\n{1}", args.Name, stackTrace.ToString());
            }
            return null;
        }

        /// <summary>
        /// For internal use only.
        /// </summary>
        /// <param name="assembly"></param>
        private void RecordAssembly(Assembly assembly)
        {
            grainAssemblies[assembly.FullName] = assembly;
            loadedAssemblies[assembly.GetName()] = AssemblyLoaderUtils.GetLocationSafe(assembly);
        }

        internal void ProcessLoadedAssemblies()
        {
            foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
            {
                ProcessAssembly(assembly);
            }
        }

        private void LoadGrainAssemblies(string path, bool includeSubfolders)
        {
            if (Directory.Exists(path))
            {
                LoadGrainAssembliesImpl(path);

                if (includeSubfolders)
                {
                    string[] subfolders = Directory.GetDirectories(path, "*");
                    foreach (string subfolder in subfolders)
                    {
                        logger.Info(ErrorCode.Loader_LoadingFromDir, "Searching for grains in app folder {0}", subfolder);
                        LoadGrainAssemblies(subfolder, true);
                    }
                }
            }
            else
            {
                logger.Warn(ErrorCode.Loader_DirNotFound, "Directory \"{0}\" not found. Skipping loading grain assemblies from it.", path);
            }
        }

        private void LoadGrainAssembliesImpl(string folder)
        {
            foreach (string file in Directory.GetFiles(folder))
            {
                var ext = Path.GetExtension(file).ToLowerInvariant();
                if (ext != ".dll")
                    continue;

                if (IsOrleansBinary(file))
                    continue;

                logger.Info(ErrorCode.Loader_FoundBinary, "Found assembly \"{0}\".", file);

                AssemblyName displayName;
                try
                {
                    displayName = AssemblyName.GetAssemblyName(file);
                }
                catch (Exception)
                {
                    logger.Warn(ErrorCode.Loader_SkippingFile, "Skipping file {0} because assembly manifest is not available", file);
                    continue;
                }

                if (loadedAssemblies.ContainsKey(displayName))
                {
                    if (logger.IsVerbose)
                    {
                        logger.Verbose(ErrorCode.Loader_SkippingFile, "Skipping file {0} because the assembly it contains, {1} is already loaded from {2}",
                            file, displayName.FullName, loadedAssemblies[displayName]);
                    }
                    continue;
                }

                var assembly = Assembly.Load(displayName);
                ProcessAssembly(assembly);

            }
        }

        private void ProcessAssembly(Assembly assembly)
        {
            if (assembly.IsDynamic)
            {
                if (logger.IsVerbose) logger.Verbose(ErrorCode.Loader_SkippingDynamicAssembly, "Skipping dynamic assembly {0}", assembly.FullName);
                return;
            }

            if (grainAssemblies.ContainsKey(assembly.FullName))
            {
                string newLocation = AssemblyLoaderUtils.GetLocationSafe(assembly);
                string oldLocation = AssemblyLoaderUtils.GetLocationSafe(grainAssemblies[assembly.FullName]);
                if (!newLocation.Equals(oldLocation))
                {
                    logger.Info(ErrorCode.Loader_SkippingFile,
                                "Skipping file {0} because its assembly, {1}, has already been processed in file {2}",
                                newLocation, assembly.FullName, oldLocation);
                }
                return;
            }
            RecordAssembly(assembly);

            // Load serialization info from this assembly
            SerializationManager.FindSerializationInfo(assembly);

            if (logger.IsVerbose) logger.Verbose(ErrorCode.Loader_LoadingFromFile, "Searching for grains in file {0}", grainAssemblies[assembly.FullName]);
            bool containsActivation;
            var types = GetLoadedGrainTypes(assembly, logger, out containsActivation);

            Assembly factoryAssembly;
            if (containsActivation)
            {
                factoryAssembly = assembly;
            }
            else
            {
                factoryAssembly = AssemblyLoaderUtils.GetActivationAssembly(assembly);
                if (null == factoryAssembly)
                {
                    logger.Warn(ErrorCode.Loader_NotGrainAssembly, String.Format("Assembly {0} does not contain generated code for grain classes. Skipping assembly", assembly.CodeBase));
                    return;
                }
            }
            Dictionary<string, GrainClientGenerator.GrainInterfaceData> mapping = FromGrainAssembly(factoryAssembly);

            if (types.Count > 0)
            {
             
                foreach (Type grainType in types)
                {
                    string parameterizedName = grainType.Namespace + "." + TypeUtils.GetParameterizedTemplateName(grainType);
                    try
                    {
                        if (logger.IsVerbose2) logger.Verbose2(ErrorCode.Loader_LoadingGrainType, "Loading grain type = {0}", grainType.FullName);

                        var className = TypeUtils.GetFullName(grainType);
                        if (installedGrainTypes.ContainsKey(className))
                            continue;

                        GrainClientGenerator.GrainInterfaceData grainGrainInterfaceData;
                        if (typeof(ISystemTarget).IsAssignableFrom(grainType)
                            || typeof(SystemTarget).IsAssignableFrom(grainType))
                        {
                            grainGrainInterfaceData = mapping[parameterizedName] = GrainClientGenerator.GrainInterfaceData.FromGrainClass(grainType);
                        }
                        else
                        {
                            grainGrainInterfaceData = mapping[parameterizedName];
                            grainGrainInterfaceData.SetType(grainType);
                        }
                        var typeData = GetTypeData(grainGrainInterfaceData, factoryAssembly);
                        if (typeData != null)
                        {
                            ValidateGrainType(assembly.Location, grainType, logger);

                            // TODO: need more than class name (assembly, version, etc)

                            installedGrainTypes.Add(className, typeData);
                        }

                    }
                    catch (Exception ex) // TODO: need more robust error handling
                    {
                        logger.Error(ErrorCode.Loader_TypeLoadError, String.Format("Error loading type data for {0}", grainType.FullName), ex);
                    }
                }

               RecordAssembly(factoryAssembly);
            }
        }



        private static Dictionary<string, GrainClientGenerator.GrainInterfaceData> FromGrainAssembly(Assembly factoryAssembly)
        {
            Dictionary<string, GrainClientGenerator.GrainInterfaceData> interfaces = new Dictionary<string, GrainClientGenerator.GrainInterfaceData>();

            foreach (Type t in factoryAssembly.GetTypes())
            {
                if (t.GetCustomAttributes(typeof(GrainReferenceAttribute), true).Length > 0)
                {
                    GrainReferenceAttribute attr = (GrainReferenceAttribute)t.GetCustomAttributes(typeof(GrainReferenceAttribute), true)[0];
                    GrainClientGenerator.GrainInterfaceData grainInterfaceData;
                    if (!interfaces.ContainsKey(attr.ForGrainType))
                    {
                        interfaces[attr.ForGrainType] = new GrainClientGenerator.GrainInterfaceData();
                    }
                    grainInterfaceData = interfaces[attr.ForGrainType];
                    grainInterfaceData.ReferenceClassBaseName = TypeUtils.GetSimpleTypeName(t);
                }
                if (t.GetCustomAttributes(typeof(MethodInvokerAttribute), true).Length > 0)
                {
                    MethodInvokerAttribute attr = (MethodInvokerAttribute)t.GetCustomAttributes(typeof(MethodInvokerAttribute), true)[0];
                    GrainClientGenerator.GrainInterfaceData grainInterfaceData;
                    if (!interfaces.ContainsKey(attr.ForGrainType))
                    {
                        interfaces[attr.ForGrainType] = new GrainClientGenerator.GrainInterfaceData();
                    }
                    grainInterfaceData = interfaces[attr.ForGrainType];
                    grainInterfaceData.InvokerClassBaseName = TypeUtils.GetSimpleTypeName(t);
                    if (GrainTypeManager.Instance != null) //only for testing we may run this code without a GrainTypeManager
                        GrainTypeManager.Instance.AddInvokerClass(attr.InterfaceId, t);
                }
                if (t.GetCustomAttributes(typeof(GrainStateAttribute), true).Length > 0)
                {
                    GrainStateAttribute attr = (GrainStateAttribute)t.GetCustomAttributes(typeof(GrainStateAttribute), true)[0];
                    GrainClientGenerator.GrainInterfaceData grainInterfaceData;
                    if (!interfaces.ContainsKey(attr.ForGrainType))
                    {
                        interfaces[attr.ForGrainType] = new GrainClientGenerator.GrainInterfaceData();
                    }
                    grainInterfaceData = interfaces[attr.ForGrainType];
                    grainInterfaceData.StateClassBaseName = TypeUtils.GetSimpleTypeName(t);
                    grainInterfaceData.StateObjectType = t;
                }
            }
            return interfaces;
        }
        /// <summary>
        /// Checks if this is one of our own binaries. TODO: the list has to be manually updated. Is there a better way of maintaining it?
        /// </summary>
        private static bool IsOrleansBinary(string filePath)
        {
            string lcFileName = Path.GetFileName(filePath).ToLowerInvariant();
            return SkipGrainSearchInTheseBinaries.Contains(lcFileName);
        }

        private static bool ValidateGrainType(string dllPath, Type grainType, Logger logger)
        {
#if TODO // new validation criteria
            
#endif
            return true;
        }

        /// <summary>
        /// Find all grain types in the given dll file, return a list of them
        /// </summary>
        private static List<Type> GetLoadedGrainTypes(Assembly assembly, Logger logger, out bool containsActivation)
        {
            var grainTypes = new List<Type>();
            Type[] types;
            containsActivation = false;
            try
            {
                types = assembly.GetTypes();
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.Loader_AssemblyInspectError, String.Format("Exception while inspecting assembly {0} for grain classes.",
                    AssemblyLoaderUtils.GetLocationSafe(assembly)), exc);
                return grainTypes;
            }

            foreach (Type t in types)
            {
                if (!t.IsClass || t.BaseType == null)
                    continue;

                // must skip types that are from the code we generated. They are not user defined grain types.
                if (t.GetCustomAttributes(typeof(GeneratedAttribute), false).Length > 0)
                {
                    containsActivation = true;
                    continue;
                }

                if (IsGrainClass(t))
                {
                    if (t.IsAbstract)
                    {
                        if(!t.Equals(typeof(SystemTarget)))
                            logger.Info(ErrorCode.Loader_IgnoreAbstractGrainClass, "Ignoring abstract grain class {0}. Orleans only loads and instantiates non-abstract grain classes.", TypeUtils.GetFullName(t));
                        continue;
                    }

                    logger.Verbose3("Found grain class {0}", TypeUtils.GetFullName(t));
                    grainTypes.Add(t);
                }
            }

            return grainTypes;
        }

        /// <summary>
        /// decide whether the class is derived from GrainBase
        /// </summary>
        private static bool IsGrainClass(Type type)
        {
            if (type == typeof(GrainBase) || type == typeof(GrainBase<>))
                return false;

            if (typeof(GrainReference).IsAssignableFrom(type))
                return false;

            if (typeof(GrainBase).IsAssignableFrom(type) || typeof(SystemTarget).IsAssignableFrom(type))
                return true;

            return false;
        }

        /// <summary>
        /// Get type data for the given grain type
        /// </summary>
        private static GrainTypeData GetTypeData(GrainClientGenerator.GrainInterfaceData grainInterfaceData, Assembly factoryAssembly)
        {
            if (grainInterfaceData.Type.IsGenericTypeDefinition)
                return new GenericGrainTypeData(grainInterfaceData.Type, grainInterfaceData.StateObjectType);

            return new GrainTypeData(grainInterfaceData.Type, grainInterfaceData.StateObjectType);
        }
    }
}
