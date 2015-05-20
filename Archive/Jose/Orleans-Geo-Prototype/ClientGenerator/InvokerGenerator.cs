using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.Objects;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using Microsoft.CSharp;
using Orleans;


namespace GrainClientGenerator
{
    /// <summary>
    /// Debug purpose
    /// 0: generate factory and invoker source file and dll file in memory
    /// 1: output factory.cs and invoker.cs file
    /// 2: output factory.dll file
    /// </summary>
    [Flags]
    internal enum CompileParameters
    {
        OutputNothing = 0,
        OutputSourceFile = 1,
        OutputDllFile = 2,
    }

    internal class InvokerGenerator : InvokerGeneratorBasic
    {
        #region helper methods
        /// <summary>
        /// Find all grain types in the given dll file, return a list of them
        /// </summary>
        public static List<Type> GetGrainTypes(string dllPath, out string assemblyFullName, out Assembly assembly)
        {
            List<Type> grainTypes = new List<Type>();
            AssemblyName assemblyName;
            assemblyFullName = null;
            assembly = null;
            Type[] types;

            try
            {
                assemblyName = AssemblyName.GetAssemblyName(dllPath);
                assemblyFullName = assemblyName.FullName;
                assembly = Assembly.LoadFrom(dllPath);

                types = assembly.GetTypes();
            }
            catch (Exception exc)
            {
                String loadErrorDetails = null;
                ReflectionTypeLoadException typeLoadException = exc as ReflectionTypeLoadException;
                if (typeLoadException != null)
                {
                    loadErrorDetails = string.Join<Exception>(" LoaderException=", typeLoadException.LoaderExceptions);
                }
                ConsoleText.WriteError(string.Format("Exception while inspecting assembly {0} for grain classes: {1} InnerException={2} \r\n Loader Error Details={3}", dllPath, exc, exc.InnerException, loadErrorDetails));
                throw;
            }
            foreach (Type t in types)
            {
                if (!t.IsClass || t.BaseType == null)
                    continue;

                if (t.IsAbstract)
                {
                        ConsoleText.WriteStatus("Skipping abstract grain classes: {0}", TypeUtils.GetFullName(t));
                    continue;
                }

                if (IsGrainClass(t))
                {
                    grainTypes.Add(t);
                }
            }

            return grainTypes;
        }

        /// <summary>
        /// decide whether the class is derived from GrainBase
        /// </summary>
        internal static bool IsGrainClass(Type type)
        {
            if (type.FullName == typeof(GrainBase).FullName)
                return false;

            if (typeof(GrainReference).IsAssignableFrom(type))
                return false;

            if (typeof(GrainBase).IsAssignableFrom(type))
                return true;

            if (type.GetInterfaces().Any(i => i.FullName == typeof(IGrainObserver).FullName))
                return false;

            return type.GetInterfaces().Any(i => i.FullName == typeof(IAddressable).FullName);
        }
        #endregion

        /// <summary>
        ///  This is the entry point into factory generation logic.
        ///  Here is the sequence of steps
        ///  1. Create factory assmebly. We must unload the grain assembly so that it can be overwritten in step 2.
        ///  2. Merge it with original assembly. Because of problems with .pdb files being locked we create new merged assembly in a subfolder.
        ///  3. Overwrite the original grain assembly with the merged assembly.
        /// </summary>
        /// <param name="inputLib">Assembly to read.</param>
        /// <param name="activationLib">Assembly to generate.</param>
        /// <param name="sourcesDir">Directory where sources should be generated.</param>
        /// <param name="signingKey">Key file.</param>
        /// <param name="referencedAssemblyPath">Array to reference assemblies</param>
        /// <param name="defines">List of #define statements to include in generated code</param>
        /// <param name="shouldMerge"> Should the file be merge back.</param>
        internal static void GenerateFactoryAssembly(FileInfo inputLib, FileInfo activationLib, string sourcesDir, FileInfo signingKey, List<string> referencedAssemblyPath, List<string> defines, bool shouldMerge)
        {

            bool isfactoryGenerated;
            AppDomain appDomain = null;
            // STEP 1. generate assemblies
            try
            {
                // Create AppDomain.
                AppDomainSetup appDomainSetup = new AppDomainSetup();
                appDomainSetup.ApplicationBase = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                appDomainSetup.DisallowBindingRedirects = false;
                appDomainSetup.ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile;
                appDomain = AppDomain.CreateDomain("Invoker Generation Domain", null, appDomainSetup);
                ReferenceResolver refResolver = new ReferenceResolver(referencedAssemblyPath);
                appDomain.AssemblyResolve += refResolver.ResolveAssembly;

                // Create an instance 
                InvokerGenerator invokerGenerator = (InvokerGenerator)appDomain.CreateInstanceAndUnwrap(
                    Assembly.GetExecutingAssembly().FullName,
                    typeof(InvokerGenerator).FullName);
                
                // Call a method 
                isfactoryGenerated = invokerGenerator.GenerateFactoryAssembly(inputLib.FullName,
                    activationLib.FullName,
                    sourcesDir,
                    signingKey,
                    referencedAssemblyPath,
                    defines);
            }
            catch (Exception ex)
            {
                ConsoleText.WriteError(string.Format("ERROR -- Factory code-gen FAILED -- Exception caught -- {0}", ex));
                throw;
            }
            finally
            {
                if (appDomain != null)
                {
                    // Unload the AppDomain
                    AppDomain.Unload(appDomain);
                }
            }

            // Merge if needed
            if (shouldMerge && isfactoryGenerated)
            {
                MergeAssemblies(inputLib, activationLib, sourcesDir, signingKey, referencedAssemblyPath);
            }
        }


        /// <summary>
        /// This is an entry point into the proxy/factory generation.
        /// </summary>
        /// <param name="dllPath">Grain assembly to read.</param>
        /// <param name="factoryAssemblyPath">Path to output factory assembly.</param>
        /// <param name="sourceOutputDirectory">Directory where sources are generated.</param>
        /// <param name="signingKey">The signing key used for signing.</param>
        /// <param name="referencedAssemblyPath">All the referenced assemblies.</param>
        /// <param name="defines">List of #define statements to include in generated code</param>
        internal bool GenerateFactoryAssembly(string dllPath, string factoryAssemblyPath, string sourceOutputDirectory, FileInfo signingKey, List<string> referencedAssemblyPath, List<string> defines)
        {
            List<string> sourcesGenerated = new List<string>();
            CompileParameters flag = CompileParameters.OutputDllFile | CompileParameters.OutputSourceFile;
            ConsoleText.WriteStatus("Generating factory for {0} at {1}", dllPath, factoryAssemblyPath);
            Assembly grainAssembly;
            string assemblyFullName;
            List<Type> grainTypes = GetGrainTypes(dllPath, out assemblyFullName, out grainAssembly);

            // skip generation if ActivationIncluded is already merged.
            try
            {
                if (TypeUtils.AssemblyContainsAttribute(grainAssembly, typeof(ActivationIncludedAttribute)))
                {
                    ConsoleText.WriteStatus("Activation already present in the assembly. Skipping factory generation.");
                    return false;
                }
            }
            catch (Exception exc)
            {
                ConsoleText.WriteError("Error inspecting assembly" + grainAssembly.GetName().Name + ". Skipping factory generation.", exc);
                return false;
            }

            ReferenceResolver.AssertUniqueLoadForEachAssembly();
            try
            {
                foreach (Type grainType in grainTypes)
                {
                    referredNamespaces.Clear();
                    GrainInterfaceData grainInterfaceData = GrainInterfaceData.FromGrainClass(grainType);
                    ConsoleText.WriteLine("Generating new factory class for grain type {0} ( {1} )", grainInterfaceData.ServiceTypeName, grainType.FullName);
                    GenerateFactorySources(grainInterfaceData,
                        flag,
                        sourceOutputDirectory,
                        sourcesGenerated);
                }
                if (sourcesGenerated.Count == 0)
                {
                    return false;
                }

                using (StreamWriter writer = new StreamWriter(Path.Combine(sourceOutputDirectory, "ActivationIncludedAttribute.cs")))
                {
                    // add a ActivationIncluded attribute to mark the assembly as generated by ClientGenerator.
                    writer.WriteLine("[assembly: {0}()]", typeof(ActivationIncludedAttribute).FullName);
                }
                sourcesGenerated.Add(Path.Combine(sourceOutputDirectory, "ActivationIncludedAttribute.cs"));
                CompileSources(dllPath, flag, factoryAssemblyPath, sourcesGenerated, referencedAssemblyPath, signingKey);
            }
            catch (Exception ex) // TODO: need more robust error handling
            {
                ConsoleText.WriteError("Error loading type data for assembly " + factoryAssemblyPath, ex);
                throw;
            }

            return true;
        }

        public void GenerateFactorySources(GrainInterfaceData grainInterfaceData, CompileParameters flag, string sourceOutputDirectory, List<string> sourcesGenerated)
        {
            if (!Directory.Exists(sourceOutputDirectory))
            {
                Directory.CreateDirectory(sourceOutputDirectory);
            }
            string factoryCodeGenFile = Path.Combine(sourceOutputDirectory, grainInterfaceData.FactoryClassBaseName + ".cs");
            
            CodeCompileUnit unit = new CodeCompileUnit();

            //namespace and includes 
            ReferredNamespaceAndAssembly(typeof(GrainId));
            ReferredNamespaceAndAssembly(typeof(ObjectContext));
            ReferredNamespaceAndAssembly(typeof(Object));

            currentNamespace = grainInterfaceData.Namespace;
            CodeNamespace factoryNamespace = GetActivationNamespace(grainInterfaceData);
            unit.Namespaces.Add(factoryNamespace);

            //link to assemblies for implemented interfaces.
            foreach (Type t in grainInterfaceData.Type.GetInterfaces())
                ReferredNamespaceAndAssembly(t);

            // add imports for all referred namespaces
            foreach (var ns in referredNamespaces)
            {
                if (ns != grainInterfaceData.Namespace)
                {
                    factoryNamespace.Imports.Add(new CodeNamespaceImport(ns));
                }
            }

            using (CodeDomProvider cSharpCodeProvider = GetCSharpCodeProvider())
            {
                //Set compiler parameters
                CompilerParameters compilerParams = new CompilerParameters();

                foreach (string assembly in referredAssemblies)
                    compilerParams.ReferencedAssemblies.Add(assembly);

                if (flag.HasFlag(CompileParameters.OutputSourceFile)) //output source file
                {
                    compilerParams.GenerateInMemory = true;
                    CodeGeneratorOptions options = new CodeGeneratorOptions();
                    options.BracingStyle = "C";
                    using (StreamWriter sourceWriter = new StreamWriter(factoryCodeGenFile))
                    {
                        cSharpCodeProvider.GenerateCodeFromCompileUnit(
                                unit, sourceWriter, options);
                    }
                }
            }

            string[] sourcesToCompile = new string[] {factoryCodeGenFile };
            sourcesGenerated.AddRange(sourcesToCompile);
        }

        private CodeNamespace GetActivationNamespace(GrainInterfaceData grainInterfaceData)
        {

            CodeNamespace factoryNamespace = new CodeNamespace(grainInterfaceData.Namespace);
            factoryNamespace.Imports.Add(new CodeNamespaceImport("System"));
            referredNamespaces.Add("System");
            factoryNamespace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            referredNamespaces.Add("System.Collections.Generic");
            factoryNamespace.Imports.Add(new CodeNamespaceImport("Orleans"));
            referredNamespaces.Add("Orleans");
            
            var genericTypeParams = grainInterfaceData.GenericTypeParams;

            if (typeof(GrainBase).IsAssignableFrom(grainInterfaceData.Type))
            {
                // create state classes & interfaces
                bool hasStateClasses;
                foreach (var code in GetStateClasses(grainInterfaceData,
                    type => ReferredNamespaceAndAssembly(type),
                        grainInterfaceData.StateClassBaseName,
                        grainInterfaceData.StateClassName,
                        out hasStateClasses))
                {
                    factoryNamespace.Types.Add(code);
                }
            }
            return factoryNamespace;
        }

        private void CompileSources(string dllPath, CompileParameters flag, string cachedfactoryAssemblyPath, List<string> sourcesGenerated, List<string> referencedAssemblyPath, FileInfo signingKey)
        {
            using (CodeDomProvider cSharpCodeProvider = GetCSharpCodeProvider())
            {
                //Set compiler parameters
                CompilerParameters compilerParams = new CompilerParameters();

                Assembly factoryAssembly;
                if (flag.HasFlag(CompileParameters.OutputDllFile)) //output dll
                {
                    // Save the assembly as a physical file.
                    compilerParams.GenerateInMemory = false;
                    compilerParams.GenerateExecutable = false;
                    compilerParams.IncludeDebugInformation = true;
                    compilerParams.OutputAssembly = cachedfactoryAssemblyPath;
                }

                HashSet<string> libDirectories = new HashSet<string>();
                libDirectories.Add(Path.GetDirectoryName(dllPath));
                libDirectories.Add(AppDomain.CurrentDomain.SetupInformation.ApplicationBase);
                //libDirectories.Add(Path.GetDirectoryName(typeof(DbContext).Assembly.Location));
                libDirectories.Add(Path.GetDirectoryName(typeof(ObjectContext).Assembly.Location));

                if (null != referencedAssemblyPath)
                {
                    Dictionary<string, string> referredAssemblies = new Dictionary<string, string>();
                    foreach (string defaultAsm in compilerParams.ReferencedAssemblies)
                    {
                        referredAssemblies.Add(Path.GetFileName(defaultAsm), defaultAsm);
                    }
                    //referredAssemblies.Add(Path.GetFileName(typeof(DbContext).Assembly.Location), typeof(DbContext).Assembly.Location);
                    referredAssemblies.Add(Path.GetFileName(typeof(ObjectContext).Assembly.Location), typeof(ObjectContext).Assembly.Location);
                    foreach (string refAssembly in referencedAssemblyPath)
                    {
                        string libName = Path.GetFileName(refAssembly);
                        if (!referredAssemblies.ContainsKey(libName))
                        {
                            referredAssemblies.Add(libName, refAssembly);
                        }
                    }
                    referredAssemblies.Remove("mscorlib.dll"); // csc doesn't allow referencing same dll twice.
                    foreach (string refAssemblyName in referredAssemblies.Keys)
                    {
                        string assyPath = referredAssemblies[refAssemblyName];
                        compilerParams.ReferencedAssemblies.Add(assyPath);
                        string libPath = Path.GetDirectoryName(assyPath);
                        if (!Directory.Exists(libPath))
                        {
                            ConsoleText.WriteStatus("Skipping non existant lib directory {0}", libPath);
                        }
                        else if (!libDirectories.Contains(libPath))
                        {
                            libDirectories.Add(libPath);
                        }
                    }
                }
                foreach (string dir in libDirectories)
                {
                    ConsoleText.WriteLine("Added lib directory {0}", dir);
                    compilerParams.CompilerOptions += String.Format("/lib:\"{0}\" ", dir);
                }

                if (null != signingKey)
                {
                    if (File.Exists(signingKey.FullName))
                    {
                        compilerParams.CompilerOptions += String.Format(@" /keyfile:""{0}""", signingKey.FullName);
                    }
                }

                //compilerParams.TreatWarningsAsErrors = true;
                //Compile
                ConsoleText.WriteLine("Compiling factory for assembly {0} \n with CompilerOptions= {1} \n from Sources= {2}", 
                    dllPath, compilerParams.CompilerOptions, string.Join(",", sourcesGenerated));
                CompilerResults results = cSharpCodeProvider.CompileAssemblyFromFile(compilerParams, sourcesGenerated.ToArray());

                //Check compile errors
                if (results.Errors.Count > 0)
                {
                    string errorString = string.Empty;
                    foreach (CompilerError error in results.Errors)
                    {
                        //ConsoleText.WriteError(string.Format("Error compiling factory for assembly {0} : {1} at line {2} in file {3}", dllPath, error.ErrorText, error.Line, error.FileName));
                        errorString += String.Format("{0} line {1}: {2}\n", error.FileName, error.Line, error.ErrorText);
                    }
                    throw new OrleansException(String.Format("Fail to generate factory for {0} because can not compile factory in CodeDom:\n", dllPath) + errorString);
                }
                ConsoleText.WriteLine("Generated factory assembly for {0}", dllPath);
                factoryAssembly = results.CompiledAssembly;
            }
        }

        internal static CodeDomProvider GetCSharpCodeProvider(bool debug = false)
        {
            var providerOptions = new Dictionary<string, string>()
            {
                { "CompilerVersion", "v4.0" }
            };
            if (debug)
            {
                providerOptions.Add("debug", "full");
            }
            return new CSharpCodeProvider(providerOptions);
        }

        internal static bool IsInitOnly(PropertyInfo info)
        {
            return (typeof(IGrainState).IsAssignableFrom(info.DeclaringType) && !info.CanWrite) ||
                typeof(IAddressable).IsAssignableFrom(info.PropertyType);
        }

        private static string GetAccessModifier(MethodAttributes attributes)
        {
            return (attributes & MethodAttributes.Public) == MethodAttributes.Public ? "public"
                : (attributes & MethodAttributes.Family) == MethodAttributes.Family ? "protected"
                : (attributes & MethodAttributes.Assembly) == MethodAttributes.Assembly ? "internal"
                : (attributes & MethodAttributes.Private) == MethodAttributes.Private ? "private"
                : "";
        }

        private static void AddColumn(StringBuilder snippet, string propertyName, bool unique)
        {
            if (unique)
            {
                snippet.AppendFormat(@"
                entity.HasKey(c => c.{0});", propertyName);
            }
            snippet.AppendFormat(@"
                entity.Property(c => c.{0}){1};", propertyName, unique ? ".HasDatabaseGeneratedOption(System.ComponentModel.DataAnnotations.DatabaseGeneratedOption.None)" : "");
        }

        public static bool IsEntityFrameworkIndexType(Type type)
        {
            // based on the list in http://msdn.microsoft.com/en-us/library/gg696156(v=VS.103).aspx
            // todo: optimize
            return typeof(int).Equals(type) ||
                typeof(uint).Equals(type) ||
                typeof(long).Equals(type) ||
                typeof(decimal).Equals(type) ||
                typeof(float).Equals(type) ||
                typeof(byte[]).Equals(type) ||
                typeof(DateTime).Equals(type) ||
                typeof(DateTimeOffset).Equals(type) ||
                typeof(int?).Equals(type) ||
                typeof(uint?).Equals(type) ||
                typeof(long?).Equals(type) ||
                typeof(decimal?).Equals(type) ||
                typeof(float?).Equals(type) ||
                typeof(DateTime?).Equals(type) ||
                typeof(DateTimeOffset?).Equals(type) ||
                typeof(TimeSpan?).Equals(type) ||
                typeof(TimeSpan).Equals(type) ||
                typeof(string).Equals(type);
        }

        public string GetPromptTypeName(Type type)
        {
            return typeof(AsyncCompletion).IsAssignableFrom(type)
                       ? (typeof(AsyncValue<>).IsAssignableFrom(type.GetGenericTypeDefinition())
                              ? GetGenericTypeName(type.GetGenericArguments()[0])
                              : "void")
                       : GetGenericTypeName(type);
        }

        public Type GetPromptType(Type type)
        {
            return typeof (AsyncCompletion).IsAssignableFrom(type)
                       ? (typeof (AsyncValue<>).IsAssignableFrom(type.GetGenericTypeDefinition())
                              ? type.GetGenericArguments()[0]
                              : typeof(void))
                       : type;
        }
    }
}
