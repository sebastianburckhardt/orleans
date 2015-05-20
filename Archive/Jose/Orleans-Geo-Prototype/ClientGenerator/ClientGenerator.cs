using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using GrainClientGenerator.Serialization;
using Orleans;

namespace GrainClientGenerator
{
    [Serializable]
    internal class GrainClientGeneratorFlags
    {
        internal static bool Verbose = true;
        internal static bool FailOnPathNotFound = false;
    }

    /// <summary>
    /// Generates factory, grain reference, and invoker classes for grain interfaces.
    /// Generates state object classes for grain inplementation classes.
    /// </summary>
    public class GrainClientGenerator : MarshalByRefObject
    {
        private static readonly int[] suppressCompilerWarnings =
        {
             162, // CS0162 - Unreachable code detected.
             219, // CS0219 - The variable 'variable' is assigned but its value is never used.
             693, // CS0693 - Type parameter 'type parameter' has the same name as the type parameter from outer type 'type'
            1591, // CS1591 - Missing XML comment for publicly visible type or member 'Type_or_Member'
            1998, // CS1998 - This async method lacks 'await' operators and will run synchronously
        };

        /// <summary>
        /// Generates one GrainReference class for each Grain Type in the inputLib file 
        /// and output one GrainClient.dll under outputLib directory
        /// </summary>
        private static void CreateGrainClientAssembly(
            bool compileFromSources, 
            FileInfo inputLib, 
            FileInfo outputLib, 
            string sourcesDir, 
            FileInfo signingKey, 
            List<string> referencedAssemblies, 
            List<string> sourcesFiles, 
            List<string> defines, 
            bool shouldMerge,
            string codeGenFile)
        {
            AppDomain appDomain = null;
            try
            {
                // Create AppDomain.
                AppDomainSetup appDomainSetup = new AppDomainSetup();
                appDomainSetup.ApplicationBase = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                appDomainSetup.DisallowBindingRedirects = false;
                appDomainSetup.ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile;
                appDomain = AppDomain.CreateDomain("Orleans-CodeGen Domain", null, appDomainSetup);

                // Set up assembly resolver
                ReferenceResolver refResolver = new ReferenceResolver(referencedAssemblies);
                appDomain.AssemblyResolve += refResolver.ResolveAssembly;

                // Create an instance 
                GrainClientGenerator generator = (GrainClientGenerator)appDomain.CreateInstanceAndUnwrap(
                    Assembly.GetExecutingAssembly().FullName,
                    typeof(GrainClientGenerator).FullName);

                 // Call a method 
                bool success = generator.CreateGrainClient(inputLib,
                   outputLib,
                   sourcesDir,
                   signingKey,
                   referencedAssemblies,
                   defines,
                   compileFromSources,
                   codeGenFile);

            }
            catch (Exception ex)
            {
                ConsoleText.WriteError("ERROR -- Client code-gen FAILED -- Exception caught -- ", ex);
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
        }

        

        /// <summary>
        /// generate one GrainReference class for each Grain Type in the inputLib file 
        /// and output one GrainClient.dll under outputLib directory
        /// </summary>
        private bool CreateGrainClient(FileInfo inputLib, FileInfo outputLib, string sourcesDir, FileInfo keyFile, List<string> referencedAssemblyPaths, List<string> defines, bool compileFromSources, string codeGenFile)
        {
            SerializerGenerationManager.Init();
            PlacementStrategy.Initialize();

            HashSet<string> referredAssemblies = new HashSet<string>();
            HashSet<string> referredDirectories = new HashSet<string>();

            Dictionary<string, GrainNamespace> namespaceDictionary = new Dictionary<string, GrainNamespace>();

            // Load input assembly 
            AssemblyName assemblyName = AssemblyName.GetAssemblyName(inputLib.FullName);
            Assembly grainAssembly = (Path.GetFileName(inputLib.FullName) != "Orleans.dll") ?
                                            Assembly.LoadFrom(inputLib.FullName) :
                                            Assembly.Load(assemblyName);  // special case Orleans.dll because there is a circular dependency.
            
            // Process input assembly
            ProcessInputGrainAssembly(grainAssembly, namespaceDictionary, assemblyName.Name);

            // Prepare compiler parameters
            CompilerParameters compilerParams = new CompilerParameters();
            // Base search path for referenced assemblies should start with directory containing the input assembly then location of output assembly

            var inDir = Path.GetDirectoryName(grainAssembly.Location);
            ConsoleText.WriteLine("Added input directory as lib directory {0}", inDir);
            compilerParams.CompilerOptions += String.Format("/lib:\"{0}\" ", inDir);
            string outDir = Path.GetDirectoryName(outputLib.FullName);
            ConsoleText.WriteLine("Added output directory as lib directory {0}", outDir);
            compilerParams.CompilerOptions += String.Format("/lib:\"{0}\" ", outDir);

            // Add references
            string assyName = assemblyName.Name;
            if (!referredAssemblies.Contains(assyName))
            {
                string libPath = Path.GetDirectoryName(inputLib.FullName);
                ConsoleText.WriteLine("Added directory containing referenced assembly {0} as lib directory {1}", assyName, libPath);
                compilerParams.CompilerOptions += String.Format("/lib:\"{0}\" ", libPath);
                referredAssemblies.Add(assyName);
                referredDirectories.Add(libPath);
            }
            if (referencedAssemblyPaths != null && referencedAssemblyPaths.Count > 0)
            {
                foreach (string path in referencedAssemblyPaths)
                {
                    string libFileName = Path.GetFileNameWithoutExtension(path);
                    string libPath = Path.GetDirectoryName(path);
                    // special case Orleans.dll because there is a circular dependency and we need unique type definitions.
                    if (libFileName == "Orleans.dll") continue;
                    try
                    {
                        if (!referredAssemblies.Contains(libFileName))
                        {
                            referredAssemblies.Add(libFileName);
                            if (!referredDirectories.Contains(libPath))
                            {
                                ConsoleText.WriteLine("Added directory containing library {0} as lib directory {1}", libFileName, libPath);
                                compilerParams.CompilerOptions += String.Format("/lib:\"{0}\" ", libPath);
                                referredDirectories.Add(libPath);
                            }
                        }
                    }
                    catch (Exception)
                    {
                        ConsoleText.WriteError(string.Format("Unable to locate referenced grain assembly {0}", libFileName));
                        return false;
                    }

                }
            }

            if (namespaceDictionary.Keys.Count == 0)
            {
                throw new OrleansException(string.Format("This {0} does not contain any public and non-abstract grain class\n", inputLib));
            }
            // Create sources directory
            if(!Directory.Exists(sourcesDir))
            {
                Directory.CreateDirectory(sourcesDir);
            }

            // Generate source
            string outputFileName = Path.Combine(sourcesDir, Path.GetFileNameWithoutExtension(inputLib.Name) + ".codegen.cs");
            ConsoleText.WriteStatus("Orleans-CodeGen - Generating file {0}", outputFileName);
            using (StreamWriter sourceWriter = new StreamWriter(outputFileName))
            {
                foreach (GrainNamespace grainNamespace in namespaceDictionary.Values)
                {
                    OutputReferenceSourceFile(grainNamespace, sourceWriter);
                }
            }
            ConsoleText.WriteStatus("Orleans-CodeGen - Generated file written {0}", outputFileName);
            // Post process
            ConsoleText.WriteStatus("Orleans-CodeGen - Post-processing file {0}", outputFileName);
            PostProcessSourceFiles(outputFileName);
            ConsoleText.WriteStatus("Orleans-CodeGen - Updating IntelliSense file {0} -> {1}", outputFileName, codeGenFile);
            UpdateIntellisenseFile(codeGenFile, outputFileName);
            
            // Finally  compile the sources to an assembly
            if (!compileFromSources)
            {
                CreateClientAssembly(namespaceDictionary.Values.ToArray(), referredAssemblies.ToList(), compilerParams, outputLib, keyFile, defines, outputFileName);
            }
            return true;
        }

        private void DisableWarnings(StreamWriter sourceWriter, int[] warnings)
        {
            foreach (int warningNum in warnings)
            {
                sourceWriter.WriteLine("#pragma warning disable {0}", warningNum);
            }
        }
        private void RestoreWarnings(StreamWriter sourceWriter, int[] warnings)
        {
            foreach (int warningNum in warnings)
            {
                sourceWriter.WriteLine("#pragma warning restore {0}", warningNum);
            }
        }

        /// <summary>
        /// Updates the source file in the project if required.
        /// </summary>
        /// <param name="sourceFileToBeUpdated">Path to file to be updated.</param>
        /// <param name="outputFileGenerated">File that was updated.</param>
        private bool UpdateIntellisenseFile(string sourceFileToBeUpdated, string outputFileGenerated)
        {
            if (string.IsNullOrEmpty(sourceFileToBeUpdated)) throw new ArgumentNullException("sourceFileToBeUpdated", "Output file must not be blank");
            if (string.IsNullOrEmpty(outputFileGenerated)) throw new ArgumentNullException("outputFileGenerated", "Generated file must already exist");
            if (!File.Exists(sourceFileToBeUpdated)) throw new Exception("Output file must already exist");
            if (!File.Exists(outputFileGenerated)) throw new Exception("Generated file must already exist");
            
            FileInfo sourceToUpdateFileInfo = new FileInfo(sourceFileToBeUpdated);
            FileInfo generatedFileInfo = new FileInfo(outputFileGenerated);
            bool filesMatch = CheckFilesMatch(generatedFileInfo, sourceToUpdateFileInfo);
            if (filesMatch)
            {
                ConsoleText.WriteStatus("Orleans-CodeGen - No changes to the generated file {0}", sourceFileToBeUpdated);
                return false;
            }
            // we come here only if files don't match
            sourceToUpdateFileInfo.Attributes = sourceToUpdateFileInfo.Attributes & (~FileAttributes.ReadOnly); // remove read only attribute
            ConsoleText.WriteStatus("Orleans-CodeGen - copying file {0} to {1}", outputFileGenerated, sourceFileToBeUpdated);
            File.Copy(outputFileGenerated, sourceFileToBeUpdated, true);
            filesMatch = CheckFilesMatch(generatedFileInfo,sourceToUpdateFileInfo);
            ConsoleText.WriteStatus("Orleans-CodeGen - After copying file {0} to {1} Matchs={2}", outputFileGenerated, sourceFileToBeUpdated, filesMatch);
            return true;
        }

        private static bool CheckFilesMatch(FileInfo file1, FileInfo file2)
                {
            bool isMatching;
            long len1 = -1;
            long len2 = -1;
            if (file1.Exists)
            {
                len1 = file1.Length;
            }
            if (file2.Exists)
            {
                len2 = file2.Length;
            }
            if (len1 <= 0 || len2 <= 0)
            {
                isMatching = false;
            }
            else if (len1 != len2)
            {
                isMatching = false;
            }
            else
            {
                byte[] arr1 = File.ReadAllBytes(file1.FullName);
                byte[] arr2 = File.ReadAllBytes(file2.FullName);

                isMatching = true; // initially assume files match
                    for (int i = 0; i < arr1.Length; i++)
                    {
                        if (arr1[i] != arr2[i])
                        {
                            isMatching = false; // unless we know they don't match
                            break;
                        }
                    }
            }
            if (GrainClientGeneratorFlags.Verbose)
            {
                ConsoleText.WriteStatus("Orleans-CodeGen - CheckFilesMatch = {0} File1 = {1} Len = {2} File2 = {3} Len = {4}",
                    isMatching, file1, len1, file2, len2);
            }
            return isMatching;
        }

        /// <summary>
        /// Read a grain assembly and extract codegen info for each Orleans grain / service interface
        /// </summary>
        /// <param name="grainAssembly">Input grain assembly</param>
        /// <param name="namespaceDictionary">output list of grain namespace</param>
        internal void ProcessInputGrainAssembly(Assembly grainAssembly, Dictionary<string, GrainNamespace> namespaceDictionary, string outputAssemblyName)
        {
            ReferenceResolver.AssertUniqueLoadForEachAssembly();
            List<string> processedGrainInterfaces = new List<string>();
            ConsoleText.WriteStatus("Orleans-CodeGen - Adding grain namespaces ");
            foreach (Type t in grainAssembly.GetTypes())
            {
                GrainNamespace grainNamespace;
                if ( !t.IsNested && !t.IsGenericParameter && t.IsSerializable)
                {
                    SerializerGenerationManager.RecordTypeToGenerate(t);
                }
                if (GrainInterfaceData.IsGrainInterface(t))
                {
                    if (!namespaceDictionary.ContainsKey(t.Namespace))
                    {
                        grainNamespace = new GrainNamespace(grainAssembly, t.Namespace);
                        
                        ConsoleText.WriteStatus("\t" + t.Namespace);
                        namespaceDictionary.Add(t.Namespace, grainNamespace);
                    }
                    else
                    {
                        grainNamespace = namespaceDictionary[t.Namespace];
                    }
                    processedGrainInterfaces.Add(t.FullName);

                    // Processing steps to be take for each grain interface type found

                    GrainInterfaceData grainInterfaceData = new GrainInterfaceData(t);

                    // add Task<T> async reference classes & interfaces for this grain
                    if (typeof(IGrainObserver).IsAssignableFrom(t))
                    {
                        // Always skip grain observer interfaces
                    }

                    // add reference class that will implement the interface
                    grainNamespace.AddReferenceClass(grainInterfaceData);
                    
                    foreach (var code in grainNamespace.GetPropertyClasses(
                        grainInterfaceData,
                        type => grainNamespace.ReferredNamespaceAndAssembly(type)))
                    {
                        grainNamespace.ReferenceNamespace.Types.Add(code);
                    }
                }
            }
            ConsoleText.WriteStatus("Orleans-CodeGen - Processed grain classes: ");
            foreach (string name in processedGrainInterfaces)
            {
                ConsoleText.WriteStatus("\t" + name);
            }


            // Generate serializers for types we encountered along the way
            SerializerGenerationManager.GenerateSerializers(grainAssembly, namespaceDictionary, outputAssemblyName);
        }

        /// <summary>
        /// Codedom does not directly support extension methods therefore
        /// we must post process source files to do token 
        /// </summary>
        /// <param name="source"></param>
        private void PostProcessSourceFiles(string source)
        {
            bool headerWritten = false;

            using (StreamWriter output = File.CreateText(source + ".copy"))
            {
                using (StreamReader input = File.OpenText(source))
                {
                    string line = input.ReadLine();
                    while (line != null)
                    {
                        if (line.StartsWith("//"))
                        {
                            // pass through
                        }
                        else
                        {
                            // Now past the header comment lines

                            if (!headerWritten)
                            {
                                // Write Header

                                //output.WriteLine("[assembly: Orleans.ClientProxyAttribute()]"); // add a ClientProxy attribute to mark the assembly as generated by ClientGenerator.

                                // surround the generated code with defines so that we can conditionally exclude it elsewhere
                                output.WriteLine("#if !EXCLUDE_CODEGEN");

                                // Write pragmas to disable selected compiler warnings in generated code
                                DisableWarnings(output, suppressCompilerWarnings);

                                headerWritten = true;
                            }

                            if (line.Contains("ExtensionMethods"))
                            {
                                line = line.Replace("public", "public static");
                            }
                        }
                        output.WriteLine(line);
                        line = input.ReadLine();
                    }
                }
                // Write Footer
                RestoreWarnings(output, suppressCompilerWarnings);
                output.WriteLine("#endif"); 
            }
            File.Delete(source);
            File.Move(source + ".copy", source);
        }

        /// <summary>
        /// output grain reference source file for debug issue
        /// </summary>
        private void OutputReferenceSourceFile(GrainNamespace grainNamespace, StreamWriter sourceWriter)
        {
            //code compiler unit
            CodeCompileUnit unit = new CodeCompileUnit();

            CodeNamespace referenceNameSpace = grainNamespace.ReferenceNamespace;

            // add referrenced named spaces
            foreach (string referredNS in grainNamespace.ReferredNameSpaces)
            {
                if (referredNS != referenceNameSpace.Name)
                    referenceNameSpace.Imports.Add(new CodeNamespaceImport(referredNS));
            }

            unit.Namespaces.Add(referenceNameSpace);
            //output CS file under the current dir, for debug issue
            CodeGeneratorOptions options = new CodeGeneratorOptions();
            options.BracingStyle = "C";

            using (CodeDomProvider cSharpCodeProvider = InvokerGenerator.GetCSharpCodeProvider())
            {
                cSharpCodeProvider.GenerateCodeFromCompileUnit(unit, sourceWriter, options);
            }
        }

        /// <summary>
        /// generate one client assembly for all grain references and put it under outputLib
        /// </summary>
        private void CreateClientAssembly(IEnumerable<GrainNamespace> referenceList, List<string> referredAssemblies, CompilerParameters compilerParams, FileInfo clientPath, FileInfo keyFile, List<string> defines, string source)
        {
            List<string> sources = new List<string>();
            sources.Add(source);

            //code compiler unit
            CodeCompileUnit unit = new CodeCompileUnit();
            foreach (GrainNamespace grainNamespace in referenceList)
            {
                // add referrenced assemblies
                foreach (string name in grainNamespace.ReferredAssemblies)
                {
                    string assemblyName = name;
                    if (assemblyName.EndsWith(".dll"))
                        assemblyName = assemblyName.Replace(".dll", "");
                    if (!referredAssemblies.Contains(assemblyName))
                    {
                        referredAssemblies.Add(assemblyName);
                    }
                }
            }

            //Set compiler parameters

            foreach (string assembly in referredAssemblies)
                compilerParams.ReferencedAssemblies.Add(assembly + ".dll");
            compilerParams.ReferencedAssemblies.Add("System.Core.dll");
            compilerParams.ReferencedAssemblies.Add("System.dll");
            compilerParams.ReferencedAssemblies.Add("System.Xml.dll");
            if(!referredAssemblies.Contains("Orleans"))
                compilerParams.ReferencedAssemblies.Add("Orleans.dll");
            
            compilerParams.GenerateExecutable = false;
            compilerParams.OutputAssembly = clientPath.FullName;
            compilerParams.IncludeDebugInformation = true;
            // TODO: Add Back // compilerParams.TreatWarningsAsErrors = true;
            
            // Save the assembly as a physical file.
            compilerParams.GenerateInMemory = false;
            if (keyFile != null)
                compilerParams.CompilerOptions += String.Format(" /keyfile:\"{0}\" ", keyFile); /*+ " /delaysign"; */
            foreach (string def in defines)
            {
                compilerParams.CompilerOptions += String.Format(" /define:{0} ", def);
            }
            ConsoleText.WriteLine("Compiling client for assembly {0} with CompilerOptions={1}", clientPath.FullName, compilerParams.CompilerOptions);
            using (CodeDomProvider cSharpCodeProvider = InvokerGenerator.GetCSharpCodeProvider(true))
            {
                // set the reference path for the current assembly 
                // Compile
                CompilerResults results = cSharpCodeProvider.CompileAssemblyFromFile(compilerParams, sources.ToArray());
                //Check compile errors
                if (results.Errors.Count > 0)
                {
                    ConsoleText.WriteError(string.Format("Generator encountered {0} compilation errors", results.Errors.Count));
                    string errorString = string.Empty;
                    foreach (CompilerError error in results.Errors)
                    {
                        ConsoleText.WriteError(error.ToString());
                        errorString += error.ErrorText + "\n";

                    }
                    throw new OrleansException(String.Format("Could not compile and generate {0}\n", clientPath) + errorString);
                }
                else
                {
                    ConsoleText.WriteStatus("No errors encountered, client generator was successful");
                }
            }
        }

        private const string CodeGenFileRelativePath = "Properties\\orleans.codegen.cs";
        private const string FileNameForTimeStamp = "orleans.codegen.timestamp";

        internal static void BuildInputAssembly(FileInfo inputLib, FileInfo signingKey, List<string> referencedAssemblies, List<string> sourcesFiles, List<string> defines)
        {
            ConsoleText.WriteStatus("Orleans-CodeGen - Generating assembly for preprocessing.");
            CompilerParameters compilerParams = new System.CodeDom.Compiler.CompilerParameters();
            compilerParams.OutputAssembly = inputLib.FullName;
            StringBuilder newArgs = new StringBuilder(" /nostdlib ");
            if (null != signingKey)
            {
                newArgs.AppendFormat(" \"/keyfile:{0}\"", signingKey.FullName);
            }
            foreach (var source in sourcesFiles)
            {
                if (source.EndsWith(CodeGenFileRelativePath)) continue;
                newArgs.AppendFormat(" \"{0}\" ", source);
            }
            compilerParams.CompilerOptions += newArgs.ToString();
            HashSet<string> refs = new HashSet<string>();
            foreach (string refPath in referencedAssemblies)
            {
                if (!refs.Contains(refPath)) refs.Add(refPath);
            }
            foreach (string refPath in refs)
            {
                compilerParams.CompilerOptions += string.Format(" /reference:\"{0}\" ", refPath);
            }
            foreach (string def in defines)
            {
                compilerParams.CompilerOptions += string.Format(" /define:{0} ", def);
            }
            compilerParams.CompilerOptions += string.Format(" /define:EXCLUDE_CODEGEN ");

            using (CodeDomProvider cSharpCodeProvider = InvokerGenerator.GetCSharpCodeProvider(true))
            {
                CompilerResults results = cSharpCodeProvider.CompileAssemblyFromFile(compilerParams);
                //Check compile errors
                if (results.Errors.Count > 0)
                {
                    Console.WriteLine("Generator encountered {0} compilation errors", results.Errors.Count);
                    string errorString = string.Empty;
                    foreach (CompilerError error in results.Errors)
                    {
                        Console.WriteLine(error.ToString());
                        errorString += error.ErrorText + "\n";

                    }
                    throw new Exception(String.Format("Could not compile and generate {0}\n", errorString));
                }
            }
        }

        public int RunMain(string[] args)
        {
            //Debugger.Launch();
            //Debugger.Break();
            if (args.Length < 1)
            {
                Console.WriteLine("Usage: ClientGenerator.exe <grain interface dll path> [<client dll path>] [<key file>] [<referenced assemblies>]");
                Console.WriteLine("       ClientGenerator.exe /server <grain dll path> [<factory dll path>] [<key file>] [<referenced assemblies>]");
                return 1;
            }
            try
            {
                FileInfo inputLib = null;
                FileInfo outputLib = null;
                FileInfo signingKey = null;
                
                List<string> referencedAssemblies = new List<string>();
                List<string> sourceFiles = new List<string>();
                List<string> defines = new List<string>();

                bool serverGen = false;             
                bool shouldMerge = true;            // should be set to false in case of preprocessing
                bool compileFromSources = false;    // Do we need to compile input assembly ?
                bool bootstrap = false;             // Used to handle circular dependencies
                string codeGenFile = string.Empty;  // Assume it to be "orleans.codegen.cs" unless other name is provided on commandline.
                string sourcesDir;                  // folder where we save generated sources.

               // STEP 1 : Parse parameters
                //Console.WriteLine("Orleans-CodeGen - Step 1");
                if (args.Length == 1 && args[0].StartsWith("@"))
                {
                    // Read command line args from file
                    string arg = args[0];
                    string argsFile = arg.Trim('"').Substring(1).Trim('"');
                    Console.WriteLine("Orleans-CodeGen - Reading code-gen params from file={0}", argsFile);
                    AssertWellFormed(argsFile, true);
                    args = File.ReadAllLines(argsFile);
                }
                int i = 1;
                foreach (string a in args)
                {
                    string arg = a.Trim('"').Trim().Trim('"');
                    if (GrainClientGeneratorFlags.Verbose)
                    {
                        Console.WriteLine("Orleans-CodeGen - arg #{0}={1}", i++, arg);
                    }
                    if (String.IsNullOrEmpty(arg) || String.IsNullOrWhiteSpace(arg))
                        continue;
                    if (arg.StartsWith("/"))
                    {
                        if (arg == "/server" || arg == "/svr")
                        {
                            serverGen = true;
                        }
                        else if (arg.StartsWith("/reference:") || arg.StartsWith("/r:"))
                        {
                            // list of references passed from from project file. separator =';'
                            string refstr = arg.Substring(arg.IndexOf(':')+1);
                            string[] refs = refstr.Split(';');
                            foreach (string rp in refs)
                            {
                                AssertWellFormed(rp, true);
                                referencedAssemblies.Add(rp);
                            }
                        }
                        else if (arg.StartsWith("/in:"))
                        {
                            string infile = arg.Substring(arg.IndexOf(':')+1);
                            AssertWellFormed(infile);
                            inputLib = new FileInfo(infile);
                        }
                        else if (arg.StartsWith("/out:"))
                        {
                            string outfile = arg.Substring(arg.IndexOf(':')+1);
                            AssertWellFormed(outfile);
                            outputLib = new FileInfo(outfile);
                        }
                        else if (arg.StartsWith("/keyfile:") || arg.StartsWith("/key:"))
                        {
                            string keyFile = arg.Substring(arg.IndexOf(':')+1);
                            if (!string.IsNullOrWhiteSpace(keyFile))
                            {
                                AssertWellFormed(keyFile, true);
                                signingKey = new FileInfo(keyFile);
                            }
                        }
                        else if (arg.StartsWith("/update:") || arg.StartsWith("/upd:"))
                        {
                            // Explicitly specify path to codgen file to be updated.
                            // Useful when running clientgen in complicated dependecies.
                            codeGenFile = arg.Substring(arg.IndexOf(':')+1);
                            AssertWellFormed(codeGenFile);
                        }
                        else if (arg.StartsWith("/nomerge"))
                        {
                            // Do not merge assembly only generate sources.
                            shouldMerge = false;
                        }
                        else if (arg.StartsWith("/bootstrap") || arg.StartsWith("/boot"))
                        {
                            // special case for building circular dependecy in preprocessing: 
                            // Do not build the input assembly, assume that some other build step 
                            bootstrap = true;
                            codeGenFile = Path.GetFullPath(CodeGenFileRelativePath);
                            if (GrainClientGeneratorFlags.Verbose)
                            {
                                Console.WriteLine("Orleans-CodeGen - Set CodeGenFile={0} from bootstrap", codeGenFile);
                            }
                            serverGen = false;
                        }
                        else if (arg.StartsWith("/define:") || arg.StartsWith("/d:"))
                        {
                            // #define constants passed from project file. separator =';'
                            string defstr = arg.Substring(arg.IndexOf(':')+1);
                            string[] defs = defstr.Split(';');
                            foreach (string def in defs)
                            {
                                defines.Add(def);
                            }
                        }
                        else if (arg.StartsWith("/sources:") || arg.StartsWith("/src:"))
                        {
                            // C# sources passed from from project file. separator = ';'
                            compileFromSources = true;
                            if (GrainClientGeneratorFlags.Verbose)
                            {
                                Console.WriteLine("Orleans-CodeGen - Unpacking source file list arg={0}", arg);
                            }
                            string srcstr = arg.Substring(arg.IndexOf(':') + 1);
                            if (GrainClientGeneratorFlags.Verbose)
                            {
                                Console.WriteLine("Orleans-CodeGen - Splitting source file list={0}", srcstr);
                            }
                            string[] srcList = srcstr.Split(';');
                            foreach (string src in srcList)
                            {
                                AssertWellFormed(src, true);
                                sourceFiles.Add(src);
                                if (GrainClientGeneratorFlags.Verbose)
                                {
                                    Console.WriteLine("Orleans-CodeGen - Added source file={0}", src);
                                }
                                if (src.EndsWith(CodeGenFileRelativePath))
                                {
                                    codeGenFile = Path.GetFullPath(CodeGenFileRelativePath);
                                    if (GrainClientGeneratorFlags.Verbose)
                                    {
                                        Console.WriteLine("Orleans-CodeGen - Set CodeGenFile={0} from {1}", codeGenFile, src);
                                    }
                                    serverGen = false;
                                }
                            }
                        }
                    }
                    else
                    {
                        // files passed in without associated flags , we'll make the best guess.
                        if (arg.ToLowerInvariant().EndsWith(".snk"))
                        {
                            signingKey = new FileInfo(arg);
                        }
                        else
                        {
                            if (compileFromSources)
                            {
                                AssertWellFormed(arg, true);
                                sourceFiles.Add(arg);
                                if (GrainClientGeneratorFlags.Verbose)
                                {
                                    Console.WriteLine("Orleans-CodeGen - Added source file={0}", arg);
                                }
                                if (arg.EndsWith(CodeGenFileRelativePath))
                                {
                                    codeGenFile = Path.GetFullPath(CodeGenFileRelativePath);
                                    if (GrainClientGeneratorFlags.Verbose)
                                    {
                                        Console.WriteLine("Orleans-CodeGen - Set CodeGenFile={0} from {1}", codeGenFile, arg);
                                    }
                                    serverGen = false;
                                }
                            }
                            else
                            {
                                if (null == inputLib)
                                {
                                    inputLib = new FileInfo(arg.ToLowerInvariant().EndsWith(".dll") ?
                                            arg :
                                            arg + ".dll");
                                }
                                else if (null == outputLib)
                                {
                                    outputLib = new FileInfo(arg);
                                }
                                else
                                {
                                    referencedAssemblies.Add(arg);
                                }
                            }
                        }
                    }
                }

                // STEP 2 : Validate and calculate unspecified parameters
                //Console.WriteLine("Orleans-CodeGen - Step 2");
                if (inputLib == null)
                {
                    Console.WriteLine("Orleans-CodeGen - no input file specified.");
                    return 2;
                }
                
                if (outputLib == null)
                {
                    // generate sources under "Generated"
                    sourcesDir = Path.Combine(inputLib.DirectoryName, "Generated");
                    // Figure out what the output file name is and create output file under "Generated" 
                    string suffix = serverGen ? GrainInterfaceData.ActivationDllSuffix : GrainInterfaceData.ClientDllSuffix;
                    outputLib = new FileInfo(Path.Combine(sourcesDir, inputLib.Name.Replace(inputLib.Extension, suffix + ".dll")));
                }
                else
                {
                    // don't merge back into input 
                    shouldMerge = false;
                    sourcesDir = outputLib.DirectoryName;
                }

                if (!serverGen)
                {
                    // we do not support merge back into input when processing interface.
                    shouldMerge = false;
                }    
                
                // STEP 3 :  Check timestamps and skip if output is up-to-date wrt to all inputs
                //Console.WriteLine("Orleans-CodeGen - Step 3");
                if (IsProjectUpToDate(serverGen, inputLib, outputLib, sourceFiles, referencedAssemblies))
                {
                    Console.WriteLine("Orleans-CodeGen - Skipping because all output files are up-to-date with respect to the input files.");
                    return 0;
                }
                
                // STEP 4 : Dump useful info for debugging
                //Console.WriteLine("Orleans-CodeGen - Step 4");
                Console.WriteLine("Orleans-CodeGen - Options \n\tInputLib={0} \n\tOutputLib={1} \n\tSigningKey={2} \n\tServerGen={3} \n\tCodeGenFile={4}", 
                    inputLib.FullName, 
                    outputLib.FullName, 
                    signingKey != null ? signingKey.FullName : "", 
                    serverGen,
                    codeGenFile);
                if (referencedAssemblies != null)
                {
                    Console.WriteLine("Orleans-CodeGen - Using referenced libraries:");
                    foreach (string assy in referencedAssemblies)
                    {
                        Console.WriteLine("\t{0} => {1}", Path.GetFileName(assy), assy);
                    }
                }

                // STEP 5 :
                //Console.WriteLine("Orleans-CodeGen - Step 5");
                if (!serverGen && compileFromSources)
                {
                    if (!shouldMerge) 
                    {
                        // we should not overwrite original file so instead build the assembly of same name in Generated folder
                        // and use that file as input.
                        inputLib = new FileInfo(Path.Combine(sourcesDir,inputLib.Name));
                    }
                    if (!bootstrap)
                    {
                        //Console.WriteLine("Orleans-CodeGen - Building input assembly");
                        BuildInputAssembly(inputLib, signingKey, referencedAssemblies, sourceFiles, defines);
                    }
                }


                // STEP 6 : Finally call code that generates, build and merges the assembly.
                //Console.WriteLine("Orleans-CodeGen - Step 6");
                if (serverGen)
                {
                    //Console.WriteLine("Orleans-CodeGen - Generating server code");
                    referencedAssemblies.Add(inputLib.FullName);
                    InvokerGenerator.GenerateFactoryAssembly(
                        inputLib,
                        outputLib,
                        sourcesDir,
                        signingKey,
                        referencedAssemblies,
                        defines,
                        shouldMerge);
                }
                else
                {
                    //Console.WriteLine("Orleans-CodeGen - Generating client code");
                    CreateGrainClientAssembly(
                        compileFromSources,
                        inputLib,
                        outputLib,
                        sourcesDir,
                        signingKey,
                        referencedAssemblies,
                        sourceFiles,
                        defines,
                        shouldMerge,
                        codeGenFile);
                }

                // STEP 7 : save a timestamp file that will help us optimise rebuild.
                // Because grain assembly is post processed, the original assembly is always newer than the sources. It becomes tricky and bug-prone to use timestamps.
                // We need to check timestamps wrt some file OTHER THAN input sources, references or output binary.
                // The easiest solution is to write a file AFTER we are done writing output bin, and use time stamp of that file to decide.
                //Console.WriteLine("Orleans-CodeGen - Step 7");
                try
                {
                    using (var timestampFile = File.CreateText(Path.Combine(inputLib.DirectoryName, FileNameForTimeStamp)))
                    {
                        timestampFile.WriteLine("Last Access:{0}", DateTime.UtcNow);
                        timestampFile.Flush();
                    }
                }
                catch 
                { }

                // DONE!
                return 0;
            }
            catch (Exception ex)
            {
                //Debugger.Launch(); Debugger.Break();
                Console.WriteLine("ERROR -- Code-gen FAILED -- Exception caught -- {0}", ex);
                return 3;
            }

        }

        private static bool IsProjectUpToDate(bool isFactory ,FileInfo inputLib, FileInfo outputLib, List<string> sourceFiles, List<string> referencedAssemblies)
        {
            if (inputLib == null) return false;
            if (!inputLib.Exists) return false;
            if (outputLib == null) return false;
            if (!outputLib.Exists) return false;
            
            if (sourceFiles == null) return false;
            if (sourceFiles.Count==0) return false; // don't know so safer to say always out of date.

            // We can't rely on timestamps of input sources or references when it is grain assembly.
            // Instead we check the timestamp of "orleans.codegen.timestamp" file that we wrote last time we ran.
            // if any of the sources or references are newer the build system should first build the project output, 
            // which means we should regenerate activations.
            if (isFactory)
            {
                FileInfo fileForTimeStamp = new FileInfo(Path.Combine(inputLib.DirectoryName, FileNameForTimeStamp));
                if (!fileForTimeStamp.Exists) return false;
                // Input is newer than last time we updated/merged it.
                // This is only valid for activation/grains
                if (inputLib.LastWriteTimeUtc > fileForTimeStamp.LastWriteTimeUtc) return false;
            }
            foreach (string source in sourceFiles)
            {
                FileInfo sourceInfo = new FileInfo(source);
                // if any of the source files is newer than input lib or the output (i.e actvation/factory) then project is not up to date
                if (sourceInfo.LastWriteTimeUtc > inputLib.LastWriteTimeUtc) return false;
                if (sourceInfo.LastWriteTimeUtc > outputLib.LastWriteTimeUtc) return false;
            }
            foreach (string lib in referencedAssemblies)
            {
                FileInfo libInfo = new FileInfo(lib);
                if (libInfo.Exists)
                {
                    // if any of the reference files is newer than input lib or the output (i.e actvation/factory) then project is not up to date
                    if (libInfo.LastWriteTimeUtc > inputLib.LastWriteTimeUtc) return false;
                    if (libInfo.LastWriteTimeUtc > outputLib.LastWriteTimeUtc) return false;
                }
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <param name="mustExist"></param>
        private static void AssertWellFormed(string path, bool mustExist = false)
        {
            CheckPathNotStartWith(path, ":");
            CheckPathNotStartWith(path, "\"");
            CheckPathNotEndsWith(path, "\"");
            CheckPathNotEndsWith(path, "/");
            CheckPath(path, p => !string.IsNullOrWhiteSpace(p), "Empty path string");
            bool exists = FileExists(path);
            if (mustExist && GrainClientGeneratorFlags.FailOnPathNotFound)
            {
                CheckPath(path, p => exists, "Path not exists");
            }
        }
        private static bool FileExists(string path)
        {
            bool exists = File.Exists(path) || Directory.Exists(path);
            if (!exists) Console.WriteLine("MISSING: Path not exists: {0}", path);
            return exists;
        }
        private static void CheckPathNotStartWith(string path, string str)
        {
            CheckPath(path, p => !p.StartsWith(str), string.Format("Cannot start with '{0}'", str));
        }
        private static void CheckPathNotEndsWith(string path, string str)
        {
            CheckPath(path, p => !p.EndsWith(str), string.Format("Cannot end with '{0}'", str));
        }
        private static void CheckPath(string path, Func<string, bool> condition, string what)
        {
            if (!condition(path)) 
            {
                string errMsg = string.Format("Bad path {0} Reason = {1}", path, what);
                Console.WriteLine("CODEGEN-ERROR: " + errMsg);
                throw new ArgumentException("FAILED: " + errMsg);
            }
        }
    }

    public class Program
    {
        public static int Main(string[] args)
        {
            GrainClientGenerator generator = new GrainClientGenerator();
            return generator.RunMain(args);
        }
    }

    /// <summary>
    /// Simple class that loads the reference assemblies upon the AppDomain.AssemblyResolve
    /// </summary>
    [Serializable]
    internal class ReferenceResolver
    {
        /// <summary>
        /// Dictionary : Assembly file name without extension -> full path
        /// </summary>
        private Dictionary<string, string> referenceAssemblyPaths = new Dictionary<string, string>();
        /// <summary>
        /// Needs to be public so can be serialized accross the the app domain.
        /// </summary>
        public Dictionary<string, string> ReferenceAssemblyPaths
        {
            get { return referenceAssemblyPaths; }
            set { referenceAssemblyPaths = value; }
        }
        /// <summary>
        /// Inits the resolver
        /// </summary>
        /// <param name="referencedAssemblies">Full paths of referenced assemblies</param>
        public ReferenceResolver(IEnumerable<string> referencedAssemblies)
        {
            if (null != referencedAssemblies)
            {
                foreach (string asmPath in referencedAssemblies)
                {
                    referenceAssemblyPaths[Path.GetFileNameWithoutExtension(asmPath)] = asmPath;
                }
            }
        }

        /// <summary>
        /// Diagnostic method to verify that no duplicate types are loaded.
        /// </summary>
        /// <param name="message"></param>
        public static void AssertUniqueLoadForEachAssembly(string message = null)
        {
            if (!string.IsNullOrWhiteSpace(message)) ConsoleText.WriteStatus(message);
            ConsoleText.WriteStatus("Orleans-CodeGen - Assemblies loaded:");
            Dictionary<string, string> loaded = new Dictionary<string, string>();
            foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
            {
                string asmName = Path.GetFileName(asm.Location);
                ConsoleText.WriteStatus("\t{0} => {1}", asmName, asm.Location);
                if (!loaded.ContainsKey(asmName))
                {
                    loaded.Add(asmName, asm.Location);
                }
                else
                {
                    throw new Exception(string.Format("Assembly already loaded.Possible internal error !!!. \n\t{0}\n\t{1}", asm.Location, loaded[asmName]));
                }
            }
        }
        /// <summary>
        /// Handles System.AppDomain.AssemblyResolve event of an System.AppDomain
        /// </summary>
        /// <param name="sender">The source of the event.</param>
        /// <param name="args">The event data.</param>
        /// <returns>The assembly that resolves the type, assembly, or resource; 
        /// or null if theassembly cannot be resolved.
        /// </returns>
        public Assembly ResolveAssembly(object sender, ResolveEventArgs args)
        {
            Assembly asm = null;
            string path;
            AssemblyName asmName = new AssemblyName(args.Name);
            if (referenceAssemblyPaths.TryGetValue(asmName.Name, out path))
            {
                asm = Assembly.LoadFrom(path);
            }
            else
            {
                ConsoleText.WriteStatus("Could not resolve {0}:", asmName.Name);
            }
            return asm;
        }
    }
}
