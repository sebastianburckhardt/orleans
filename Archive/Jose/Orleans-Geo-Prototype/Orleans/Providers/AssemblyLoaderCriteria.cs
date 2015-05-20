using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;

namespace Orleans.Providers
{
    internal static class AssemblyLoaderCriteria
    {
        internal static readonly AssemblyLoaderPathNameCriterion ExcludeResourceAssemblies =
            AssemblyLoaderPathNameCriterion.NewCriterion(
                (string pathName, out string[] complaints) =>
                {
                    if (pathName.EndsWith(".resources.dll", StringComparison.OrdinalIgnoreCase))
                    {
                        complaints = new string[] {"Assembly filename indicates that it is a resource assembly."};
                        return false;                        
                    }
                    else
                    {
                        complaints = null;
                        return true;
                    }
                });

        private static bool InterpretReflectionTypeLoadException(ReflectionTypeLoadException outer, out string[] complaints)
        {
            var badFiles = new Dictionary<string, string>();
            foreach (var exception in outer.LoaderExceptions)
            {
                var fileNotFound = exception as FileNotFoundException;
                var fileLoadError = exception as FileLoadException;
                string fileName = null;
                if (fileNotFound != null)
                {
                    fileName = fileNotFound.FileName;
                }
                else if (fileLoadError != null)
                {
                    fileName = fileLoadError.FileName;
                }

                if (fileName != null)
                {
                    if (badFiles.ContainsKey(fileName)) 
                    {
                        // Don't overright first entry for this file, because it probably contains best error
                    }
                    else
                    {
                        badFiles.Add(fileName, exception.Message);
                    }
                }
                else
                {
                    // we haven't anticipated this specific exception, so rethrow.
                    complaints = null;
                    return false;
                }
            }            
            // experience shows that dependency errors in ReflectionTypeLoadExceptions tend to be redundant.
            // here, we ensure that each missing dependency is reported only once.
            complaints = badFiles.Select(
                        (fileName, msg) =>
                            String.Format("An assembly dependency {0} could not be loaded: {1}", fileName, msg))
                    .ToArray();
            // exception was anticipated.
            return true;
        }

        internal static AssemblyLoaderReflectionCriterion LoadTypesAssignableFrom(Type[] requiredTypes)
        {
            return
                AssemblyLoaderReflectionCriterion.NewAssemblyLoaderReflectionCriterion(
                    (Assembly assembly, out string[] complaints) =>
                    {
                        // any types provided must be converted to reflection-only
                        // types, or they aren't comparable with other reflection-only 
                        // types.
                        var reflectionTypes = 
                            requiredTypes
                                .Select(
                                    t => 
                                        Type.ReflectionOnlyGetType(t.AssemblyQualifiedName, true, false))
                                .ToArray();

                        Type[] types;
                        try
                        {
                            types = assembly.GetTypes();
                        }
                        catch (ReflectionTypeLoadException e)
                        {
                            if (InterpretReflectionTypeLoadException(e, out complaints))
                                return false;
                            else
                            {
                                // the default representation of a ReflectionTypeLoadException isn't very helpful
                                // to the user, so we flatten it into an AggregateException.
                                throw e.Flatten();
                            }
                        }

                        foreach (var i in types)
                        {
                            foreach (var j in reflectionTypes)
                            {
                                if (j.IsAssignableFrom(i))
                                {
                                    //  we found a match! load the assembly.
                                    complaints = null;
                                    return true;
                                }
                            }
                        }
                        // none of the types in the assembly could be matched against anything
                        // within requiredTypes.
                        complaints = new string[requiredTypes.Length];
                        for (var i = 0; i < requiredTypes.Length; ++i)
                        {
                            complaints[i] = 
                                String.Format("Assembly contains no types assignable from {0}.", requiredTypes[i].FullName);
                        }
                        return false;  
                    });
        }

        internal static AssemblyLoaderReflectionCriterion LoadTypesAssignableFrom(Type requiredType)
        {
            return LoadTypesAssignableFrom(new Type[] {requiredType});
        }

        internal static readonly string[] 
            SystemBinariesList = 
                new string[]
                    {
                        "Microsoft.Data.Edm.dll",
                        "Orleans.dll",
                        "OrleansRuntime.dll"
                    };

        internal static AssemblyLoaderPathNameCriterion ExcludeSystemBinaries()
        {
            return ExcludeFileNames(SystemBinariesList);
        }

        internal static AssemblyLoaderPathNameCriterion ExcludeFileNames(IEnumerable<string> list)
        {
            return
                AssemblyLoaderPathNameCriterion.NewCriterion(
                    (string pathName, out string[] complaints) =>
                    {
                        var fileName = Path.GetFileName(pathName);
                        foreach (var i in list)
                        {
                            if (String.Equals(fileName, i, StringComparison.OrdinalIgnoreCase))
                            {
                                complaints = new string[] {"Assembly filename is excluded."};
                                return false;
                            }
                        }
                        complaints = null;
                        return true;
                    });
        }
    }
}
