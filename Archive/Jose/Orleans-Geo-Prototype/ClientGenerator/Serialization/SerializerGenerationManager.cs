using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Orleans.Serialization;
using Orleans;

using System.Runtime.Serialization;

namespace GrainClientGenerator.Serialization
{
    internal static class SerializerGenerationManager
    {
        private static HashSet<Type> typesToProcess;
        private static HashSet<Type> processedTypes;

        internal static void Init()
        {
            ConsoleText.WriteStatus("Initializing serializer generation manager");
            typesToProcess = new HashSet<Type>();
            processedTypes = new HashSet<Type>();
        }

        internal static void RecordTypeToGenerate(Type t)
        {
            if (t.IsGenericParameter || processedTypes.Contains(t) || typesToProcess.Contains(t) || typeof(Exception).IsAssignableFrom(t))
            {
                return;
            }

            if (t.IsArray)
            {
                RecordTypeToGenerate(t.GetElementType());
                return;
            }

            if (t.IsNestedPublic || t.IsNestedFamily || t.IsNestedPrivate)
            {
                Console.WriteLine("Skipping serializer generation for nested type {0}. If this type is used frequently, you may wish to consider making it non-nested.",
                    t.Name);
            } 
            
            if (t.IsGenericType)
            {
                var args = t.GetGenericArguments();
                foreach (var arg in args)
                {
                    if (!arg.IsGenericParameter)
                    {
                        RecordTypeToGenerate(arg);
                    }
                }
            }

            if (t.IsInterface || t.IsAbstract || t.IsEnum || t.Equals(typeof(object)) || t.Equals(typeof(void)) 
                || typeof(AsyncCompletion).IsAssignableFrom(t)
                || GrainInterfaceData.IsTaskType(t))
            {
                return;
            }

            //if (!t.IsSerializable)
            //{
            //    throw new SerializationException("Type " + t.FullName + " is not marked as Serializable");
            //}

            if (t.IsGenericType)
            {
                var def = t.GetGenericTypeDefinition();
                if (def.Equals(typeof(AsyncValue<>)) || def.Equals(typeof(Task<>)) || (SerializationManager.GetSerializer(def) != null) ||
                    processedTypes.Contains(def) || typeof(IAddressable).IsAssignableFrom(def))
                {
                    return;
                }
                if (def.Namespace.Equals("System") || def.Namespace.StartsWith("System."))
                {
                    ConsoleText.WriteError("System type " + def.Name + " requires a serializer.");
                }
                else
                {
                    typesToProcess.Add(def);
                }
                return;
            }

            if (t.IsOrleansPrimitive() || (SerializationManager.GetSerializer(t) != null) || typeof(IAddressable).IsAssignableFrom(t))
            {
                return;
            }

            if (t.Namespace.Equals("System") || t.Namespace.StartsWith("System."))
            {
                ConsoleText.WriteError("System type " + t.Name + " may require a custom serializer for optimal performance.");
                ConsoleText.WriteError("If you use arguments of this type a lot, consider asking the Orleans team to build a custom serializer for it.");
                return;
            }

            if (t.IsArray)
            {
                RecordTypeToGenerate(t.GetElementType());
                return;
            }

            bool hasCopier = false;
            bool hasSerializer = false;
            bool hasDeserializer = false;
            foreach (var method in t.GetMethods(BindingFlags.Static | BindingFlags.Public))
            {
                if (method.GetCustomAttributes(typeof(SerializerMethodAttribute), false).Length > 0)
                {
                    hasSerializer = true;
                }
                else if (method.GetCustomAttributes(typeof(DeserializerMethodAttribute), false).Length > 0)
                {
                    hasDeserializer = true;
                }
                if (method.GetCustomAttributes(typeof(CopierMethodAttribute), false).Length > 0)
                {
                    hasCopier = true;
                }
            }
            if (hasCopier && hasSerializer && hasDeserializer)
            {
                return;
            }

            typesToProcess.Add(t);
        }

        internal static bool GetNextTypeToProcess(out Type next)
        {
            next = null;
            if (typesToProcess.Count == 0)
            {
                return false;
            }

            var enumerator = typesToProcess.GetEnumerator();
            enumerator.MoveNext();
            next = enumerator.Current;

            typesToProcess.Remove(next);
            processedTypes.Add(next);

            return true;
        }

        internal static void GenerateSerializers(Assembly grainAssembly, Dictionary<string, GrainNamespace> namespaceDictionary, string outputAssemblyName)
        {
            Type toGen;
            GrainNamespace extraNamespace = null;
            ConsoleText.WriteStatus("ClientGenerator - Generating serializer classes");
            while (GetNextTypeToProcess(out toGen))
            {
                ConsoleText.WriteStatus("\ttype " + toGen.FullName + " in namespace " + toGen.Namespace);
                GrainNamespace typeNamespace;
                if (!namespaceDictionary.TryGetValue(toGen.Namespace, out typeNamespace))
                {
                    if (extraNamespace == null)
                    {
                        // Calculate a unique namespace name based on the output assembly name
                        extraNamespace = new GrainNamespace(grainAssembly, outputAssemblyName + "Serializers");
                        namespaceDictionary.Add("OrleansSerializers", extraNamespace);
                    }
                    typeNamespace = extraNamespace;
                    typeNamespace.ReferredAssembly(toGen);
                    foreach (var info in toGen.GetFields()) { typeNamespace.ReferredNamespaceAndAssembly(info.FieldType); }
                    foreach (var info in toGen.GetProperties()) { typeNamespace.ReferredNamespaceAndAssembly(info.PropertyType); }
                    foreach (var info in toGen.GetMethods())
                    {
                        typeNamespace.ReferredNamespaceAndAssembly(info.ReturnType);
                        foreach (var arg in info.GetParameters()) { typeNamespace.ReferredNamespaceAndAssembly(arg.ParameterType); }
                    }
                }
                SerializationGenerator.GenerateSerializationForClass(toGen, typeNamespace.ReferenceNamespace, typeNamespace.ReferredNameSpaces);
            }
        }
    }
}
