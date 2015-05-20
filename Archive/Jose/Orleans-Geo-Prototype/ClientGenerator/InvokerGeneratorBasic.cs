using System;
using System.CodeDom;
using System.CodeDom.Compiler;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using ILMerging;
using Orleans;
using GrainClientGenerator.Serialization;

namespace GrainClientGenerator
{
    // ReSharper disable CheckForReferenceEqualityInstead.1

    internal class InvokerGeneratorBasic : MarshalByRefObject
    {
        private const string codeGeneratorName = "Orleans-CodeGenerator";
        static readonly string codeGeneratorVersion = Orleans.Version.FileVersion;

        protected HashSet<string> referredNamespaces = new HashSet<string>();
        protected HashSet<string> referredAssemblies = new HashSet<string>();
        protected string currentNamespace;
        protected const string GetPropertiesMethodName = "__special_method_get_properties__";
        public static void GetPropertyExpression(Type grainType, PropertyInfo property, string propVar, out string propExpr, out string thisExpr)
        {
            var dot = property.Name.LastIndexOf('.');
            var name = property.Name.Substring(dot + 1);
            thisExpr = "this." + name;
            propExpr = propVar + "." + name;
        }
        /// <summary>
        /// Returns a name string for a nested class type name (ClassName.TypeName)
        /// for a serializable type, the name string is only the TypeName
        /// </summary>
        internal static string GetNestedClassName(string name, bool noNamespace)
        {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            int start = 0;
            while (start < name.Length)
            {
                index = name.IndexOf('+', start);
                if (index == -1) break;
                builder.Append(name.Substring(start, index - start));
                builder.Append('.');
                start = index + 1;
            }
            if (index == -1)
            {
                if (noNamespace)
                    return name.Substring(start);
                builder.Append(name.Substring(start));
            }

            return builder.ToString();
        }

        /// <summary>
        /// Get the name string for generic type
        /// </summary>
        public static string GetGenericTypeName(Type type, Action<Type> referred, Func<Type,bool> noNamespace = null)
        {
            if (type == null) throw new ArgumentNullException("type");
            if (noNamespace == null)
                noNamespace = t => true;

            referred(type);

            if (!type.IsGenericType)
            {
                if (type.FullName == null) return type.Name;

                int length = type.Namespace.Length;
                var result = GetNestedClassName( noNamespace(type) ? type.Name : TypeUtils.GetFullName(type));
                return result == "Void" ? "void" : result;
            }

            StringBuilder builder = new StringBuilder();
            string name = noNamespace(type) ? type.Name : TypeUtils.GetFullName(type);
            int index = name.IndexOf("`", StringComparison.Ordinal);
            builder.Append(GetNestedClassName(name.Substring(0, index), noNamespace(type)));
            builder.Append('<');
            bool isFirstArgument = true;
            foreach (Type argument in type.GetGenericArguments())
            {
                if (!isFirstArgument)
                {
                    builder.Append(',');
                }
                builder.Append(GetGenericTypeName(argument, referred, noNamespace));
                isFirstArgument = false;
            }
            builder.Append('>');
            return builder.ToString();
        }

        /// <summary>
        /// Stores the compile errors
        /// </summary>
        protected static List<string> errors = new List<string>();

        /// <summary>
        /// Finds the Persistent interface given the grain class
        /// </summary>
        /// <param name="sourceType">source grain type</param>
        /// <returns>Persistent interface </returns>
        private static Type GetPersistentInterface(Type sourceType)
        {
            Type persistentInterface = null;
            if (typeof(GrainBase).IsAssignableFrom(sourceType))
            {
                Type t = sourceType.BaseType;
                if (!t.Equals(typeof(GrainBase))) // not directly deriving from GrainBase
                {
                    // go up till we find the base classe that derives directly from GrainBase
                    while (!t.BaseType.Equals(typeof(GrainBase)))
                    {
                        t = t.BaseType;
                    }
                    // Now we have a base class that derives from GrainBase,
                    // make sure it is generic and actually the GrainBase<T>
                    if (t.IsGenericType && t.Name.StartsWith(typeof(GrainBase).Name) && t.Namespace == typeof(GrainBase).Namespace)
                    {
                        // the argument is type of peristent interface
                        persistentInterface = t.GetGenericArguments()[0];
                    }
                }
            }
            return persistentInterface;
        }

        public IEnumerable<CodeTypeDeclaration> GetStateClasses(GrainInterfaceData grainInterfaceData,
            Action<Type> referred,
            string stateClassBaseName,
            string stateClassName,
            out bool hasStateClasses)
        {
            Type sourceType = grainInterfaceData.Type;

            CodeTypeParameterCollection genericTypeParams = grainInterfaceData.GenericTypeParams;
            var results = new List<CodeTypeDeclaration>();

            Type persistentInterface = GetPersistentInterface(sourceType);
            Dictionary<string,PropertyInfo> asyncProperties = GrainInterfaceData.GetPersistentProperties(persistentInterface)
                .ToDictionary(p => p.Name.Substring(p.Name.LastIndexOf('.') + 1), p => p);

            Dictionary<string,string> properties = asyncProperties.ToDictionary(p => p.Key,
                    p => GetGenericTypeName(GrainInterfaceData.GetPromptType(p.Value.PropertyType), referred, t => currentNamespace == t.Namespace || referredNamespaces.Contains(t.Namespace)));

            var stateClass = new CodeTypeDeclaration(stateClassBaseName);
            if (genericTypeParams != null) stateClass.TypeParameters.AddRange(genericTypeParams);
            stateClass.IsClass = true;
            stateClass.TypeAttributes = TypeAttributes.Public;
            stateClass.BaseTypes.Add(GetGenericTypeName(typeof(GrainState)));
            MarkAsGeneratedCode(stateClass);
            referred(typeof(GrainState));
            if (persistentInterface != null)
            {
                stateClass.BaseTypes.Add(GetGenericTypeName(persistentInterface));
                referred(persistentInterface);
            }
            stateClass.CustomAttributes.Add(new CodeAttributeDeclaration(typeof(SerializableAttribute).Name));
            stateClass.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(GrainStateAttribute)),
                        new CodeAttributeArgument(new CodePrimitiveExpression(grainInterfaceData.Type.Namespace + "." + TypeUtils.GetParameterizedTemplateName((grainInterfaceData.Type)))))
                        );

            referred(typeof(SerializableAttribute));
            referred(typeof(OnDeserializedAttribute));
            var snippet = new StringBuilder();
            snippet.AppendFormat(@"
            public {0}() : base(""{1}"")
            {{ 
                _InitStateFields();
            }}",
                stateClassBaseName,
                TypeUtils.GetFullName(grainInterfaceData.Type));

            snippet.Append(@"
            private void _InitStateFields()
            {");
            foreach (var pair in asyncProperties)
            {
                Type t = pair.Value.PropertyType;

                bool noCreateNew = 
                    t.IsPrimitive || typeof(string).IsAssignableFrom(t) // Primative types
                    || t.IsAbstract || t.IsInterface || t.IsGenericParameter // No concrete implementation
                    || t.GetConstructor(Type.EmptyTypes) == null; // No default constructor

                string dataTypeName = GetGenericTypeName(t);
                string dataTypeInit;
                if (noCreateNew)
                {
                    // Pre-initialize this type to default value

                    dataTypeInit = string.Format("default({0})", dataTypeName);
                }
                else
                {
                    dataTypeInit = string.Format("new {0}()", dataTypeName);
                }
                snippet.AppendFormat(@"
                this.{0} = {1};",
                    pair.Key, dataTypeInit);
            }
            snippet.Append(@"
            }");

            hasStateClasses = properties.Count > 0;
            string setAllBody = null;
            if (hasStateClasses)
            {
                setAllBody = @"object value;
                if (values == null) { _InitStateFields(); return; }";
                foreach (var pair in properties)
                {
                    setAllBody += @"
                ";

                    if ("long".Equals(pair.Value, StringComparison.OrdinalIgnoreCase)
                        || "int64".Equals(pair.Value, StringComparison.OrdinalIgnoreCase))
                    {
                        setAllBody += string.Format(
@"if (values.TryGetValue(""{0}"", out value)) {0} = value is Int32 ? (Int32)value : (Int64)value;",
                                         pair.Key);
                    }
                    else if ("int".Equals(pair.Value, StringComparison.OrdinalIgnoreCase)
                        || "int32".Equals(pair.Value, StringComparison.OrdinalIgnoreCase))
                    {
                        setAllBody += string.Format(
@"if (values.TryGetValue(""{0}"", out value)) {0} = value is Int64 ? (Int32)(Int64)value : (Int32)value;",
                                         pair.Key);
                    }
                    else
                    {
                        setAllBody += string.Format(
@"if (values.TryGetValue(""{0}"", out value)) {0} = ({1}) value;",
                                         pair.Key, pair.Value);
                    }
                }
            }
            snippet.AppendFormat(@"
            public override void SetAll(Dictionary<string,object> values)
            {{   
                {0}
            }}", setAllBody);
            if (hasStateClasses)
            {
                foreach (var pair in properties)
                {
                    snippet.AppendFormat(@"
            public {1} {0} {{ get; set; }}",
                    pair.Key, pair.Value);
                }
                snippet.AppendFormat(@"
            public override Dictionary<string,object> AsDictionary()
            {{
                var result = new Dictionary<string,object>();");
                foreach (var pair in properties)
                {
                    snippet.AppendFormat(@"
                result[""{0}""] = {0};",
                        pair.Key);
                }
                snippet.AppendFormat(@"
                return result;
            }}");
                int i = 0;
                snippet.AppendFormat(@"
            public override string ToString()
            {{
                return String.Format(""{0}( {1})""{2});
            }}",
                    stateClassName,
                    properties.Keys.ToStrings(p => p + "={" + i++ + "} ", ""),
                    properties.Keys.ToStrings(p => p, ", "));
            }
            stateClass.Members.Add(new CodeSnippetTypeMember(snippet.ToString()));

            // Copier, serializer, and deserializer for the state class
            var copier = SerializerGenerationUtilities.GenerateCopier("_Copier", stateClassName, genericTypeParams);
            var serializer = SerializerGenerationUtilities.GenerateSerializer("_Serializer", stateClassName, genericTypeParams);
            var deserializer = SerializerGenerationUtilities.GenerateDeserializer("_Deserializer", stateClassName, genericTypeParams);

            copier.Statements.Add(new CodeMethodReturnStatement(
                new CodeMethodInvokeExpression(
                    new CodeVariableReferenceExpression("input"),
                    "DeepCopy")));

            serializer.Statements.Add(
                new CodeMethodInvokeExpression(
                    new CodeVariableReferenceExpression("input"),
                    "SerializeTo",
                    new CodeArgumentReferenceExpression("stream")));

            deserializer.Statements.Add(new CodeVariableDeclarationStatement(stateClassName, "result",
                new CodeObjectCreateExpression(stateClassName)));
            deserializer.Statements.Add(new CodeMethodInvokeExpression(
                    new CodeVariableReferenceExpression("result"),
                    "DeserializeFrom",
                    new CodeArgumentReferenceExpression("stream")));
            deserializer.Statements.Add(new CodeMethodReturnStatement(
                    new CodeVariableReferenceExpression("result")));

            stateClass.Members.Add(copier);
            stateClass.Members.Add(serializer);
            stateClass.Members.Add(deserializer);

            results.Add(stateClass);

            return results;
        }


        public IEnumerable<CodeTypeDeclaration> GetPropertyClasses(GrainInterfaceData grainInterfaceData,
            Action<Type> referred)
        {
            Type sourceType = grainInterfaceData.Type;
            CodeTypeParameterCollection genericTypeParams = grainInterfaceData.GenericTypeParams;
            var results = new List<CodeTypeDeclaration>();
            var asyncProperties = GrainInterfaceData.GetProperties(sourceType)
                .ToDictionary(p => p.Name.Substring(p.Name.LastIndexOf('.') + 1), p => p);
            var properties = asyncProperties.ToDictionary(p => p.Key,
                    p => GetGenericTypeName(GrainInterfaceData.GetPromptType(p.Value.PropertyType), referred));
            
            if (sourceType.IsInterface && typeof(IAddressable).IsAssignableFrom(sourceType))
            {
                var propertiesClass = new CodeTypeDeclaration(grainInterfaceData.PropertiesClassBaseName);
                if (genericTypeParams != null) propertiesClass.TypeParameters.AddRange(genericTypeParams);
                propertiesClass.CustomAttributes.Add(new CodeAttributeDeclaration("Serializable"));
                propertiesClass.IsClass = true;
                propertiesClass.TypeAttributes = grainInterfaceData.Type.IsPublic ? TypeAttributes.Public : TypeAttributes.NotPublic;
                MarkAsGeneratedCode(propertiesClass);
                var snippet = new StringBuilder();
                foreach (var pair in properties)
                {
                    snippet.AppendFormat(@"
            public {1} {0} {{ get; set; }}",
                    pair.Key, pair.Value);
                }
                snippet.AppendFormat(@"
            public Dictionary<string,object> AsDictionary()
            {{  
                var retValue = new Dictionary<string,object>();");
                foreach (var pair in properties)
                {
                    snippet.AppendFormat(@"
                retValue[""{0}""] = {0};",
                        pair.Key);
                }
                snippet.AppendFormat(@"
                return retValue;
            }}");

                propertiesClass.Members.Add(new CodeSnippetTypeMember(snippet.ToString()));
                results.Add(propertiesClass);
            }
            return results;
        }

        internal CodeTypeDeclaration GetInvokerClass(GrainInterfaceData si, bool isClient)
        {
            Type grainType = si.Type;
            CodeTypeParameterCollection genericTypeParams = si.GenericTypeParams;

            CodeTypeDeclaration invokerClass = new CodeTypeDeclaration(si.InvokerClassBaseName);

            if (genericTypeParams != null) invokerClass.TypeParameters.AddRange(genericTypeParams);
            invokerClass.IsClass = true;
            MarkAsGeneratedCode(invokerClass);
            if (si.IsExtension)
            {
                invokerClass.BaseTypes.Add(GetGenericTypeName(typeof(IGrainExtensionMethodInvoker)));
            }
            else
            {
                invokerClass.BaseTypes.Add(GetGenericTypeName(typeof(IGrainMethodInvoker)));
            }

            GrainInterfaceInfo grainInterfaceInfo = GetInterfaceInfo(grainType);
            var interfaceId = grainInterfaceInfo.Interfaces.Keys.First();
            invokerClass.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(MethodInvokerAttribute)),
                        new CodeAttributeArgument(new CodePrimitiveExpression(grainType.Namespace + "." + TypeUtils.GetParameterizedTemplateName(grainType))),
                        new CodeAttributeArgument(new CodePrimitiveExpression(interfaceId)))
                        );

            CodeMemberProperty interfaceIdProperty = new CodeMemberProperty();
            interfaceIdProperty.Name = "InterfaceId";
            interfaceIdProperty.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            interfaceIdProperty.Type = new CodeTypeReference(typeof (int));
            interfaceIdProperty.GetStatements.Add(new CodeMethodReturnStatement(new CodePrimitiveExpression(interfaceId)));
            invokerClass.Members.Add(interfaceIdProperty);

            //Add invoke method for Orleans message 
            CodeMemberMethod orleansInvoker = new CodeMemberMethod();
            orleansInvoker.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            orleansInvoker.Name = "Invoke";
            orleansInvoker.ReturnType = new CodeTypeReference("async " + GetGenericTypeName(typeof(Task<object>)));
            orleansInvoker.Parameters.Add(new CodeParameterDeclarationExpression(GetGenericTypeName(typeof(IAddressable)), "grain"));
            orleansInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "interfaceId"));
            orleansInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "methodId"));
            orleansInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(object[]), "arguments"));

            CodeSnippetStatement orleansInvokerImpl = new CodeSnippetStatement(GetInvokerImpl(si, invokerClass, grainType, grainInterfaceInfo, isClient, false));
            orleansInvoker.Statements.Add(orleansInvokerImpl);
            invokerClass.Members.Add(orleansInvoker);

            //Add TryInvoke method for Orleans message, if the type is an extension interface
            if (si.IsExtension)
            {
                CodeMemberMethod orleansTryInvoker = new CodeMemberMethod();
                orleansTryInvoker.Attributes = MemberAttributes.Public | MemberAttributes.Final;
                orleansTryInvoker.Name = "Invoke";
                orleansTryInvoker.ReturnType = new CodeTypeReference("async " + GetGenericTypeName(typeof(Task<object>)));
                orleansTryInvoker.Parameters.Add(new CodeParameterDeclarationExpression(GetGenericTypeName(typeof(IGrainExtension)), "grain"));
                orleansTryInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "interfaceId"));
                orleansTryInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "methodId"));
                orleansTryInvoker.Parameters.Add(new CodeParameterDeclarationExpression(typeof(object[]), "arguments"));
                
                CodeSnippetStatement orleansTryInvokerImp =
                    new CodeSnippetStatement(GetInvokerImpl(si, invokerClass, grainType, grainInterfaceInfo, isClient,
                        false));
                orleansTryInvoker.Statements.Add(orleansTryInvokerImp);
                invokerClass.Members.Add(orleansTryInvoker);
            }

            //Add GetMethodName() method 
            CodeMemberMethod getMethodName = new CodeMemberMethod();
            getMethodName.Attributes = MemberAttributes.Public | MemberAttributes.Final | MemberAttributes.Static;
            getMethodName.Name = "GetMethodName";
            getMethodName.ReturnType = new CodeTypeReference(typeof(string));
            getMethodName.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "interfaceId"));
            getMethodName.Parameters.Add(new CodeParameterDeclarationExpression(typeof(int), "methodId"));

            CodeSnippetStatement orleansGetMethodNameImpl = new CodeSnippetStatement(GetOrleansGetMethodNameImpl(grainType, grainInterfaceInfo));
            getMethodName.Statements.Add(orleansGetMethodNameImpl);
            invokerClass.Members.Add(getMethodName);
            return invokerClass;
        }

        /// <summary>
        /// Get the name string for generic type
        /// </summary>
        internal string GetGenericTypeName(Type type)
        {
            // Add in the namespace of the type and the assembly file in which the type is defined
            AddReferencedAssembly(type);
            // Add in the namespace of the type and the assembly file in which any generic argument types are defined
            if (type.IsGenericType)
            {
                foreach (Type argument in type.GetGenericArguments())
                    AddReferencedAssembly(argument);
            }

            string typeName = TypeUtils.GetTemplatedName(type, t => currentNamespace!=t.Namespace && !referredNamespaces.Contains(t.Namespace) );
            return GetNestedClassName(typeName);
        }

        /// <summary>
        /// Calls the appropriate GetInterfaceInfo method depending on whether we are dealing with an implicit or explicit service type and
        /// returns the a dictionary of Inteface and Event info exposed by either service type
        /// </summary>
        /// <param name="grainType"></param>
        /// <returns></returns>
        internal static GrainInterfaceInfo GetInterfaceInfo(Type grainType)
        {
            GrainInterfaceInfo result = new GrainInterfaceInfo();
            Dictionary<int, Type> interfaces = GrainInterfaceData.GetServiceInterfaces(grainType);
            if (interfaces.Keys.Count == 0)
            {
                // Should never happen!
                Debug.Fail("Could not find any service interfaces for type=" + grainType.Name);
            }

            IEqualityComparer<InterfaceInfo> ifaceComparer = new InterfaceInfoComparer();
            foreach (var interfaceId in interfaces.Keys)
            {
                Type interfaceType = interfaces[interfaceId];
                InterfaceInfo interfaceInfo = new InterfaceInfo(interfaceType);

                if (!result.Interfaces.Values.Contains(interfaceInfo, ifaceComparer))
                {
                    result.Interfaces.Add(GrainInterfaceData.GetGrainInterfaceId(interfaceType), interfaceInfo);
                }
            }

            return result;
        }

        protected string GetInvokerImpl(GrainInterfaceData si, CodeTypeDeclaration invokerClass, Type grainType, GrainInterfaceInfo grainInterfaceInfo, bool isClient,
            bool isTry)
        {
            //No public method is implemented in this grain type for orleans messages
            if (grainInterfaceInfo.Interfaces.Count == 0)
            {
                if (isTry)
                {
                    return @"
                result = Orleans.TaskDone.Done;
                return false;
";
                }
                else
                {
                    return string.Format(@"
                throw new NotImplementedException(""No grain interfaces for type {0}"");
                ", TypeUtils.GetFullName(grainType));
                }
            }

            string interfaceSwitchBody = String.Empty;
            foreach (int interfaceId in grainInterfaceInfo.Interfaces.Keys)
            {
                InterfaceInfo interfaceInfo = grainInterfaceInfo.Interfaces[interfaceId];
                Type interfaceType = interfaceInfo.InterfaceType;

                interfaceSwitchBody += GetMethodDispatchSwitchForInterface(interfaceId, interfaceInfo, isClient, isTry);

                GrainInterfaceData iface = GrainInterfaceData.FromGrainClass(interfaceType);

                if (!isClient && RequiresPropertiesClass(iface, false) && !si.IsGeneric && !isTry)
                {
                    string propsClassName = GetGenericTypeName(interfaceType);
                    string propsClassTemplatedName = iface.PropertiesClassBaseName;

                    AddGetPropertiesMethod(invokerClass,
                        interfaceType,
                        propsClassTemplatedName,
                        propsClassName);
                }
            }

            if (isTry)
            {
                return string.Format(
                    @"                {1}
                    if (grain == null) throw new System.ArgumentNullException(""grain"");

                switch (interfaceId)
                {{
                    {0}
                    default:
                        {2};
                }}", interfaceSwitchBody, @"result = System.Threading.Tasks.Task.FromResult((object)null);", "return false");
            }
            else
            {
                return string.Format(
                    @"                if (grain == null) throw new System.ArgumentNullException(""grain"");
                switch (interfaceId)
                {{
                    {0}
                    default:
                        {1};
                }}", interfaceSwitchBody, "throw new System.InvalidCastException(\"interfaceId=\"+interfaceId)");
            }
        }

        private string GetMethodDispatchSwitchForInterface(int interfaceId, InterfaceInfo interfaceInfo, bool isClient, bool isTry)
        {
            string methodSwitchBody = String.Empty;
            string caseBodyStatements;

            foreach (int methodId in interfaceInfo.Methods.Keys)
            {
                MethodInfo methodInfo = interfaceInfo.Methods[methodId];
                Type returnType = methodInfo.ReturnType;
                GetGenericTypeName(returnType); // Adds return type assembly and namespace to import / library lists if necessary
                string invokeGrainArgs = string.Empty;

                ParameterInfo[] paramInfoArray = methodInfo.GetParameters();
                for (int count = 0; count < paramInfoArray.Length; count++)
                {
                    invokeGrainArgs += string.Format("({0})arguments[{1}]",
                        GetGenericTypeName(paramInfoArray[count].ParameterType), count);
                    if (count < paramInfoArray.Length - 1)
                        invokeGrainArgs += ", ";
                }

                // todo: parameters for indexed properties
                string grainTypeName = GetGenericTypeName(interfaceInfo.InterfaceType);
                
                string methodName = methodInfo.Name;
                
                string invokeGrainMethod;
                if (!methodInfo.IsSpecialName)
                {
                    invokeGrainMethod = string.Format("(({0})grain).{1}({2})", grainTypeName, methodName, invokeGrainArgs);

                }
                else if (methodInfo.Name.StartsWith("get_"))
                {
                    invokeGrainMethod = string.Format("(({0})grain).{1}", grainTypeName, methodName.Substring(4));
                }
                else if (methodInfo.Name.StartsWith("set_"))
                {
                    invokeGrainMethod = string.Format("(({0})grain).{1} = {2}", grainTypeName, methodName.Substring(4), invokeGrainArgs);
                }
                else
                {
                    // Should never happen
                    throw new InvalidOperationException("Don't know how to handle method " + methodInfo);
                }

                if (returnType == typeof(void))
                {
                    if (isTry)
                    {
                        caseBodyStatements = string.Format(
                            @"{0}; result = TaskDone.Done; return true;
",
                            invokeGrainMethod);
                    }
                    else
                    {
                        caseBodyStatements = string.Format(
                            @"{0}; return true;
",
                            invokeGrainMethod);
                    }
                }
                else if (GrainInterfaceData.IsTaskType(returnType))
                {
                    string valueType = String.Empty;
                    if (returnType != typeof(Task))
                        valueType = GetGenericTypeName(returnType.GetGenericArguments()[0]);

                    if (isTry)
                    {
                        bool voidTask = returnType == typeof(Task);
                        if(voidTask)
                            caseBodyStatements = string.Format(
                                @"{{var x = {0};
                                  result = x.ContinueWith(t => 
                                    {{
                                        if( t.Status == System.Threading.Tasks.TaskStatus.Faulted )
                                            throw t.Exception;
                                        else
                                            return (object) null;
                                    }});}}
                                  return true;
    ",
                                invokeGrainMethod);
                        else
                            caseBodyStatements = string.Format(
                                @"{{var x = {0};
                                  result = x.ContinueWith(t => (object)t.Result);}}
                                  return true;
    ",
                                invokeGrainMethod);
                    }
                    else
                    {
                        if (returnType == typeof (Task))
                        {
                            caseBodyStatements = string.Format(
                                @"await {0};
                              return true;
",
                                invokeGrainMethod);
                        }
                        else
                            caseBodyStatements = string.Format(
                                @"return await {0};
",
                                invokeGrainMethod);
                    }
                }
                else
                {
                    // Should never happen
                    throw new InvalidOperationException(string.Format(
                        "Don't know how to create invoker for method {0} with Id={1} of returnType={2}", methodInfo, methodId, returnType));
                }

                methodSwitchBody += string.Format(@"                            case {0}: 
                                {1}", methodId, caseBodyStatements);
            }

            GrainInterfaceData iface = GrainInterfaceData.FromGrainClass(interfaceInfo.InterfaceType);

            if (RequiresPropertiesClass(iface, isClient) && !iface.IsGeneric && !isTry)
            {
                string propsClassName = GetGenericTypeName(interfaceInfo.InterfaceType);
                int getPropsMethodId = Utils.CalculateIdHash(GetPropertiesMethodName);

                string getPropsMethodCall = isClient
                        ? string.Format("(({0})grain).GetProperties()", propsClassName)
                        : string.Format("GetPropertiesSeparatelyAndJoin(({0})grain)", propsClassName);

                caseBodyStatements = string.Format(
                    @"return {0};",
                    getPropsMethodCall);

                methodSwitchBody += string.Format(@"                            case {0}: {1}
                            ", getPropsMethodId, caseBodyStatements);
            }

            string defaultCase;
            if (isTry)
            {
                defaultCase = @"default: 
                                return false;";
            }
            else
            {
                defaultCase = @"default: 
                                throw new NotImplementedException(""interfaceId=""+interfaceId+"",methodId=""+methodId);";
            }

            return String.Format(@"case {0}:  // {1}
                        switch (methodId)
                        {{
{2}                            {3}
                        }}",
            interfaceId, interfaceInfo.InterfaceType.Name, methodSwitchBody, defaultCase);
        }

        private string CallBridgeFromTaskInvoker(Type returnType, string callInvoker)
        {
            if (returnType == typeof(Task))
            {
                callInvoker = string.Format("{0}", callInvoker);
            }
            else if (returnType.IsGenericType && returnType.GetGenericTypeDefinition().FullName == "System.Threading.Tasks.Task`1")
            {
                callInvoker = string.Format("{0}", callInvoker);
            }
            else
            {
                // Should never happen
                throw new InvalidOperationException("Don't know how to create Task invoker for call with returnType=" + returnType);
            }
            return callInvoker;
        }

        private static void AddGetPropertiesMethod(CodeTypeDeclaration invokerClass, Type type, string propertyClassName, string interfaceName)
        {
            // Ideally this method should be part of server side codegen.
            // add the method for getting properties - server side (although can be used by client if they really want to do this)
            StringBuilder result = new StringBuilder();
            var asyncProperties = GrainInterfaceData.GetProperties(type)
                .Where(p => GrainInterfaceData.IsTaskType(p.PropertyType))
                .ToDictionary(p => p.Name.Substring(p.Name.LastIndexOf('.') + 1), p => p);
            result.AppendFormat(@"
    static public async Task<{0}> GetPropertiesSeparatelyAndJoin({1} i) 
    {{ 
        System.Collections.Generic.Dictionary<string,System.Threading.Tasks.Task> promises = new System.Collections.Generic.Dictionary<string,System.Threading.Tasks.Task>();
        ", propertyClassName, interfaceName);

            foreach (string p in asyncProperties.Keys)
            {
                if (typeof(Task).IsAssignableFrom(asyncProperties[p].PropertyType))
                {
                    result.AppendFormat(@"
            promises.Add({1},i.{0});", p, string.Format("\"{0}\"", p));
                }

            }
            result.AppendFormat(@"
            System.Threading.Tasks.Task done = await System.Threading.Tasks.Task.WhenAll(promises.Values);
                {0} retValue = new {0}();
                ", propertyClassName);
            foreach (string p in asyncProperties.Keys)
            {
                if (typeof (Task).IsAssignableFrom(asyncProperties[p].PropertyType))
                {
                    result.AppendFormat(@"
                    retValue.{0}=(promises[{1}] as {2}).Result;", p, string.Format("\"{0}\"", p),
                                        TypeUtils.GetParameterizedTemplateName(asyncProperties[p].PropertyType, true));
                }
            }
            result.AppendFormat(@" 
                return retValue; 
        }}");
            invokerClass.Members.Add(new CodeSnippetTypeMember(result.ToString()));
        }

        internal static bool RequiresPropertiesClass(GrainInterfaceData grainInterfaceData, bool isClient)
        {
            if (!isClient)
            {
                Type it = grainInterfaceData.Type.GetInterface("I" + grainInterfaceData.Name);
                if (null == it)
                    return false;
                Type pt = it.Assembly.GetType(it.Namespace + "." + grainInterfaceData.PropertiesClassBaseName, false);
                if (null == pt)
                    return false;
            }

            Type sourceType = grainInterfaceData.Type;
            var properties = GrainInterfaceData.GetProperties(sourceType);
            int count = properties.Length;
            if (typeof(IAddressable).IsAssignableFrom(sourceType) && (count > 0))
            {
                return true;
            }
            return false;
        }

        internal string GetParameterisedTypeFrom(string name, CodeTypeParameterCollection genericTypeParam)
        {
            StringBuilder newType = new StringBuilder(name);
            if (genericTypeParam != null && genericTypeParam.Count > 0)
            {
                newType.Append("<");
                List<string> parameters = new List<string>();
                foreach (CodeTypeParameter param in genericTypeParam)
                {
                    parameters.Add(param.Name);
                }
                newType.Append(string.Join(",", parameters.ToArray()));
                newType.Append(">");
            }
            return newType.ToString();
        }

        protected string GetOrleansGetMethodNameImpl(Type grainType, GrainInterfaceInfo grainInterfaceInfo)
        {
            if (grainInterfaceInfo.Interfaces.Keys.Count == 0)
            {
                // No public method is implemented in this grain type for orleans messages
                string nullInvokeMethod = @"
                throw new NotImplementedException();
                ";

                return nullInvokeMethod;
            }

            var interfaces = new Dictionary<int, InterfaceInfo>(grainInterfaceInfo.Interfaces); // Copy, as we may alter the original collection in the loop below
            
            string interfaceSwitchBody = String.Empty;

            foreach (var kv in interfaces)
            {
                string methodSwitchBody = String.Empty;
                int interfaceId = kv.Key;
                InterfaceInfo interfaceInfo = kv.Value;

                foreach (int methodId in interfaceInfo.Methods.Keys)
                {
                    MethodInfo methodInfo = interfaceInfo.Methods[methodId];

                    //add return type assembly and namespace in
                    GetGenericTypeName(methodInfo.ReturnType);

                    string invokeGrainMethod = string.Format("return \"{0}\"", methodInfo.Name);
                    methodSwitchBody += string.Format(
                    @"case {0}:
                            {1};
                    "
                    , methodId, invokeGrainMethod);
                }
                methodSwitchBody += string.Format(
                    @"case {0}:
                            {1};
                    "
                    , Utils.CalculateIdHash(GetPropertiesMethodName), "return \"GetProperties\"");

                interfaceSwitchBody += String.Format(@"
                case {0}:  // {1}
                    switch (methodId)
                    {{
                        {2}
                        default: 
                            throw new NotImplementedException(""interfaceId=""+interfaceId+"",methodId=""+methodId);
                    }}",
                interfaceId, interfaceInfo.InterfaceType.Name, methodSwitchBody);
            } // End for each interface

            return string.Format(@"
            switch (interfaceId)
            {{
                {0}

                default:
                    throw new System.InvalidCastException(""interfaceId=""+interfaceId);
            }}",
            interfaceSwitchBody);
        }

        /// <summary>
        /// Find the namespace of type t and the assembly file in which type t is defined
        /// Add these in lists. Later this information is used to compile grain factory
        /// </summary>
        protected void ReferredNamespaceAndAssembly(Type t)
        {
            AddReferencedAssembly(t);
        }

        protected void AddReferencedAssembly(Type t)
        {
            var assembly = t.Assembly.GetName().Name + Path.GetExtension(t.Assembly.Location).ToLowerInvariant();
            if (!referredAssemblies.Contains(assembly))
            {
                referredAssemblies.Add(assembly);
            }
        }

        protected void AddUsingNamespace(Type t)
        {
            if (!referredNamespaces.Contains(t.Namespace))
                referredNamespaces.Add(t.Namespace);
        }

        /// <summary>
        /// get the name string for a nested class type name
        /// </summary>
        protected static string GetNestedClassName(string name)
        {
            StringBuilder builder = new StringBuilder();
            int index = 0;
            int start = 0;
            while (start < name.Length)
            {
                index = name.IndexOf('+', start);
                if (index == -1) break;
                builder.Append(name.Substring(start, index - start));
                builder.Append('.');
                start = index + 1;
            }
            if (index == -1)
                builder.Append(name.Substring(start));

            return builder.ToString();
        }

        /*
        /// <summary>
        /// decide whether the method is some special methods that implement an event. 
        /// Special Methods, like add_** and remove_**, shall be marked SpecialName in the metadata 
        /// </summary>
        protected static bool IsSpecialEventMethod(MethodInfo methodInfo)
        {
            return methodInfo.IsSpecialName &&
                   !((methodInfo.Name.StartsWith("set_") || methodInfo.Name.StartsWith("get_")) &&
                        methodInfo.GetCustomAttributes(typeof(LocalAttribute), true).Length == 0);
        }
         */
        /// <summary>
        /// Decide whether the method is some special methods that implement an event. 
        /// Special Methods, like add_** and remove_**, shall be marked SpecialName in the metadata 
        /// </summary>
        protected static bool IsSpecialEventMethod(MethodInfo methodInfo)
        {
            return methodInfo.IsSpecialName &&
                   (!(methodInfo.Name.StartsWith("get_") || methodInfo.Name.StartsWith("set_")));
        }

        protected static bool IsLocal(MethodInfo info)
        {
            return false;
        }

        /// <summary>
        /// decide whether this grain method is declared in this grain dll file
        /// </summary>
        protected static bool IsDeclaredHere(MethodInfo methodInfo, string dllPath)
        {
            Assembly assembly = Assembly.LoadFrom(dllPath);
            if (methodInfo.DeclaringType.Assembly.Equals(assembly))
                return true;
            return false;
        }
        /// <summary>
        /// Decide whether this method is a remote grain call method
        /// </summary>
        internal protected static bool IsGrainMethod(MethodInfo methodInfo)
        {
            if (methodInfo == null) throw new ArgumentNullException("methodInfo", "Cannot inspect null method info");

            // ignore static, event, or non-remote methods
            if (methodInfo.IsStatic || methodInfo.IsSpecialName || IsSpecialEventMethod(methodInfo))
            {
                return false; // Methods which are derived from base class or object class, or property getter/setter methods
            }
            if (!methodInfo.DeclaringType.IsInterface)
            {
                return false;
            }
            if (typeof(IAddressable).IsAssignableFrom(methodInfo.DeclaringType))
            {
                return true; // Methods which are from grain interfaces
            }
            return false;
        }

        public static void MarkAsGeneratedCode(CodeTypeDeclaration classRef, bool suppressDebugger = false, bool suppressCoverage = true)
        {
            classRef.CustomAttributes.Add(new CodeAttributeDeclaration(
                    new CodeTypeReference(typeof(GeneratedCodeAttribute)),
                    new CodeAttributeArgument(new CodePrimitiveExpression(codeGeneratorName)),
                    new CodeAttributeArgument(new CodePrimitiveExpression(codeGeneratorVersion))
                ));
            if (!classRef.IsInterface)
            {
                if (suppressCoverage) classRef.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute))));

                if (suppressDebugger) classRef.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(DebuggerNonUserCodeAttribute))));
            }

        }

        internal class InterfaceInfo
        {
            public Type InterfaceType { get; private set; }
            public Dictionary<int, MethodInfo> Methods { get; private set; }

            public InterfaceInfo(Type interfaceType)
            {
                this.InterfaceType = interfaceType;
                this.Methods = GetGrainMethods();
            }

            private Dictionary<int, MethodInfo> GetGrainMethods()
            {
                var grainMethods = new Dictionary<int, MethodInfo>();
                foreach (MethodInfo interfaceMethodInfo in GrainInterfaceData.GetMethods(InterfaceType))
                {
                    ParameterInfo[] parameters = interfaceMethodInfo.GetParameters();
                    Type[] args = new Type[parameters.Length];
                    for (int i = 0; i < parameters.Length; i++)
                    {
                        args[i] = parameters[i].ParameterType;
                    }
                    MethodInfo methodInfo = InterfaceType.GetMethod(interfaceMethodInfo.Name, args) ?? interfaceMethodInfo;

                    if (IsGrainMethod(methodInfo) || GrainNamespace.IsGetPropertyMethod(methodInfo) || GrainNamespace.IsSetPropertyMethod(methodInfo))
                    {
                        grainMethods.Add(GrainInterfaceData.ComputeMethodId(methodInfo), methodInfo);
                    }
                }
                return grainMethods;
            }

            public override string ToString()
            {
                return "InterfaceInfo:" + InterfaceType.FullName + ",#Methods=" + Methods.Count;
            }
        }

        internal class GrainInterfaceInfo
        {
            public Dictionary<int, InterfaceInfo> Interfaces { get; private set; }
            //public Dictionary<string, EventInfoData> Events { get; private set; }

            public GrainInterfaceInfo()
            {
                Interfaces = new Dictionary<int, InterfaceInfo>();
                //Events = new Dictionary<string, EventInfoData>();
            }
        }

        internal class InterfaceInfoComparer : IEqualityComparer<InterfaceInfo>
        {

            #region IEqualityComparer<InterfaceInfo> Members

            public bool Equals(InterfaceInfo x, InterfaceInfo y)
            {
                string xFullName = TypeUtils.GetFullName(x.InterfaceType);
                string yFullName = TypeUtils.GetFullName(y.InterfaceType);
                return String.CompareOrdinal(xFullName, yFullName) == 0;
            }

            public int GetHashCode(InterfaceInfo obj)
            {
                throw new NotImplementedException();
            }

            #endregion
        }

        internal static void MergeAssemblies(FileInfo inputLib, FileInfo generatedLib, string sourcesDir, FileInfo signingKey, List<string> referencedAssemblyPaths)
        {
            ConsoleText.WriteStatus("ClientGenerator - Merging Assemblies:");
            // STEP 2.  Merge assemblies
            AppDomain appDomain = null;
            try
            {
                // Create AppDomain.
                AppDomainSetup appDomainSetup = new AppDomainSetup();
                appDomainSetup.ApplicationBase = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
                appDomainSetup.DisallowBindingRedirects = false;
                appDomainSetup.ConfigurationFile = AppDomain.CurrentDomain.SetupInformation.ConfigurationFile;
                appDomain = AppDomain.CreateDomain("MergeHelper Domain", null, appDomainSetup);
                ReferenceResolver refResolver = new ReferenceResolver(referencedAssemblyPaths);
                appDomain.AssemblyResolve += refResolver.ResolveAssembly;
                // Create an instance 
                InvokerGeneratorBasic mergeHelper = (InvokerGeneratorBasic) appDomain.CreateInstanceAndUnwrap(
                    Assembly.GetExecutingAssembly().FullName,
                    typeof (InvokerGeneratorBasic).FullName);

                // Set up rerouting of Console WriteLines
                //mergeHelper.SetOut(Console.Out);
                //mergeHelper.SetErr(Console.Error);

                // Call a method 
                mergeHelper.MergeAssemblies(inputLib, generatedLib, signingKey, sourcesDir, referencedAssemblyPaths);
            }
            catch (Exception ex)
            {
                ConsoleText.WriteError(string.Format("ERROR -- MergeAssemblies FAILED -- Exception caught -- {0}", ex));
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
            // STEP 3.  Copy the assemblies back
            try
            {
                string mergedDll = Path.Combine(sourcesDir, Path.GetFileName(inputLib.FullName));
                File.Copy(mergedDll, inputLib.FullName, true);
                string frompdb = Path.Combine(sourcesDir, Path.GetFileNameWithoutExtension(inputLib.FullName) + ".pdb");
                string topdb = Path.Combine(inputLib.DirectoryName, Path.GetFileNameWithoutExtension(inputLib.FullName) + ".pdb");
                File.Copy(frompdb, topdb, true);
                ConsoleText.WriteStatus("\tCopying merged assembly {0} to original {1}", mergedDll, inputLib.FullName);
            }
            catch (Exception ex)
            {
                ConsoleText.WriteError(string.Format("ERROR -- MergeAssemblies FAILED -- Exception caught -- {0}", ex));
                throw;
            }
        }

        /// <summary>
        /// Using ILMerge merge the inputlib and the factory assembly into an assembly under output directory with the same name as inputlib.
        /// </summary>
        /// <param name="grainAssembly">Name of the primary assembly</param>
        /// <param name="activationLib">Name of the activation dll</param>
        /// <param name="signingKey">signing key file</param>
        /// <param name="outputDirectory">Output directory</param>
        /// <param name="referencedAssemblyPaths">List of references assemblies</param>
        public void MergeAssemblies(FileInfo grainAssembly, FileInfo activationLib, FileInfo signingKey, string outputDirectory, List<string> referencedAssemblyPaths)
        {
            try
            {
                ILMerge ilmerge = new ILMerge();

                // Set options
                // "$(SolutionDir)Dependencies\ILMerge\ILMerge.exe" "/targetplatform:v4,C:\Windows\Microsoft.NET\Framework64\v4.0.30319" "/keyfile:$(SolutionDir)Orleans.snk" "$(TargetName)$(TargetExt)" "$(TargetName)Activation$(TargetExt)" "/out:Generated\$(TargetName)$(TargetExt)"
                // File.Delete(ilmerge.OutputFile);

                ilmerge.OutputFile = Path.Combine(outputDirectory, Path.GetFileName(grainAssembly.FullName));
                string pathToFramework = Path.GetDirectoryName(typeof(object).Assembly.Location);
                ilmerge.SetTargetPlatform("v4", pathToFramework);
                ilmerge.CopyAttributes = true;
                ilmerge.AllowMultipleAssemblyLevelAttributes = true;
                ilmerge.SetInputAssemblies(new String[] { grainAssembly.FullName, activationLib.FullName });

                //ilmerge.Log = true; // Set to true for diagnostic

                // Add lib paths
                HashSet<string> libDirs = new HashSet<string>();
                foreach (string libpath in referencedAssemblyPaths)
                {
                    string libDir = Path.GetDirectoryName(libpath);
                    if (!libDirs.Contains(libDir)) libDirs.Add(libDir);
                }
                ilmerge.SetSearchDirectories(libDirs.ToArray());

                // find and use the signing key
                if (null != signingKey)
                {
                    ilmerge.KeyFile = signingKey.FullName;
                }

                // Finally merge them
                ilmerge.Merge();
                ConsoleText.WriteStatus("\tAssemblies Merged to {0}", ilmerge.OutputFile);

            }
            catch (Exception ex)
            {
                ConsoleText.WriteError(string.Format("ERROR -- ILMerge Assemblies FAILED -- Exception caught -- {0}", ex));
                throw;
            }
        }
    }
    // ReSharper restore CheckForReferenceEqualityInstead.1
}

