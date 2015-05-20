using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using GrainClientGenerator.Serialization;
using Orleans;



// Suppress ReSharper warnings about use of CodeDom *Attributes enum's which should have [Flags] attribute but don't
// ReSharper disable BitwiseOperatorOnEnumWihtoutFlags

namespace GrainClientGenerator
{
    internal class GrainNamespace : InvokerGeneratorBasic
    {
        readonly HashSet<int> methodIdCollisionDetection;
        readonly CodeNamespace referenceNamespace;
        readonly Assembly grainAssembly;

        GrainInterfaceData grainInterfaceData;

        public GrainNamespace(Assembly grainAssembly, string nameSpace)
        {
            methodIdCollisionDetection = new HashSet<int>();
            referenceNamespace = new CodeNamespace(nameSpace);
            currentNamespace = nameSpace;
            this.grainAssembly = grainAssembly;
        }

        public enum SerializeFlag
        {
            SerializeArgument = 0,
            DeserializeResult = 1,
            NoSerialize = 2,
        }

        public CodeNamespace ReferenceNamespace
        {
            get
            {
                return referenceNamespace;
            }
        }

        public HashSet<string> ReferredNameSpaces
        {
            get { return this.referredNamespaces; }
        }

        public HashSet<string> ReferredAssemblies
        {
            get { return this.referredAssemblies; }
        }

        internal static CodeMemberProperty ConvertToTaskAsyncProperty(PropertyInfo prop)
        {
            var p = new CodeMemberProperty();
            Type t = prop.PropertyType;
            bool isTaskType = GrainInterfaceData.IsTaskType(t);
            p.Name = prop.Name;
            if (!isTaskType && !typeof(IAddressable).IsAssignableFrom(t))
                p.Name += "Async";
            p.HasGet = true; // (propertyInfo.GetGetMethod() != null);
            p.HasSet = false; // (propertyInfo.GetSetMethod() != null);

            if (typeof(IAddressable).IsAssignableFrom(t))
            {
                // Property is a grain reference type, so leave unaltered
            }
            else if (isTaskType)
            {
                // No-op - property type is already Task<T>
            }
            else if (typeof(AsyncCompletion).IsAssignableFrom(t))
            {
                if (t.IsGenericType)
                {
                    // AsyncValue<T>
                    Type[] gArgs = t.GetGenericArguments();
                    if (gArgs.Length == 0)
                    {
                        throw new InvalidOperationException("Cannot find any generic type parma info for " + t.FullName);
                    }
                    t = typeof(Task<>).MakeGenericType(gArgs);
                }
                else
                {
                    // AsyncCompletion - not valid on a property
                    throw new InvalidOperationException("Property type " + t.FullName + " cannot be an Async Task property");
                }
            }
            else
            {
                throw new InvalidOperationException(string.Format(
                    "Cannot convert property type {0} to an Async Task<T> type. Property Name={1} Declaring Type={2}",
                    t.FullName, prop.Name, TypeUtils.GetFullName(prop.DeclaringType)));
            }
            p.Type = CreateCodeTypeReference(t);

            return p;
        }

        internal CodeMemberMethod ConvertToTaskAsyncMethod(MethodInfo methodInfo, bool isTaskInterface)
        {
            var m = new CodeMemberMethod();
            m.Name = methodInfo.Name;
            if (!isTaskInterface) m.Name += "Async";

            Type t = methodInfo.ReturnType;
            Type[] gArgs = t.GetGenericArguments();

            if (typeof(IAddressable).IsAssignableFrom(t))
            {
                // Method return value is a grain reference type, so leave unaltered
            }
            else if (GrainInterfaceData.IsTaskType(t))
            {
                // Method return value is a Task-based type, so leave unaltered
            }
            else if (typeof(AsyncCompletion).IsAssignableFrom(t))
            {
                if (t.IsGenericType)
                {
                    // AsyncValue<T>
                    if (gArgs.Length == 0)
                    {
                        throw new InvalidOperationException("Cannot find any generic type parma info for " + t.FullName);
                    }
                    t = typeof(Task<>).MakeGenericType(gArgs);
                }
                else
                {
                    // AsyncCompletion
                    t = typeof(Task);
                }
            }
            else
            {
                throw new InvalidOperationException(string.Format(
                    "Cannot convert method return type {0} to an Async Task<T> type. Method Name={1} Declaring Type={2}",
                    t.FullName, methodInfo.Name, TypeUtils.GetFullName(methodInfo.DeclaringType)));
            }
            m.ReturnType = CreateCodeTypeReference(t);

            foreach (var param in methodInfo.GetParameters())
            {
                CodeParameterDeclarationExpression p;
                if (param.ParameterType.IsGenericType)
                {
                    p = new CodeParameterDeclarationExpression(TypeUtils.GetParameterizedTemplateName(param.ParameterType, true, tt => currentNamespace != tt.Namespace && !referredNamespaces.Contains(tt.Namespace)),
                        param.Name);
                }
                else
                {
                    p = new CodeParameterDeclarationExpression(param.ParameterType, param.Name);
                }
                p.Direction = FieldDirection.In;
                m.Parameters.Add(p);
            }
            return m;
        }

        internal static CodeTypeReference CreateCodeTypeReference(Type t)
        {
            string baseName = TypeUtils.GetSimpleTypeName(t);
            if (!t.IsGenericParameter) baseName = t.Namespace + "." + baseName;

            var codeRef = new CodeTypeReference(baseName);
            if ((t.IsGenericType || t.IsGenericTypeDefinition))
            {
                foreach (Type gArg in t.GetGenericArguments())
                {
                    codeRef.TypeArguments.Add(CreateCodeTypeReference(gArg));
                }
            }
            return codeRef;
        }

        internal void AddReferenceClass(GrainInterfaceData interfaceData)
        {
            this.grainInterfaceData = interfaceData;

            bool isTaskInterface = GrainInterfaceData.IsTaskBasedInterface(interfaceData.Type);

            CodeTypeParameterCollection genericTypeParam = interfaceData.GenericTypeParams;

            // Declare factory class
            CodeTypeDeclaration factoryClass = new CodeTypeDeclaration(interfaceData.FactoryClassBaseName);
            if (genericTypeParam != null) factoryClass.TypeParameters.AddRange(genericTypeParam);
            factoryClass.IsClass = true;
            factoryClass.TypeAttributes = interfaceData.Type.IsPublic ? TypeAttributes.Public : TypeAttributes.NotPublic;
            MarkAsGeneratedCode(factoryClass);
            AddFactoryMethods(interfaceData, factoryClass);
            AddCastMethods(interfaceData, true, factoryClass);
            if (ShouldGenerateObjectRefFactory(interfaceData)) 
                AddCreateObjectReferenceMethods(interfaceData, factoryClass);
            int factoryClassIndex = referenceNamespace.Types.Add(factoryClass);

            CodeTypeDeclaration referenceClass = new CodeTypeDeclaration(interfaceData.ReferenceClassBaseName);
            if (genericTypeParam != null) referenceClass.TypeParameters.AddRange(genericTypeParam);
            referenceClass.IsClass = true;
            MarkAsGeneratedCode(referenceClass);
            referenceClass.TypeAttributes = TypeAttributes.NestedAssembly;
            referenceClass.CustomAttributes.Add(
                new CodeAttributeDeclaration(new CodeTypeReference(typeof(SerializableAttribute))));

            referenceClass.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(GrainReferenceAttribute)),
                        new CodeAttributeArgument(new CodePrimitiveExpression(interfaceData.Type.Namespace + "." + TypeUtils.GetParameterizedTemplateName((interfaceData.Type)))))
                        );
            CodeConstructor baseReferenceConstructor2 = new CodeConstructor();
            baseReferenceConstructor2.Attributes = MemberAttributes.FamilyOrAssembly;
            baseReferenceConstructor2.Parameters.Add(new CodeParameterDeclarationExpression("GrainReference",
                                                                                            "reference"));
            baseReferenceConstructor2.BaseConstructorArgs.Add(new CodeVariableReferenceExpression("reference"));
            referenceClass.Members.Add(baseReferenceConstructor2);

            CodeConstructor baseReferenceConstructor3 = new CodeConstructor();
            baseReferenceConstructor3.Attributes = MemberAttributes.FamilyOrAssembly;
            baseReferenceConstructor3.Parameters.Add(new CodeParameterDeclarationExpression("SerializationInfo",
                                                                                            "info"));
            baseReferenceConstructor3.BaseConstructorArgs.Add(new CodeVariableReferenceExpression("info"));
            baseReferenceConstructor3.Parameters.Add(new CodeParameterDeclarationExpression("StreamingContext",
                                                                                            "context"));
            baseReferenceConstructor3.BaseConstructorArgs.Add(new CodeVariableReferenceExpression("context"));
            referenceClass.Members.Add(baseReferenceConstructor3);

            // Copier, serializer, and deserializer for this type
            var copier = SerializerGenerationUtilities.GenerateCopier("_Copier", grainInterfaceData.ReferenceClassName, genericTypeParam);
            // return (<DomainReference>)GrainReference.CopyGrainReference(input);
            copier.Statements.Add(new CodeMethodReturnStatement(new CodeCastExpression(grainInterfaceData.ReferenceClassName,
                new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("GrainReference"), "CopyGrainReference", new CodeVariableReferenceExpression("input")))));
            referenceClass.Members.Add(copier);

            var serializer = SerializerGenerationUtilities.GenerateSerializer("_Serializer", grainInterfaceData.ReferenceClassName, genericTypeParam);
            // GrainReference.SerializeGrainReference(obj, stream, typeof(<DomainReference>));
            serializer.Statements.Add(new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("GrainReference"),
                        "SerializeGrainReference", new CodeVariableReferenceExpression("input"), new CodeArgumentReferenceExpression("stream"),
                        new CodeArgumentReferenceExpression("expected")));
            referenceClass.Members.Add(serializer);

            var deserializer = SerializerGenerationUtilities.GenerateDeserializer("_Deserializer", grainInterfaceData.ReferenceClassName, genericTypeParam);
            // return <DomainReference>.Cast((GrainReference)GrainReference.DeserializeGrainReference(expected, stream));
            deserializer.Statements.Add(new CodeMethodReturnStatement(new CodeMethodInvokeExpression(new CodeTypeReferenceExpression(grainInterfaceData.ReferenceClassName),
                        "Cast", new CodeCastExpression(typeof(GrainReference), new CodeMethodInvokeExpression(new CodeTypeReferenceExpression("GrainReference"),
                            "DeserializeGrainReference", new CodeArgumentReferenceExpression("expected"), new CodeArgumentReferenceExpression("stream"))))));
            referenceClass.Members.Add(deserializer);

            // this private class is the "implementation class" for the interface reference type
            referenceNamespace.Types[factoryClassIndex].Members.Add(referenceClass);

            AddCastMethods(interfaceData, false, referenceClass);

            var interfaceId = GrainInterfaceData.GetGrainInterfaceId(interfaceData.Type);
            var interfaceIdMethod =
                String.Format(@"
                protected override int InterfaceId {{ get {{ return {0}; }} }}",
                              interfaceId);
            referenceClass.Members.Add(new CodeSnippetTypeMember(interfaceIdMethod));

            var interfaceIsCompatibleCheckCode = new StringBuilder("interfaceId == this.InterfaceId");
            var interfaceList = GrainInterfaceData.GetServiceInterfaces(interfaceData.Type);
            foreach (int iid in interfaceList.Keys)
            {
                if (iid == interfaceId) continue; // already covered the main interfaces

                interfaceIsCompatibleCheckCode.Append(" || interfaceId == ").Append(iid);
            }
            var interfaceIsCompatibleMethod =
                String.Format(@"
                public override bool IsCompatible(int interfaceId) {{ return {0}; }}",
                              interfaceIsCompatibleCheckCode);
            referenceClass.Members.Add(new CodeSnippetTypeMember(interfaceIsCompatibleMethod));

            var interfaceNameMethod =
                String.Format(
                    @"
                protected override string InterfaceName {{ get {{ return ""{0}""; }} }}",
                    interfaceData.TypeFullName);
            referenceClass.Members.Add(new CodeSnippetTypeMember(interfaceNameMethod));

            string invokerClassName = interfaceData.InvokerClassName;
            var methodName =
                String.Format(
                    @"
                protected override string GetMethodName(int interfaceId, int methodId) {{ return {0}.{1}; }}",
                    invokerClassName, "GetMethodName(interfaceId, methodId)");
            referenceClass.Members.Add(new CodeSnippetTypeMember(methodName));

            referenceClass.BaseTypes.Add(typeof(GrainReference));
            referenceClass.BaseTypes.Add(interfaceData.InterfaceTypeName);
            referenceClass.BaseTypes.Add(typeof(IAddressable));

            CodeTypeDeclaration invokerClass = GetInvokerClass(interfaceData, true);
            invokerClass.TypeAttributes = TypeAttributes.NotPublic;
            referenceNamespace.Types.Add(invokerClass);

            MethodInfo[] methods = GrainInterfaceData.GetMethods(interfaceData.Type);

            referenceNamespace.Imports.Add(new CodeNamespaceImport("System"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("System.Net"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("System.Runtime.Serialization"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("System.Runtime.Serialization.Formatters.Binary"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("System.IO"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("System.Collections.Generic"));
            referenceNamespace.Imports.Add(new CodeNamespaceImport("Orleans"));
            
            // recursively add serializable types to code type declaration
            AddMethods(methods, referenceClass, isTaskInterface);

            if (RequiresPropertiesClass(interfaceData, true))
            {
                // Add extra GetProperties method
                string propertiesInterface = GetParameterisedTypeFrom(interfaceData.PropertiesClassBaseName, genericTypeParam);
                // add the method for getting properties 
                string getPropertiesMethodName = string.Format(@"
            public async System.Threading.Tasks.Task<{0}> GetProperties() 
            {{ 
                return await base.InvokeMethodAsync<{0}>( {1}, new object[]{{}}, TimeSpan.Zero);
            }}
            ", propertiesInterface, Utils.CalculateIdHash(GetPropertiesMethodName));
                referenceClass.Members.Add(new CodeSnippetTypeMember(getPropertiesMethodName));
            }

            // add extension methods
            if (!interfaceData.IsGeneric)
            {
                AddExtensionMethodClass();
            }
        }

        private static bool ShouldGenerateObjectRefFactory(GrainInterfaceData ifaceData)
        {
            var ifaceType = ifaceData.Type;
            // generate CreateObjectReference in 2 cases:
            // 1) for interfaces derived from IGrainObserver
            // 2) when specifically specifies via FactoryTypes.ClientObject or FactoryTypes.Both 
            bool isObserver = typeof(IGrainObserver).IsAssignableFrom(ifaceType);
            if (isObserver)
            {
                return true;
            }
            var factoryType = FactoryAttribute.CollectFactoryTypesSpecified(ifaceType);
            return factoryType == FactoryAttribute.FactoryTypes.ClientObject || factoryType == FactoryAttribute.FactoryTypes.Both;
        }

        private static void AddCreateObjectReferenceMethods(GrainInterfaceData grainInterfaceData, CodeTypeDeclaration factoryClass)
        {
            string fieldImpl = @"
        private static IGrainMethodInvoker methodInvoker;";
            CodeSnippetTypeMember invokerField = new CodeSnippetTypeMember(fieldImpl);
            factoryClass.Members.Add(invokerField);

            string methodImpl = String.Format(@"
        public async static System.Threading.Tasks.Task<{0}> CreateObjectReference({0} obj)
        {{
            if (methodInvoker == null) methodInvoker = new {2}();
            return {1}.Cast(await GrainReference.CreateObjectReference(obj, methodInvoker));
        }}",
            grainInterfaceData.ServiceTypeName,
            grainInterfaceData.FactoryClassName,
            grainInterfaceData.InvokerClassName);
            CodeSnippetTypeMember createObjectReferenceMethod = new CodeSnippetTypeMember(methodImpl);
            factoryClass.Members.Add(createObjectReferenceMethod);

            methodImpl = String.Format(@"
        public static void DeleteObjectReference({0} reference)
        {{
            GrainReference.DeleteObjectReference(reference);
        }}",
            grainInterfaceData.ServiceTypeName);
            CodeSnippetTypeMember deleteObjectReferenceMethod = new CodeSnippetTypeMember(methodImpl);
            factoryClass.Members.Add(deleteObjectReferenceMethod);
        }

        private void AddExtensionMethodClass()
        {
            CodeTypeDeclaration extensionMethodsClass = new CodeTypeDeclaration();

            extensionMethodsClass.IsClass = true;
            extensionMethodsClass.Name = TypeUtils.GetSimpleTypeName(grainInterfaceData.ExtensionMethodsClassName);
            extensionMethodsClass.TypeAttributes = TypeAttributes.Public | TypeAttributes.Class;
            MarkAsGeneratedCode(extensionMethodsClass);
            MethodInfo[] methods = GrainInterfaceData.GetMethods(grainInterfaceData.Type);
            if(AddExtensionMethods(methods, grainInterfaceData, extensionMethodsClass, true))
                referenceNamespace.Types.Add(extensionMethodsClass);
            extensionMethodsClass.CustomAttributes.Add(new CodeAttributeDeclaration(new CodeTypeReference(typeof(SerializableAttribute))));
        }

        private bool AddExtensionMethods(MethodInfo[] methods, GrainInterfaceData si, CodeTypeDeclaration referenceClass, bool includeImpl)
        {
            if (RequiresPropertiesClass(si, true))
            {
                // add the method for getting properties - client side
                string propertiesInterface = GetParameterisedTypeFrom(si.PropertiesClassBaseName, si.GenericTypeParams);
                string getPropertiesMethodName =
                    string.Format(@"static {3} System.Threading.Tasks.Task<{0}> GetProperties(this {1} interfaceData) 
            {{ 
                {2} gref = interfaceData as {2};
                 return gref.GetProperties();
            }}
            ", propertiesInterface, si.ServiceTypeName, si.FactoryClassBaseName + "." + si.ReferenceClassBaseName,
                si.Type.IsPublic ? "public" : "internal");
                referenceClass.Members.Add(new CodeSnippetTypeMember(getPropertiesMethodName));
                return true;
            }
            else
                return false;
        }

        private void AddMethods(MethodInfo[] methods, CodeTypeDeclaration referenceClass, bool isTaskinterface)
        {
            methodIdCollisionDetection.Clear();
            if (methods != null && methods.Length > 0)
            {
                foreach (MethodInfo methodInfo in methods)
                {
                    AddMethod(methodInfo, referenceClass, isTaskinterface);
                }
            }
        }


        private void AddMethod(MethodInfo methodInfo, CodeTypeDeclaration referenceClass, bool isTaskInterface)
        {
            bool notAsyncReturnType;
            if (methodInfo.IsStatic || IsSpecialEventMethod(methodInfo))
                return; // skip such methods            

            int methodId = GrainInterfaceData.ComputeMethodId(methodInfo);
            if (methodIdCollisionDetection.Contains(methodId))
            {
                ReportErrorAndThrow(
                    string.Format("Collision detected for method {0}, declaring type {1}, consider renaming method name", methodInfo.Name, methodInfo.DeclaringType.FullName));
            }
            else if (IsGetPropertyMethod(methodInfo))
            {
                var code = GetGetProperty(methodInfo, isTaskInterface, out notAsyncReturnType);

                referenceClass.Members.Add(code);
            }
            else
            {
                string name = IsSetPropertyMethod(methodInfo) ? "Set_" + methodInfo.Name.Substring(4) : null;
                var code = GetBasicReferenceMethod(methodInfo, name, isTaskInterface, isTaskInterface, out notAsyncReturnType);

                referenceClass.Members.Add(code); // method with original argument types

                methodIdCollisionDetection.Add(methodId);
            }
            if (typeof(IAddressable).IsAssignableFrom(methodInfo.ReturnType))
            {
                ReferredNamespaceAndAssembly(methodInfo.ReturnType);
            }
        }

        #region utility methods

        private static void ReportErrorAndThrow(string errorMsg)
        {
            ConsoleText.WriteError("Orleans code generator found error: " + errorMsg);
            throw new OrleansException(errorMsg);
        }
        private void AddFactoryMethods(GrainInterfaceData si, CodeTypeDeclaration factoryClass)
        {
            ReferredNamespaceAndAssembly(si.Type);

            if (GrainInterfaceData.IsGrainType(si.Type) && ShouldGenerateGetGrainMethods(si.Type))
            {
                this.AddGetGrainMethods(si, factoryClass);
            }
        }

        private static bool ShouldGenerateGetGrainMethods(Type type)
        {
            // [mlr] we don't generate these methods if this is a client object factory.
            var factoryType = FactoryAttribute.CollectFactoryTypesSpecified(type);
            return factoryType != FactoryAttribute.FactoryTypes.ClientObject;
        }
        
        private void AddGetGrainMethods(GrainInterfaceData iface, CodeTypeDeclaration factoryClass)
        {
            ReferredNamespaceAndAssembly(typeof(GrainId));
            ReferredNamespaceAndAssembly(iface.Type);
            var interfaceId = GrainInterfaceData.GetGrainInterfaceId(iface.Type);
            Action<string> add =
                codeFmt =>
                    factoryClass.Members.Add(
                        new CodeSnippetTypeMember(
                            String.Format(codeFmt, iface.InterfaceTypeName, interfaceId)));

            PlacementStrategy placement = GrainInterfaceData.GetPlacementStrategy(iface.Type);
            bool hasExplicitPlacement = Object.Equals(placement, ExplicitPlacement.Tbd);
            bool hasKeyExt = GrainInterfaceData.UsesPrimaryKeyExtension(iface.Type);
            if (hasKeyExt && hasExplicitPlacement)
                throw new NotSupportedException("Orleans currently does not support using explicit placement with the primary key extension.");

            if (hasExplicitPlacement)
            {
                // [mlr] the programmer has specified [ExplicitPlacement] on the interface.
                // note: ExplicitPlacement.Tbd serves as a placeholder in the type information to indicate that
                // the programmer intends to specify arguments during a call to the generated GetGrain() method.
                // The actual ExplicitPlacement object used is generated in the call to 
                // GrainFactoryBase.MakeGrainReferenceInternal.
                add(
                    @"
                        public static {0} GetGrain(long primaryKey, IPEndPoint placeOnSilo)
                        {{
                            return Cast(GrainFactoryBase.MakeExplicitlyPlacedGrainReferenceInternal(typeof({0}), {1}, primaryKey, placeOnSilo));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(long primaryKey, IPEndPoint placeOnSilo, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeExplicitlyPlacedGrainReferenceInternal(typeof({0}), {1}, primaryKey, placeOnSilo, grainClassNamePrefix));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey, IPEndPoint placeOnSilo)
                        {{
                            return Cast(GrainFactoryBase.MakeExplicitlyPlacedGrainReferenceInternal(typeof({0}), {1}, primaryKey, placeOnSilo));
                        }}"); 
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey, IPEndPoint placeOnSilo, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeExplicitlyPlacedGrainReferenceInternal(typeof({0}), {1}, primaryKey, placeOnSilo, grainClassNamePrefix));
                        }}");
            }
            else if (hasKeyExt)
            {
                // [mlr] the programmer has specified [ExtendedPrimaryKey] on the interface.
                add(
                    @"
                        public static {0} GetGrain(long primaryKey, string keyExt)
                        {{
                            return Cast(GrainFactoryBase.MakeKeyExtendedGrainReferenceInternal(typeof({0}), {1}, primaryKey, keyExt));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(long primaryKey, string keyExt, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeKeyExtendedGrainReferenceInternal(typeof({0}), {1}, primaryKey, keyExt, grainClassNamePrefix));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey, string keyExt)
                        {{
                            return Cast(GrainFactoryBase.MakeKeyExtendedGrainReferenceInternal(typeof({0}), {1}, primaryKey, keyExt));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey, string keyExt, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeKeyExtendedGrainReferenceInternal(typeof({0}), {1}, primaryKey, keyExt,grainClassNamePrefix));
                        }}");
            }
            else
            {
                // [mlr] the programmer has not specified [ExplicitPlacement] on the interface nor [ExtendedPrimaryKey].
                add(
                    @"
                        public static {0} GetGrain(long primaryKey)
                        {{
                            return Cast(GrainFactoryBase.MakeGrainReferenceInternal(typeof({0}), {1}, primaryKey));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(long primaryKey, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeGrainReferenceInternal(typeof({0}), {1}, primaryKey, grainClassNamePrefix));
                        }}");
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey)
                        {{
                            return Cast(GrainFactoryBase.MakeGrainReferenceInternal(typeof({0}), {1}, primaryKey));
                        }}"); 
                add(
                    @"
                        public static {0} GetGrain(Guid primaryKey, string grainClassNamePrefix)
                        {{
                            return Cast(GrainFactoryBase.MakeGrainReferenceInternal(typeof({0}), {1}, primaryKey, grainClassNamePrefix));
                        }}");
            }
        }


        /// <summary>
        /// Generate Cast method in CodeDom and add it in reference class
        /// </summary>
        /// <param name="si">The service interface this grain reference type is being generated for</param>
        /// <param name="isFactory">whether the class being generated is a factory class rather than a grainref implementation</param>
        /// <param name="referenceClass">The class being generated for this grain reference type</param>
        private static void AddCastMethods(GrainInterfaceData si, bool isFactory, CodeTypeDeclaration referenceClass)
        {
            string castImplCode;
            string checkCode = null;
            if (isFactory)
            {
                castImplCode = string.Format(
                    @"{0}.Cast(grainRef)",
                    si.ReferenceClassName);

                if (si.IsSystemTarget)
                    checkCode =
                        @"if(!((GrainReference)grainRef).IsInitializedSystemTarget)
                            throw new InvalidOperationException(""InvalidCastException cast of a system target grain reference. Must have SystemTargetSilo set to the target silo address"");";
            }
            else
            {
                castImplCode = string.Format(
                    @"({0}) GrainReference.CastInternal(typeof({0}), (GrainReference gr) => {{ return new {1}(gr);}}, grainRef, {2})",
                    si.InterfaceTypeName, // Interface type for references for this grain
                    si.ReferenceClassName, // Concrete class for references for this grain
                    GrainInterfaceData.GetGrainInterfaceId(si.Type));
            }
            string methodImpl = string.Format(@"
            {3} static {0} Cast(IAddressable grainRef)
            {{
                {1}
                return {2};
            }}",
                si.InterfaceTypeName,
                checkCode,
                castImplCode,
                "public");

            string getSystemTarget = null;
            if (isFactory && si.IsSystemTarget)
            {
                getSystemTarget = string.Format(@"
            internal static {0} GetSystemTarget(GrainId grainId, SiloAddress silo)
            {{
                return GrainReference.GetSystemTarget(grainId, silo, Cast);
            }}",
                si.InterfaceTypeName);
            }

            var castMethod = new CodeSnippetTypeMember(methodImpl + getSystemTarget);
            referenceClass.Members.Add(castMethod);
        }

        private static bool NeedsArgumentsWrapped(MethodInfo methodInfo)
        {
            foreach (ParameterInfo paramInfo in methodInfo.GetParameters())
                if (!paramInfo.ParameterType.IsSubclassOf(typeof(AsyncCompletion)) && !GrainInterfaceData.IsGrainReference(paramInfo.ParameterType))
                    return true;

            return false;
        }

        internal static bool IsGetPropertyMethod(MethodInfo methodInfo)
        {
            return methodInfo.IsSpecialName && methodInfo.Name.StartsWith("get_");
        }

        internal static bool IsSetPropertyMethod(MethodInfo methodInfo)
        {
            return methodInfo.IsSpecialName && methodInfo.Name.StartsWith("set_");
        }

        /// <summary>
        /// Decide whether this grain method is declared in this grain dll file
        /// </summary>
        private bool IsDeclaredHere(Type type)
        {
            return type.Assembly.Equals(this.grainAssembly);
        }

        /// <summary>
        /// Find the namespace of type t and the assembly file in which type t is defined
        /// Add these in lists. Later this information is used to compile grain client
        /// </summary>
        new internal void ReferredNamespaceAndAssembly(Type t)
        {
            ReferredNamespaceAndAssembly(t, true);
        }

        private void ReferredNamespaceAndAssembly(Type t, bool addNamespace)
        {
            ReferredNamespaceAndAssembly(addNamespace ? t.Namespace : null, t.Assembly.GetName().Name);
            var indirect = t.GetInterfaces().ToList();
            for (var parent = t.BaseType; typeof(GrainBase).IsAssignableFrom(parent); parent = parent.BaseType)
            {
                indirect.Add(parent);
            }
            foreach (var t2 in indirect)
            {
                ReferredNamespaceAndAssembly(addNamespace ? t2.Namespace : null, t2.Assembly.GetName().Name);
            }
        }

        internal void ReferredAssembly(Type t)
        {
            ReferredNamespaceAndAssembly(t, false);
        }

        private void ReferredNamespaceAndAssembly(string nspace, string assembly)
        {
            if(!String.IsNullOrEmpty((nspace)))
                if (!referredNamespaces.Contains(nspace) && referenceNamespace.Name != nspace)
                    referredNamespaces.Add(nspace);

            if (!referredAssemblies.Contains(assembly) && grainAssembly.GetName().Name + "Client" != assembly)
                referredAssemblies.Add(assembly);
        }

        /// <summary>
        /// Get the name string for generic type
        /// </summary>
        private string GetGenericTypeName(Type type, SerializeFlag flag = SerializeFlag.NoSerialize, bool includeImpl = true)
        {
            return GetGenericTypeName(type, ReferredNamespaceAndAssembly,
                t=> flag != SerializeFlag.NoSerialize && IsDeclaredHere(type) && includeImpl);
        }

        /// <summary>
        /// Get the name string for generic type
        /// </summary>
        private CodeTypeReference GetGenericTypeNameImpl(Type type, SerializeFlag flag, bool includeImpl = true)
        {
            bool IsSerializable = false;

            if (flag == SerializeFlag.NoSerialize || !IsDeclaredHere(type))
            {
                ReferredNamespaceAndAssembly(type);
            }
            else if (includeImpl)
            {
                IsSerializable = true;
            }

            if (!type.IsGenericType)
            {
                int length = type.Namespace.Length;
                string typeName = GetNestedClassName(type.FullName, IsSerializable);
                return new CodeTypeReference(typeName);
            }
            else
            {
                string name = TypeUtils.GetSimpleTypeName(type.FullName);
                List<CodeTypeReference> codeReferences = new List<CodeTypeReference>();
                foreach (Type argument in type.GetGenericArguments())
                {
                    codeReferences.Add(GetGenericTypeNameImpl(argument, flag, includeImpl));
                }
                return new CodeTypeReference(GetNestedClassName(name, IsSerializable), codeReferences.ToArray());
            }
        }

        private CodeMemberProperty GetGetProperty(MethodInfo methodInfo, bool asTask, out bool notAsyncType)
        {
            CodeMemberProperty referenceProperty;
            if (asTask)
            {
                notAsyncType = false;
                PropertyInfo prop = methodInfo.DeclaringType.GetProperties().Where(p => p.GetGetMethod() == methodInfo).FirstOrDefault();
                referenceProperty = ConvertToTaskAsyncProperty(prop);
                referenceProperty.Attributes = MemberAttributes.Public | MemberAttributes.Final;
            }
            else
            {
                referenceProperty = new CodeMemberProperty
                {
                    Attributes = MemberAttributes.Public | MemberAttributes.Final,
                    Name = methodInfo.Name.Substring(4),
                    Type = GetReturnTypeName(methodInfo.ReturnType, SerializeFlag.DeserializeResult, out notAsyncType),
                    HasGet = true
                };
                SerializerGenerationManager.RecordTypeToGenerate(methodInfo.ReturnType);

                foreach (ParameterInfo paramInfo in methodInfo.GetParameters())
                {
                    referenceProperty.Parameters.Add(new CodeParameterDeclarationExpression(GetGenericTypeName(paramInfo.ParameterType, SerializeFlag.SerializeArgument), GrainInterfaceData.GetParameterName(paramInfo)));
                    SerializerGenerationManager.RecordTypeToGenerate(paramInfo.ParameterType);
                }
            }

            CodeSnippetStatement methodImpl = new CodeSnippetStatement(GetBasicMethodImpl(methodInfo, asTask, true));
            referenceProperty.GetStatements.Add(methodImpl);
            return referenceProperty;
        }

        /// <summary>
        /// Generates a wrapper method that takes arguments of the original method.
        /// </summary>
        CodeMemberMethod GetBasicReferenceMethod(MethodInfo methodInfo, string name, bool asTask, bool isTaskInterface, out bool notAsyncType)
        {
            SerializerGenerationManager.RecordTypeToGenerate(methodInfo.ReturnType);
            foreach (ParameterInfo paramInfo in methodInfo.GetParameters())
            {
                SerializerGenerationManager.RecordTypeToGenerate(paramInfo.ParameterType);
            }

            CodeMemberMethod referenceMethod;
            if (asTask)
            {
                notAsyncType = false;
                referenceMethod = ConvertToTaskAsyncMethod(methodInfo, isTaskInterface);
            }
            else
            {
                referenceMethod = new CodeMemberMethod();
                referenceMethod.Name = name ?? methodInfo.Name;
                if (IsSetPropertyMethod(methodInfo))
                {
                    Type returnType = isTaskInterface ? typeof(Task) : typeof(AsyncCompletion);
                    referenceMethod.ReturnType = new CodeTypeReference(returnType); // property setters should return AsyncCompletion or Task
                    notAsyncType = false;
                }
                else
                {
                    referenceMethod.ReturnType = GetReturnTypeName(methodInfo.ReturnType, SerializeFlag.DeserializeResult, out notAsyncType);
                }

                foreach (ParameterInfo paramInfo in methodInfo.GetParameters())
                {
                    referenceMethod.Parameters.Add(new CodeParameterDeclarationExpression(GetGenericTypeName(paramInfo.ParameterType, SerializeFlag.SerializeArgument), GrainInterfaceData.GetParameterName(paramInfo)));
                }
            }
            referenceMethod.Attributes = MemberAttributes.Public | MemberAttributes.Final;

            CodeSnippetStatement methodImpl = new CodeSnippetStatement(GetBasicMethodImpl(methodInfo, asTask, false));
            referenceMethod.Statements.Add(methodImpl);
            return referenceMethod;
        }

        /// <summary>
        /// Generate reference method body with original argument types
        /// </summary>
        string GetBasicMethodImpl(MethodInfo methodInfo, bool asTaskInterface, bool isProperty)
        {
            string invokeArguments = GetInvokeArguments(methodInfo);

            int methodId = GrainInterfaceData.ComputeMethodId(methodInfo);
            string methodImpl;
            string optional = null;
            if (GrainInterfaceData.IsReadOnly(methodInfo))
            {
                optional = ", options: InvokeMethodOptions.ReadOnly";
            }
            if (GrainInterfaceData.IsUnordered(methodInfo))
            {
                if (optional == null)
                    optional = ", options: ";
                else
                    optional += " | ";

                optional += " InvokeMethodOptions.Unordered";
            }
            if (GrainInterfaceData.IsAlwaysInterleave(methodInfo))
            {
                if (optional == null)
                    optional = ", options: ";
                else
                    optional += " | ";

                optional += " InvokeMethodOptions.AlwaysInterleave";
            }
            // TODO: remove this additional param to InvokeMethod. It is redundnat. For now keep for backward compatability.
            if (typeof(ISystemTarget).IsAssignableFrom(methodInfo.DeclaringType))
            {
                optional += ", silo: this.SystemTargetSilo";
            }

            if (methodInfo.ReturnType == typeof(void))
            {
                methodImpl = string.Format(@"
                base.InvokeOneWayMethod({0}, new object[] {{{1}}} {2});",
                methodId, invokeArguments, optional);
            }
            else if (asTaskInterface)
            {
                if (isProperty)
                {
                    // Handle any Cacheable properties - Only allowed for Task grains
                    string cacheDurationStmt = @"TimeSpan.Zero";
                    var cacheTime = GrainInterfaceData.CacheableDuration(methodInfo);
                    if (cacheTime != TimeSpan.Zero)
                    {
                        Console.WriteLine(methodInfo.DeclaringType.FullName + "::" + methodInfo.Name +
                                          " - Cacheable Duration = " + cacheTime);
                        cacheDurationStmt = "TimeSpan.FromTicks(" + cacheTime.Ticks + ")";
                    }

                    methodImpl = string.Format(@"
                return base.InvokeMethodAsync<{0}>({1}, new object[] {{{2}}}, {3} {4});",
                        GetActualMethodReturnType(methodInfo.ReturnType, SerializeFlag.NoSerialize),
                        methodId,
                        invokeArguments,
                        cacheDurationStmt,
                        optional);

                }
                else if (methodInfo.ReturnType == typeof(Task))
                {
                    methodImpl = string.Format(@"
                return base.InvokeMethodAsync<object>({0}, new object[] {{{1}}}, TimeSpan.Zero {2});",
                        methodId,
                        invokeArguments,
                        optional);
                }
                else
                {
                    methodImpl = string.Format(@"
                return base.InvokeMethodAsync<{0}>({1}, new object[] {{{2}}}, TimeSpan.Zero {3});",
                        GetActualMethodReturnType(methodInfo.ReturnType, SerializeFlag.NoSerialize),
                        methodId,
                        invokeArguments,
                        optional);
                }
            }
            else // AsyncValue / AsyncCompletion
            {
                string invokeMethodStmt = string.Format(@"AsyncValue<object> __invoke = base.InvokeMethod({0}, new object[] {{{1}}} {2});",
                    methodId,
                    invokeArguments,
                    optional);

                string returnStmt;
                if (methodInfo.ReturnType == typeof(AsyncCompletion))
                {
                    // if it's AsyncCompletion, don't need to bother deserializing resulting byte[]
                    returnStmt = @"return __invoke;";
                }
                else
                {
                    returnStmt = string.Format(@"return __invoke.ContinueWith<{0}>( (object __res) => {{ return ({0}) __res; }});",
                        GetActualMethodReturnType(methodInfo.ReturnType, SerializeFlag.NoSerialize));
                }
                methodImpl = String.Format(@"
                {0}
                {1}",
                    invokeMethodStmt, returnStmt);
            }
            return GetParamGuardCheckStatements(methodInfo) + methodImpl;
        }

        /// <summary>
        /// Generate any safeguard check statements for the generated Invoke for the specified method
        /// </summary>
        /// <param name="methodInfo">The method for which the invoke is being generated for </param>
        /// <returns></returns>
        private static string GetParamGuardCheckStatements(MethodInfo methodInfo)
        {
            StringBuilder paramGuardStatements = new StringBuilder();
            foreach (ParameterInfo p in methodInfo.GetParameters())
            {
                // For any parameters of type IGrainObjerver, the object passed at runtime must also be a GrainReference
                if (typeof(IGrainObserver).IsAssignableFrom(p.ParameterType))
                {
                    paramGuardStatements.AppendLine(
                        string.Format(@"GrainFactoryBase.CheckGrainObserverParamInternal({0});", GrainInterfaceData.GetParameterName(p)));
                }
            }
            return paramGuardStatements.ToString();
        }

        private static string GetInvokeArguments(MethodInfo methodInfo)
        {
            var system = GrainInterfaceData.IsSystemTargetType(methodInfo.DeclaringType);
            string invokeArguments = string.Empty;
            int count = 1;
            var parameters = methodInfo.GetParameters();
            foreach (ParameterInfo paramInfo in parameters)
            {
                if (paramInfo.ParameterType.GetInterface("Orleans.IAddressable") != null && !typeof(GrainReference).IsAssignableFrom(paramInfo.ParameterType))
                    invokeArguments += string.Format("{0} is GrainBase ? {2}.{1}.Cast({0}.AsReference()) : {0}", 
                        GrainInterfaceData.GetParameterName(paramInfo), 
                        GrainInterfaceData.GetFactoryClassForInterface(paramInfo.ParameterType),
                        paramInfo.ParameterType.Namespace);
                else
                    invokeArguments += GrainInterfaceData.GetParameterName(paramInfo);
                if (count++ < parameters.Length)
                    invokeArguments += ", ";
            }
            return invokeArguments;
        }

        /// <summary>
        /// Gets the name of the result type differentiating promises from normal types. For promises it returns the type of the promised value instead of the promises type itself.
        /// </summary>
        private string GetActualMethodReturnType(Type type, SerializeFlag flag)
        {
            if (!type.IsGenericType)
                return GetGenericTypeName(type, flag);

            Type typeDefinition = type.GetGenericTypeDefinition();
            if (typeDefinition.BaseType == typeof(AsyncCompletion))
            {
                if (typeDefinition.FullName != "Orleans.AsyncValue`1")
                {
                    string errorMsg = String.Format("Unexpected generic type {0} used as a return type. Only AsyncValue<T> are supported as generic return types of grain methods.", type);
                    ConsoleText.WriteError(errorMsg);
                    throw new ApplicationException(errorMsg);
                }

                Type[] genericArguments = type.GetGenericArguments();
                if (genericArguments.Length != 1)
                {
                    string errorMsg = String.Format("Unexpected number of arguments {0} for generic type {1} used as a return type. Only AsyncValue<T> are supported as generic return types of grain methods.", genericArguments.Length, type);
                    ConsoleText.WriteError(errorMsg);
                    throw new ApplicationException(errorMsg);
                }

                return GetGenericTypeName(genericArguments[0], flag);
            }
            else if (GrainInterfaceData.IsTaskType(type))
            {
                Type[] genericArguments = type.GetGenericArguments();
                if (genericArguments.Length != 1)
                {
                    string errorMsg = String.Format("Unexpected number of arguments {0} for generic type {1} used as a return type. Only Type<T> are supported as generic return types of grain methods.", genericArguments.Length, type);
                    ConsoleText.WriteError(errorMsg);
                    throw new ApplicationException(errorMsg);
                }

                return GetGenericTypeName(genericArguments[0], flag);
            }
            else
                return GetGenericTypeName(type, flag);
        }


        private CodeTypeReference GetReturnTypeName(Type type, SerializeFlag flag, out bool notAsyncType)
        {
            notAsyncType = !(typeof(AsyncCompletion).IsAssignableFrom(type) || typeof(IAddressable).IsAssignableFrom(type));
            if (type.Equals(typeof(void)))
                return new CodeTypeReference(typeof(void));
            else if (type.Equals(typeof(AsyncCompletion)))
                return new CodeTypeReference(typeof(AsyncCompletion));
            else if (type.IsSubclassOf(typeof(AsyncCompletion)))
                return new CodeTypeReference(GetGenericTypeName(type, flag));
            else if (GrainInterfaceData.IsTaskType(type))
                return new CodeTypeReference(GetGenericTypeName(type, flag));
            else if (GrainInterfaceData.IsGrainReference(type))
                return new CodeTypeReference(GetGenericTypeName(type, flag));
            else
                return new CodeTypeReference("AsyncValue", new CodeTypeReference[] { GetGenericTypeNameImpl(type, flag) });
        }

        private CodeTypeReference GetReturnTypeNameAsTask(Type type, SerializeFlag flag)
        {
            if (type.Equals(typeof(void)))
                return new CodeTypeReference(typeof(void));
            else if (type.Equals(typeof(AsyncCompletion)))
                return new CodeTypeReference("Task");
            else if (type.IsSubclassOf(typeof(AsyncCompletion)))
                return new CodeTypeReference(GetGenericTypeName(type, flag));
            else if (GrainInterfaceData.IsGrainReference(type))
                return new CodeTypeReference(GetGenericTypeName(type, flag));
            else
                return new CodeTypeReference("Task", new CodeTypeReference[] { GetGenericTypeNameImpl(type, flag) });
        }

        #endregion
    }
}
