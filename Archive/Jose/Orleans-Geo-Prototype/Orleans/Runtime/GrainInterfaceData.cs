using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Orleans;



namespace GrainClientGenerator
{
    internal class GrainInterfaceData
    {
        public Type Type { get; private set; }
        public bool IsGeneric { get; private set; }
        public bool IsTaskGrain { get; private set; }

        public bool IsExtension
        {
            get { return typeof (IGrainExtension).IsAssignableFrom(Type); }
        }

        public CodeTypeParameterCollection GenericTypeParams { get; private set; }
        public string Name { get; private set; }
        public string Namespace { get; private set; }
        public string ServiceTypeName { get; private set; }
        public string FactoryClassBaseName { get; private set; }

        public string FactoryClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(FactoryClassBaseName, Type); }
        }

        public string ReferenceClassBaseName { get; set; }

        public string ReferenceClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(ReferenceClassBaseName, Type); }
        }

        public string InterfaceTypeName
        {
            get { return TypeUtils.GetParameterizedTemplateName(Type); }
        }

        public string TaskReferenceClassBaseName { get; private set; }

        public string TaskReferenceClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(TaskReferenceClassBaseName, Type); }
        }

        public string TaskRemoteInterfaceTypeBaseName { get; private set; }

        public string TaskRemoteInterfaceTypeName
        {
            get { return TypeUtils.GetParameterizedTemplateName(TaskRemoteInterfaceTypeBaseName, Type); }
        }

        public string GrainFactoryClassBaseName { get; internal set; }

        public string GrainFactoryClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(GrainFactoryClassBaseName, Type); }
        }

        public string StateClassBaseName { get; internal set; }

        public string StateClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(StateClassBaseName, Type); }
        }

        public Type StateObjectType { get; internal set; }

        public string PropertiesClassBaseName { get; private set; }

        public string PropertiesClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(PropertiesClassBaseName, Type); }
        }

        public string ActivationClassBaseName { get; internal set; }

        public string ActivationClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(ActivationClassBaseName, Type); }
        }

        public string AbstractImplementationClassBaseName { get; private set; }

        public string AbstractImplementationClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(AbstractImplementationClassBaseName, Type); }
        }

        public string InvokerClassBaseName { get; internal set; }

        public string InvokerClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(InvokerClassBaseName, Type); }
        }

        public string ExtensionMethodsClassBaseName { get; internal set; }

        public string ExtensionMethodsClassName
        {
            get { return TypeUtils.GetParameterizedTemplateName(ExtensionMethodsClassBaseName, Type); }
        }

        public string TypeFullName
        {
            get { return Namespace + "." + TypeUtils.GetParameterizedTemplateName(Type); }
        }

        public const string ActivationClassNameSuffix = "Activation";
        public const string ActivationDllSuffix = "Activation";
        public const string ClientDllSuffix = "Client";

        public GrainInterfaceData()
        {
        }

        public GrainInterfaceData(Type type)
        {
            if (!IsGrainInterface(type))
                throw new ArgumentException(String.Format("{0} is not a grain interface", type.FullName));

            List<string> violations;

            bool ok = ValidateInterfaceRules(type, out violations);

            if (!ok && violations != null && violations.Count > 0)
            {
                StringBuilder msg = new StringBuilder();

                foreach (string violation in violations)
                {
                    msg.Append(violation);
                    msg.Append("\n");
                }

                throw new ArgumentException(
                    String.Format("Interface {0} does not conform to the grain interface rules: \n{1}", type, msg));
            }

            this.Type = type;

            this.IsTaskGrain = IsTaskBasedInterface(type);

            this.DefineClassNames(true);
        }

        internal static bool IsTaskBasedInterface(Type type)
        {
            var methods = type.GetMethods();
            var properties = type.GetProperties();
            // An interface is task-based if it has no methods or properties that return an AsyncCompletion/AsyncValue,
            // and either has at least one method or property that returns a Task or at least one parent that's task-based.
            if (methods.Any(m => typeof (AsyncCompletion).IsAssignableFrom(m.ReturnType))
                || properties.Any(p => typeof (AsyncCompletion).IsAssignableFrom(p.PropertyType)))
            {
                return false;
            }
            if (methods.Any(m => IsTaskType(m.ReturnType)) || properties.Any(p => IsTaskType(p.PropertyType)))
            {
                return true;
            }
            if (type.GetInterfaces().Any(t => IsTaskBasedInterface(t)))
            {
                return true;
            }
            return false;
        }

        internal void SetType(Type type)
        {
            this.Type = type;

            this.DefineClassNames(false);
        }

        public static GrainInterfaceData FromGrainClass(Type grainType)
        {
            GrainInterfaceData si = new GrainInterfaceData();
            si.Type = grainType;
            si.DefineClassNames(false);
            return si;
        }

        private void DefineClassNames(bool client)
        {
            var typeNameBase = TypeUtils.GetSimpleTypeName(Type, t => false);
            if (Type.IsInterface && typeNameBase.Length > 1 && typeNameBase[0] == 'I' && Char.IsUpper(typeNameBase[1]))
            {
                typeNameBase = typeNameBase.Substring(1);
            }

            Namespace = Type.Namespace;
            IsGeneric = Type.IsGenericType;
            if (IsGeneric)
            {
                Name = TypeUtils.GetParameterizedTemplateName(Type);
                GenericTypeParams = TypeUtils.GenericTypeParameters(Type);
            }
            else
            {
                Name = Type.Name;
            }
            if (client)
            {
                ServiceTypeName = InterfaceTypeName;
            }
            else
            {
                // todo: Multiple service interfaces?
                //Type = GetServiceInterface(Type);
                //RemoteInterfaceTypeName = "I" + TypeUtils.GetParameterizedTemplateName(Type);
                //ReferenceClassBaseName = typeNameBase + "Reference";
                //ReferenceClassName = TypeUtils.GetParameterizedTemplateName(ReferenceClassBaseName, Type);

                ServiceTypeName = TypeUtils.GetParameterizedTemplateName(Type);
            }

            AbstractImplementationClassBaseName = typeNameBase + "Base";
            ActivationClassBaseName = typeNameBase + ActivationClassNameSuffix;
            ExtensionMethodsClassBaseName = typeNameBase + "ExtensionMethods";
            FactoryClassBaseName = GetFactoryNameBase(typeNameBase);
            GrainFactoryClassBaseName = typeNameBase + "GrainFactory";
            InvokerClassBaseName = typeNameBase + "MethodInvoker";
            StateClassBaseName = typeNameBase + "State";
            PropertiesClassBaseName = typeNameBase + "Properties";
            ReferenceClassBaseName = typeNameBase + "Reference";

            TaskReferenceClassBaseName = IsTaskGrain ? ReferenceClassBaseName : ReferenceClassBaseName + "Async";
            TaskRemoteInterfaceTypeBaseName = IsTaskGrain
                                                  ? TypeUtils.GetSimpleTypeName(Type, t => false)
                                                  : TypeUtils.GetSimpleTypeName(Type, t => false) + "Async";
        }

        private static bool ValidateInterfaceRules(Type type, out List<string> violations)
        {
            bool success = true;
            violations = new List<string>();

            success = success && ValidateInterfaceMethods(type, violations);
            success = success && ValidateInterfaceProperties(type, violations);

            return success;
        }

        private static bool ValidateInterfaceMethods(Type type, List<string> violations)
        {
            bool success = true;

            MethodInfo[] methods = type.GetMethods();
            foreach (MethodInfo method in methods)
            {
                if (method.IsSpecialName)
                    continue;

                if (IsPureObserverInterface(method.DeclaringType))
                {
                    if (method.ReturnType != typeof (void))
                    {
                        success = false;
                        violations.Add(
                            String.Format(
                                "Method {0}.{1} must return void because it is defined within the observer interface.",
                                type.FullName, method.Name));
                    }
                }
                else if (!IsTaskType(method.ReturnType))
                {
                    success = false;
                    violations.Add(
                        String.Format(
                            "Method {0}.{1} must return Task or Task<T> because it is defined within the grain interface.",
                            type.FullName, method.Name));
                }

                ParameterInfo[] parameters = method.GetParameters();
                foreach (ParameterInfo parameter in parameters)
                {
                    if (parameter.IsOut)
                    {
                        success = false;
                        violations.Add(
                            String.Format(
                                "Argument {0} of method {1}.{2} is an output parameter. Output parameters are not allowed.",
                                GetParameterName(parameter), type.FullName, method.Name));
                    }

                    if (parameter.ParameterType.IsByRef)
                    {
                        success = false;
                        violations.Add(
                            String.Format(
                                "Argument {0} of method {1}.{2} is an a reference parameter. Reference parameters are not allowed.",
                                GetParameterName(parameter), type.FullName, method.Name));
                    }
                }
            }

            return success;
        }

        private static bool ValidateInterfaceProperties(Type type, List<string> violations)
        {
            bool success = true;

            PropertyInfo[] properties = type.GetProperties();
            foreach (PropertyInfo property in properties)
            {

                //if (typeof(IGrainObserver).IsAssignableFrom(property.DeclaringType)) // if the method is declared by a interface that is an observer interface or inherits from one
                if (IsPureObserverInterface(property.DeclaringType))
                {
                    success = false;
                    violations.Add(String.Format("Properties are not allowed on observer interfaces:  {0}.{1}.",
                                                 type.FullName, property.Name));
                }
                else // inferface that inherits from IAddressable but not from IGrainObserver
                {
                    if (!IsTaskType(property.PropertyType))
                    {
                        success = false;
                        violations.Add(
                            String.Format(
                                "Property {0}.{1} must be of type Task or Task<T> because it is defined within the grain interface.",
                                type.FullName, property.Name));
                    }
                   
                    if (property.CanWrite)
                    {
                        success = false;
                        violations.Add(
                            String.Format(
                                "Property {0}.{1} has a setter. Property setters are not allowed in grain interfaces.",
                                type.FullName, property.Name));
                    }
                }
            }

            return success;
        }

        internal static bool IsGrainType(Type grainType)
        {
            return typeof (IGrain).IsAssignableFrom(grainType);
        }

        private static bool IsPureObserverInterface(Type t)
        {
            if (!typeof (IGrainObserver).IsAssignableFrom(t))
                return false;

            if (t == typeof (IGrainObserver))
                return true;

            if (t == typeof (IAddressable))
                return false;

            bool pure = false;
            foreach (Type iface in t.GetInterfaces())
            {
                if (iface == typeof (IAddressable)) // skip IAddressable that will be in the list regardless
                    continue;

                if (iface == typeof (IGrainExtension))
                    // Skip IGrainExtension, it's just a marker that can go on observer or grain interfaces
                    continue;

                pure = IsPureObserverInterface(iface);
                if (!pure)
                    return false;
            }

            return pure;
        }

        public static bool IsGrainInterface(Type t)
        {
            if (t.IsClass)
                return false;

            if (t.Equals(typeof (IGrainObserver)) || t.Equals(typeof (IAddressable)))
                return false;

            if (t.Equals(typeof (IGrain)))
                return false;

            if (t.Equals(typeof (ISystemTarget)))
                return false;

            return typeof (IAddressable).IsAssignableFrom(t);
        }

        public static bool IsGrainReference(Type t)
        {
            return typeof (IAddressable).IsAssignableFrom(t);
        }

        public static IEnumerable<Type> GetAllServiceInterfaces(Type type)
        {
            if (type.IsInterface)
                return new[] {type};
            var interfaces = type.GetInterfaces()
                                 .Where(
                                     t => typeof (IAddressable).IsAssignableFrom(t) && !typeof (IAddressable).Equals(t))
                                 .ToList();
            return interfaces.Count == 1 ? interfaces : interfaces.LeafTypes();
            // need to filter out superclass chains
        }

        public static MethodInfo[] GetMethods(Type grainType, bool bAllMethods = false)
        {
            List<MethodInfo> methodInfos = new List<MethodInfo>();
            GetServiceIntefaceMethodsImpl(grainType, grainType, methodInfos);
            BindingFlags flags = BindingFlags.Public | BindingFlags.InvokeMethod | BindingFlags.Instance;
            if (!bAllMethods)
            {
                flags |= BindingFlags.DeclaredOnly;
            }
            MethodInfo[] infos = grainType.GetMethods(flags);
            IEqualityComparer<MethodInfo> methodComparer = new MethodInfoComparer();
            foreach (MethodInfo methodInfo in infos)
            {
                if (!methodInfos.Contains(methodInfo, methodComparer))
                {
                    methodInfos.Add(methodInfo);
                }
            }

            return methodInfos.ToArray();
        }

        public static string GetFactoryClassForInterface(Type referenceInterface)
        {
            // remove "Reference" from the end of the type name
            string name = referenceInterface.Name;
            if (name.EndsWith("Reference", StringComparison.Ordinal)) 
                name = name.Substring(0, name.Length - 9);
            return TypeUtils.GetParameterizedTemplateName(GetFactoryNameBase(name), referenceInterface);
        }

        public static string GetFactoryNameBase(string typeName)
        {
            if (typeName.Length > 1 && typeName[0] == 'I' && Char.IsUpper(typeName[1]))
            {
                typeName = typeName.Substring(1);
            }

            // TODO: need to turn this from the naming convention to actual type lookup
            return TypeUtils.GetSimpleTypeName(typeName) + "Factory";
        }

        public static string GetParameterName(ParameterInfo info)
        {
            var n = info.Name;
            return string.IsNullOrEmpty(n) ? "arg" + info.Position : n;
        }

        public static PropertyInfo[] GetPersistentProperties(Type persistenceInterface)
        {
            // those flags only apply to class members, they do not apply to inherited interfaces (so BindingFlags.DeclaredOnly is meaningless here)
            // need to explicitely take all properties from all sub interfaces.
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            if ((null != persistenceInterface) && (typeof (IGrainState).IsAssignableFrom(persistenceInterface)))
            {
                // take all inherited intefaces that are subtypes of IGrainState except for IGrainState itself (it has internal properties which we don't want to expose here)
                // plus add the persistenceInterface itself
                IEnumerable<Type> allInterfaces = persistenceInterface.GetInterfaces().
                            Where(t => !t.Equals(typeof(IGrainState))).
                            Union( new[] { persistenceInterface });
                return allInterfaces
                    .SelectMany(i => i.GetProperties(flags))
                    .GroupBy(p => p.Name.Substring(p.Name.LastIndexOf('.') + 1))
                    .Select(g => g.OrderBy(p => p.Name.LastIndexOf('.')).First())
                    .ToArray();
            }
            return new PropertyInfo[] {};
        }

        public static PropertyInfo[] GetProperties(Type grainType)
        {
            var flags = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            return new[] {grainType}
                .Concat(grainType.GetInterfaces().Where(i => typeof (IAddressable).IsAssignableFrom(i)))
                .SelectMany(i => i.GetProperties(flags))
                .GroupBy(p => p.Name.Substring(p.Name.LastIndexOf('.') + 1))
                .Select(g => g.OrderBy(p => p.Name.LastIndexOf('.')).First())
                .ToArray();
        }


        private class MethodInfoComparer : IEqualityComparer<MethodInfo>
        {

            #region IEqualityComparer<InterfaceInfo> Members

            public bool Equals(MethodInfo x, MethodInfo y)
            {
                StringBuilder xString = new StringBuilder(x.Name);
                StringBuilder yString = new StringBuilder(y.Name);

                ParameterInfo[] parms = x.GetParameters();
                foreach (ParameterInfo info in parms)
                {
                    xString.Append(info.ParameterType.Name);
                    if (info.ParameterType.IsGenericType)
                    {
                        Type[] args = info.ParameterType.GetGenericArguments();
                        foreach (Type arg in args)
                        {
                            xString.Append(arg.Name);
                        }
                    }
                }

                parms = y.GetParameters();
                foreach (ParameterInfo info in parms)
                {
                    yString.Append(info.ParameterType.Name);
                    if (info.ParameterType.IsGenericType)
                    {
                        Type[] args = info.ParameterType.GetGenericArguments();
                        foreach (Type arg in args)
                        {
                            yString.Append(arg.Name);
                        }
                    }
                }
                return String.CompareOrdinal(xString.ToString(), yString.ToString()) == 0;
            }

            public int GetHashCode(MethodInfo obj)
            {
                throw new NotImplementedException();
            }

            #endregion
        }

        /// <summary>
        /// Recurses through interface graph accumulating methods
        /// </summary>
        /// <param name="grainType">Grain type</param>
        /// <param name="serviceType">Service interface type</param>
        /// <param name="methodInfos">Accumulated </param>
        private static void GetServiceIntefaceMethodsImpl(Type grainType, Type serviceType, List<MethodInfo> methodInfos)
        {
            Type[] iTypes = GetServiceInterfaces(serviceType).Values.ToArray();
            IEqualityComparer<MethodInfo> methodComparer = new MethodInfoComparer();
            foreach (Type iType in iTypes)
            {
                InterfaceMapping mapping = new InterfaceMapping();
                if (grainType.IsClass)
                {
                    mapping = grainType.GetInterfaceMap(iType);
                }
                if (grainType.IsInterface || mapping.TargetType == grainType)
                {
                    foreach (MethodInfo methodInfo in iType.GetMethods())
                    {
                        if (grainType.IsClass)
                        {
                            MethodInfo mi = methodInfo;
                            var match = mapping.TargetMethods.Any(
                                info => methodComparer.Equals(mi, info) &&
                                        info.DeclaringType == grainType);
                            if (match)
                            {
                                if (!methodInfos.Contains(mi, methodComparer))
                                {
                                    methodInfos.Add(mi);
                                }
                            }
                        }
                        else if (!methodInfos.Contains(methodInfo, methodComparer))
                        {
                            methodInfos.Add(methodInfo);
                        }
                    }
                }
            }
        }

        public static Dictionary<int, Type> GetServiceInterfaces(Type type)
        {
            Dictionary<int, Type> list = new Dictionary<int, Type>();

            if (IsGrainInterface(type))
            {
                list.Add(ComputeInterfaceId(type), type);
            }

            Type[] interfaces = type.GetInterfaces();
            foreach (Type interfaceType in interfaces.Where(IsGrainInterface))
            {
                list.Add(ComputeInterfaceId(interfaceType), interfaceType);
            }

            return list;
        }

        internal static Type GetTaskGrainAcInterface(Type taskInterfacefaceType)
        {
            string ifaceTypeName = TypeUtils.GetFullName(taskInterfacefaceType);
#if DEBUG
            Debug.Assert(IsTaskBasedInterface(taskInterfacefaceType),
                         "Should not be calling GetTaskGrainAcInterface for non Task-based interface type " +
                         ifaceTypeName);
#endif
            if (!ifaceTypeName.EndsWith("Async", StringComparison.Ordinal))
                return null;

            ifaceTypeName = ifaceTypeName.Substring(0, ifaceTypeName.Length - 5); // Remove training "Async" suffix

            Console.WriteLine(
                "Task-based grain interface {0} - searching for AsyncCompletion interface name = {1}",
                taskInterfacefaceType.FullName, ifaceTypeName);

            Type acInterfaceType = taskInterfacefaceType.Assembly.GetType(ifaceTypeName);
            if (acInterfaceType == null)
            {
                Console.WriteLine("Could not find AsyncCompletion interface type " + ifaceTypeName);
            }
            else if (IsTaskType(acInterfaceType))
            {
                acInterfaceType = null;
            }

            return acInterfaceType;
        }

        internal static string GetTaskInterfaceName(Type grainInterfacefaceType)
        {
            string ifaceTypeName = grainInterfacefaceType.Namespace + "." + grainInterfacefaceType.Name + "Async";
            return ifaceTypeName;
        }

        public static int ComputeMethodId(MethodInfo methodInfo)
        {
            StringBuilder strMethodId = new StringBuilder(methodInfo.Name + "(");
            ParameterInfo[] parameters = methodInfo.GetParameters();
            bool bFirstTime = true;
            foreach (ParameterInfo info in parameters)
            {
                if (!bFirstTime)
                {
                    strMethodId.Append(",");
                }
                strMethodId.Append(info.ParameterType.Name);
                if (info.ParameterType.IsGenericType)
                {
                    Type[] args = info.ParameterType.GetGenericArguments();
                    foreach (Type arg in args)
                    {
                        strMethodId.Append(arg.Name);
                    }
                }
                bFirstTime = false;
            }
            strMethodId.Append(")");
            return Utils.CalculateIdHash(strMethodId.ToString());
        }

        internal static int ComputeInterfaceId(Type interfaceType)
        {
            var ifaceName = TypeUtils.GetFullName(interfaceType);
            var ifaceId = Utils.CalculateIdHash(ifaceName);
            return ifaceId;
        }

        public static bool IsSystemTargetType(Type interfaceType)
        {
            return typeof (ISystemTarget).IsAssignableFrom(interfaceType);
        }

        public static bool IsTaskType(Type t)
        {
            return t == typeof (Task)
                   || (t.IsGenericType && t.GetGenericTypeDefinition().FullName == "System.Threading.Tasks.Task`1");
        }

        public static Type GetPromptType(Type type)
        {
            if (typeof (Task).IsAssignableFrom(type))
            {
                if (typeof (Task<>).IsAssignableFrom(type.GetGenericTypeDefinition()))
                {
                    return type.GetGenericArguments()[0];
                }
            }
            return type;
        }

        /// <summary>
        /// Whether method is read-only, i.e. does not modify grain state.
        /// Either a property getter, or a method marked with [ReadOnly].
        /// </summary>
        /// <param name="info"></param>
        /// <returns></returns>
        public static bool IsReadOnly(MethodInfo info)
        {
            return info.GetCustomAttributes(typeof (ReadOnlyAttribute), true).Length > 0 ||
                   (info.IsSpecialName && info.Name.StartsWith("get_", StringComparison.Ordinal));
        }

        public static bool IsAlwaysInterleave(MethodInfo methodInfo)
        {
            return methodInfo.GetCustomAttributes(typeof (AlwaysInterleaveAttribute), true).Length > 0;
        }

        public static bool IsUnordered(MethodInfo methodInfo)
        {
            return methodInfo.DeclaringType.GetCustomAttributes(typeof (UnorderedAttribute), true).Length > 0 ||
                   (methodInfo.DeclaringType.GetInterfaces().Any(i =>
                                                                 i.GetCustomAttributes(typeof (UnorderedAttribute), true)
                                                                  .Length > 0 &&
                                                                 methodInfo.DeclaringType.GetInterfaceMap(i)
                                                                           .TargetMethods.Contains(methodInfo))) ||
                   IsStatelessWorker(methodInfo);
        }

        public static bool IsStatelessWorker(Type grainType)
        {
            return grainType.GetCustomAttributes(typeof (StatelessWorkerAttribute), true).Length > 0 ||
                   grainType.GetInterfaces()
                            .Any(i => i.GetCustomAttributes(typeof (StatelessWorkerAttribute), true).Length > 0);
        }

        public static bool IsStatelessWorker(MethodInfo methodInfo)
        {
            return methodInfo.DeclaringType.GetCustomAttributes(typeof (StatelessWorkerAttribute), true).Length > 0 ||
                   (methodInfo.DeclaringType.GetInterfaces().Any(i =>
                                                                 i.GetCustomAttributes(
                                                                     typeof (StatelessWorkerAttribute), true).Length > 0 &&
                                                                 methodInfo.DeclaringType.GetInterfaceMap(i)
                                                                           .TargetMethods.Contains(methodInfo)));
        }

        public static TimeSpan CacheableDuration(MethodInfo methodInfo)
        {
            var a = methodInfo.GetCustomAttributes(typeof (CacheableAttribute), true);
            if (a.Length == 0)
            {
                a = methodInfo.DeclaringType.GetCustomAttributes(typeof (CacheableAttribute), true);
            }
            if (a.Length == 0 && methodInfo.Name.StartsWith("get_", StringComparison.Ordinal))
            {
                var propName = methodInfo.Name.Substring(4); // skip get_
                foreach (var prop in methodInfo.DeclaringType.GetProperties())
                {
                    if (prop.Name == propName)
                    {
                        a = prop.GetCustomAttributes(typeof (CacheableAttribute), true);
                        break;
                    }
                }
            }
            return a.Length > 0 ? ((CacheableAttribute) a[0]).DurationAsTimeSpan() : TimeSpan.Zero;
        }

        private static int CountAttributes<T>(Type grainIfaceType, bool inherit)
        {
            return grainIfaceType.GetCustomAttributes(typeof (T), inherit).Length;
        }

        private static bool HasAttribute<T>(Type grainIfaceType, bool inherit)
        {
            switch (CountAttributes<T>(grainIfaceType, inherit))
            {
                case 0:
                    return false;
                case 1:
                    return true;
                default:
                    throw new InvalidOperationException(
                        string.Format(
                            "More than one {0} cannot be specified for grain interface {1}",
                            typeof (T).Name,
                            grainIfaceType.Name));
            }
        }

        public static bool UsesPrimaryKeyExtension(Type grainIfaceType)
        {
            return HasAttribute<ExtendedPrimaryKeyAttribute>(grainIfaceType, inherit: false);
        }

        public bool IsSystemTarget
        {
            get { return IsSystemTargetType(Type); }
        }

        private static bool
            GetPlacementStrategy<T>(out PlacementStrategy placement, Type grainInterface, Func<T, PlacementStrategy> extract) 
                where T : class
        {
            var attribs = 
                grainInterface.GetCustomAttributes(typeof(T), inherit: true);
            switch (attribs.Length)
            {
                case 0:
                    placement = null;
                    return false;
                case 1:
                    placement = extract((T)attribs[0]);
                    return placement != null;
                default:
                    throw new InvalidOperationException(
                        string.Format(
                            "More than one {0} cannot be specified for grain interface {1}",
                            typeof(T).Name,
                            grainInterface.Name));
            }
        }

#pragma warning disable 612,618
        public static PlacementStrategy GetPlacementStrategy(Type grainInterface)
        {
            PlacementStrategy placement;

            if (GetPlacementStrategy<StatelessWorkerAttribute>(
                out placement,
                grainInterface,
                _ => 
                    GrainStrategy.LocalPlacementAvailable))
            {
                return placement;
            }

            if (GetPlacementStrategy<PlacementAttribute>(
                out placement,
                grainInterface,
                a =>
                    a.PlacementStrategy))
            {
                return placement;
            }

            return PlacementStrategy.GetDefault();
        }
#pragma warning disable 612,618

        private static int GetTypeCode(Type grainInterfaceOrClass)
        {
            var attrs = grainInterfaceOrClass.GetCustomAttributes(typeof(TypeCodeOverrideAttribute), false);
            var attr = attrs.Length > 0 ? attrs[0] as TypeCodeOverrideAttribute : null;
            string fullName = TypeUtils.GetTemplatedName(TypeUtils.GetFullName(grainInterfaceOrClass), grainInterfaceOrClass, t=>false);
            var typeCode = attr != null && attr.TypeCode > 0 ? attr.TypeCode : Utils.CalculateIdHash(fullName);
            return typeCode;
        }

        public static int GetGrainInterfaceId(Type grainInterface)
        {
            return GetTypeCode(grainInterface);
        }

        internal static int GetGrainClassTypeCode(Type grainClass)
        {
            return GetTypeCode(grainClass);
        }

        /// <summary>
        /// Evaluate a static property/method invocation
        /// </summary>
        /// <typeparam name="T">Type which has static factory properties/methods returning type T</typeparam>
        /// <param name="param">"property name" for static field/property, or new object[] {"method name", ..params..} for static method</param>
        /// <returns></returns>
        private static PlacementStrategy GetStaticPlacementPropertyByName<T>(object param)
        {
            var propertyName = param as string;
            if (propertyName != null)
            {
                var field = typeof(T).GetField(propertyName);
                if (field != null)
                    return (PlacementStrategy)field.GetValue(null);
                var property = typeof(T).GetProperty(propertyName);
                if (property == null)
                    throw new ArgumentException(String.Format("Type {0} does not have field/property {1}", typeof(T).FullName, propertyName));
                return (PlacementStrategy)property.GetValue(null, null);
            }
            while (true) // single-iteration loop for simple error handling - break will throw
            {
                var array = param as object[];
                if (array == null) break;
                var methodName = array[0] as string;
                if (methodName == null) break;
                array = array.Skip(1).ToArray();
                var method = typeof(T).GetMethod(methodName);
                if (method == null) break;
                return (PlacementStrategy)method.Invoke(null, array);
            }
            throw new ArgumentException("Invalid static init params for type " + typeof(T).FullName);
        }
    }
}

