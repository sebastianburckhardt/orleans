using System;
using System.Collections.Generic;
using System.Linq;



namespace Orleans.Runtime
{
    internal class GrainTypeManager : ISiloShutdownParticipant
    {
        private Dictionary<string, GrainTypeData> grainTypes;
        private readonly GrainAssemblyLoader loader;
        private readonly Logger logger;
        private readonly GrainInterfaceMap grainInterfaceMap;
        private readonly Dictionary<int, InvokerData> _invokers;

        public static GrainTypeManager Instance { get; private set; }
        
        public GrainTypeManager(bool localTestMode)
        {
            logger = Logger.GetLogger("GrainTypeManager");
            grainInterfaceMap = new GrainInterfaceMap(localTestMode);
            _invokers = new Dictionary<int, InvokerData>();
            lock (typeof (GrainTypeManager))
            {
                if (Instance != null)
                    throw new InvalidOperationException("An attempt to create a second insance of GrainTypeManager.");
                Instance = this;
            }
            loader = GrainAssemblyLoader.Instance;            
        }

        public void Start()
        {
            grainTypes = loader.LoadGrainAssemblies();
            InitializeInterfaceMap();
        }

        public Dictionary<string, string> GetGrainInterfaceToClassMap()
        {
            return grainInterfaceMap.GetPrimaryImplementations();
        }

        internal GrainTypeData this[string className]
        {
            get
            {
                string msg;

                lock (this)
                {
                    string grainType;

                    if (grainInterfaceMap.TryGetPrimaryImplementation(className, out grainType))
                        return grainTypes[grainType];
                    if (grainTypes.ContainsKey(className))
                        return grainTypes[className];

                    if (TypeUtils.IsGenericClass(className))
                    {
                        var templateName = TypeUtils.GetRawClassName(className);
                        if (grainInterfaceMap.TryGetPrimaryImplementation(templateName, out grainType))
                            templateName = grainType;
                        if (grainTypes.ContainsKey(templateName))
                        {
                            // Found the generic template class
                            try
                            {
                                // Instantiate the specific type from generic template
                                var genericGrainTypeData = (GenericGrainTypeData)grainTypes[templateName];
                                Type[] typeArgs = TypeUtils.GenericTypeArgs(className);
                                var concreteTypeData = genericGrainTypeData.MakeGenericType(typeArgs);

                                // Add to lookup tables for next time
                                string grainClassName = concreteTypeData.GrainClass;
                                grainTypes.Add(grainClassName, concreteTypeData);
                                AddToGrainInterfaceToClassMap(concreteTypeData.Type, concreteTypeData.ServiceInterfaceTypes);

                                return concreteTypeData;
                            }
                            catch (Exception ex)
                            {
                                msg = "Cannot instantiate generic class " + className;
                                logger.Error(ErrorCode.Runtime_Error_100092, msg, ex);
                                throw new KeyNotFoundException(msg, ex);
                            }
                        }
                    }
                }

                msg = "Cannot find GrainTypeData for class " + className;
                logger.Error(ErrorCode.Runtime_Error_100093, msg);
                throw new TypeLoadException(msg);
            }
        }

        internal void GetTypeInfo(int typeCode, out string grainClass, out PlacementStrategy placement, string genericArguments = null)
        {
            if (!grainInterfaceMap.TryGetTypeInfo(typeCode, out grainClass, out placement, genericArguments))
                throw new OrleansException(String.Format("Unexpected: Cannot find an implementation class for grain interface {0}", typeCode));
        }

        internal void GetTypeInfo(GrainId grainId, out string grainClass, out PlacementStrategy placement, string genericArguments = null)
        {
            GetTypeInfo(grainId.GetTypeCode(), out grainClass, out placement, genericArguments);
        }

        private void InitializeInterfaceMap()
        {
            foreach (GrainTypeData grainType in grainTypes.Values)
            {
                if (grainType.ServiceInterfaceTypes.Exists(t => GrainClientGenerator.GrainInterfaceData.IsGrainType(t)))
                {
                    var grainInterfaces = grainType.ServiceInterfaceTypes.Where(t => GrainClientGenerator.GrainInterfaceData.IsGrainType(t)).ToList();
                    foreach (var grainInterface in grainInterfaces)
                    {
                        var placement = GrainClientGenerator.GrainInterfaceData.GetPlacementStrategy(grainInterface);
                        AddToGrainInterfaceToClassMap(grainType.Type, grainType.ServiceInterfaceTypes, placement);
                    }
                }
                else
                {
                    AddToGrainInterfaceToClassMap(grainType.Type, grainType.ServiceInterfaceTypes);
                }
            }

            // todo: AsyncCompletion promise = grainOwner.RegisterRuntimeInstance(grainTypes, grainInterfaceToClassMap);
        }

        private void AddToGrainInterfaceToClassMap(Type grainClass, List<Type> grainInterfaces, PlacementStrategy placement = null)
        {
            string grainClassCompleteName = TypeUtils.GetFullName(grainClass);
            var grainClassTypeCode = GrainClientGenerator.GrainInterfaceData.GetGrainClassTypeCode(grainClass);

            foreach (Type iface in grainInterfaces)
            {
                string ifaceCompleteName = TypeUtils.GetFullName(iface);
                string ifaceName = TypeUtils.GetRawClassName(ifaceCompleteName);
                var isPrimaryImplementor = IsPrimaryImplementor(grainClass, iface);
                var ifaceId = GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(iface);
                grainInterfaceMap.AddEntry(ifaceId, iface, grainClassTypeCode, ifaceName, grainClassCompleteName, grainClass.Assembly.CodeBase,
                    isPrimaryImplementor, placement);
            }
        }

        private static bool IsPrimaryImplementor(Type grainClass, Type iface)
        {
            bool isPrimaryImplementor = (
                iface.Name.Substring(1) == grainClass.Name // TODO: a crude temporary way of selecting primary implementors of grain interfaces.
            ); 
            return isPrimaryImplementor;
        }

        public bool TryGetData(string name, out GrainTypeData result)
        {
            return grainTypes.TryGetValue(name, out result);
        }
        
        internal GrainInterfaceMap GetTypeCodeMap()
        {
            // TODO: should we clone it?
            return grainInterfaceMap;
        }

        internal void AddInvokerClass(int interfaceId, Type invoker)
        {
            lock (_invokers)
            {
                if (!_invokers.ContainsKey(interfaceId))
                    _invokers.Add(interfaceId, new InvokerData(invoker));
            }
        }

        internal IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null)
        {
            try
            {
                InvokerData invokerData;

                if (_invokers.TryGetValue(interfaceId, out invokerData))
                    return invokerData.GetInvoker(genericGrainType);
            }
            catch (Exception ex)
            {
                throw new OrleansException(String.Format("Error finding invoker for interface ID: {0} (0x{0, 8:X8}). {1}", interfaceId, ex), ex);
            }

            throw new OrleansException(String.Format("Cannot find an invoker for interface ID: {0} (0x{0, 8:X8}).",
                interfaceId));
        }

        #region Implementation of ISiloShutdownParticipant

        public void BeginShutdown(Action tryFinishShutdown)
        {
            tryFinishShutdown();
        }

        public bool CanFinishShutdown()
        {
            return true;
        }

        public void FinishShutdown()
        {
            // nothing
        }

        public SiloShutdownPhase Phase
        {
            get { return SiloShutdownPhase.Middle; }
        }

        #endregion

        private class InvokerData
        {
            private readonly Type _baseInvokerType;
            private IGrainMethodInvoker _invoker;
            private readonly Dictionary<string, IGrainMethodInvoker> _cachedGenericInvokers;

            public InvokerData(Type invokerType)
            {
                _baseInvokerType = invokerType;
                if(invokerType.IsGenericType)
                    _cachedGenericInvokers = new Dictionary<string, IGrainMethodInvoker>();
            }

            public IGrainMethodInvoker GetInvoker(string genericGrainType = null)
            {
                if (String.IsNullOrEmpty(genericGrainType))
                {
                    return _invoker ?? (_invoker = (IGrainMethodInvoker) Activator.CreateInstance(_baseInvokerType));
                }

                if (_cachedGenericInvokers.ContainsKey(genericGrainType))
                    return _cachedGenericInvokers[genericGrainType];

                Type[] typeArgs = TypeUtils.GenericTypeArgs(genericGrainType);
                Type concreteType = _baseInvokerType.MakeGenericType(typeArgs);
                var inv = (IGrainMethodInvoker) Activator.CreateInstance(concreteType);
                _cachedGenericInvokers[genericGrainType] = inv;
                return inv;
            }
        }

    }
}
