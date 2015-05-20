using System;
using System.Collections.Generic;
using System.Linq;



namespace Orleans
{
    internal interface IGrainTypeResolver
    {
        bool TryGetGrainTypeCode(int grainInterfaceId, out int grainTypeCode, string grainClassNamePrefix);
        bool TryGetGrainTypeCode(string grainImplementationClassName, out int grainTypeCode);
        string GetLoadedGrainAssemblies();
    }

    /// <summary>
    /// Internal data structure that holds a grain interfaces to grain classes map.
    /// </summary>
    [Serializable]
    internal class GrainInterfaceMap : IGrainTypeResolver
    {
        private readonly Dictionary<int, GrainInterfaceData> _table;
        [NonSerialized]
        private readonly Dictionary<int, GrainClassData> _implementationIndex;

        [NonSerialized] // Client shouldn't need this
        private readonly Dictionary<string, string> _primaryImplementations; // TODO: revisit/remove the notion of 'primary' after moving to grain class type codes

        private readonly bool _localTestMode;
        private HashSet<string> _loadedGrainAsemblies;

        public GrainInterfaceMap(bool localTestMode)
        {
            _table = new Dictionary<int, GrainInterfaceData>();
            _primaryImplementations = new Dictionary<string, string>();
            _implementationIndex = new Dictionary<int, GrainClassData>();
            _localTestMode = localTestMode;
            if(localTestMode) // if we are running in test mode, we'll build a list of loaded grain assemblies to help with troubleshooting deployment issue
                _loadedGrainAsemblies = new HashSet<string>();
        }

        internal void AddEntry(int interfaceId, Type iface, int grainTypeCode, string grainInterface, string grainClass, string assembly, 
                                bool primaryImplementation = false, PlacementStrategy placement = null)
        {
            lock (this)
            {
                GrainInterfaceData grainInterfaceData;

                if (_table.ContainsKey(interfaceId))
                    grainInterfaceData = _table[interfaceId];
                else
                {
                    grainInterfaceData = new GrainInterfaceData(interfaceId, iface, grainInterface, placement);
                    _table[interfaceId] = grainInterfaceData;
                }

                var implementation = new GrainClassData(grainTypeCode, grainClass, grainInterfaceData);
                if (!_implementationIndex.ContainsKey(grainTypeCode))
                    _implementationIndex.Add(grainTypeCode, implementation);

                grainInterfaceData.AddImplementation(implementation, primaryImplementation);
                if (primaryImplementation) // TODO: revisit/remove the notion of 'primary' after moving to grain class type codes
                {
                    _primaryImplementations[grainInterface] = grainClass;
                }
                else
                {
                    if (!_primaryImplementations.ContainsKey(grainInterface))
                        _primaryImplementations.Add(grainInterface, grainClass);
                }

                if (_localTestMode)
                {
                    if (!_loadedGrainAsemblies.Contains(assembly))
                        _loadedGrainAsemblies.Add(assembly);
                }
            }
        }

        internal Dictionary<string, string> GetPrimaryImplementations() // TODO: revisit/remove the notion of 'primary' after moving to grain class type codes
        {
            lock (this)
            {
                return new Dictionary<string, string>(_primaryImplementations);
            }
        }

        internal bool TryGetPrimaryImplementation(string grainInterface, out string grainClass) // TODO: revisit/remove the notion of 'primary' after moving to grain class type codes
        {
            lock (this)
            {
                return _primaryImplementations.TryGetValue(grainInterface, out grainClass);
            }
        }

        internal bool TryGetServiceInterface(int interfaceId, out Type iface)
        {
            lock (this)
            {
                iface = null;

                if (!_table.ContainsKey(interfaceId))
                    return false;

                var interfaceData = _table[interfaceId];
                iface = interfaceData.Interface;
                return true;
            }
        }

        internal bool ContainsGrainInterface(int interfaceId)
        {
            lock (this)
            {
                return _table.ContainsKey(interfaceId);
            }
        }

        internal bool TryGetTypeInfo(int typeCode, out string grainClass, out PlacementStrategy placement, string genericArguments = null)
        {
            lock (this)
            {
                grainClass = null;
                placement = null;

                if (!_implementationIndex.ContainsKey(typeCode))
                    return false;

                var implementation = _implementationIndex[typeCode];
                grainClass = genericArguments == null ? implementation.GrainClass : implementation.GetGenericClassName(genericArguments);
                placement = implementation.InterfaceData.PlacementStrategy;
                return true;

            }
        }

        internal void SetPlacementStrategy(int interfaceId, PlacementStrategy placement)
        {
            lock (this)
            {
                if (!_table.ContainsKey(interfaceId))
                    throw new OrleansException(String.Format("Cannot find grain interface ID {0}", interfaceId));

                var item = _table[interfaceId];
                item.SetPlacementStrategy(placement);
            }
        }

        internal bool TryGetGrainClass(int grainTypeCode, out string grainClass, string genericArguments)
        {
            grainClass = null;
            GrainClassData implementation;
            if (!_implementationIndex.TryGetValue(grainTypeCode, out implementation))
            {
                return false;
            }

            grainClass = genericArguments == null ? implementation.GrainClass : implementation.GetGenericClassName(genericArguments);
            return true;
        }

        public bool TryGetGrainTypeCode(int grainInterfaceId, out int grainTypeCode, string grainClassNamePrefix=null)
        {
            grainTypeCode = 0;
            GrainInterfaceData interfaceData;
            if (!_table.TryGetValue(grainInterfaceId, out interfaceData))
            {
                return false;
            }

            GrainClassData[] implementations = interfaceData.Implementations;

            if (implementations.Length == 0)
                    return false;

            if (String.IsNullOrEmpty(grainClassNamePrefix))
            {
                if (implementations.Length == 1)
                {
                    grainTypeCode = implementations[0].GrainTypeCode;
                    return true;
                }

                if (interfaceData.PrimaryImplementation != null)
                {
                    grainTypeCode = interfaceData.PrimaryImplementation.GrainTypeCode;
                    return true;
                }

                throw new OrleansException(String.Format("Cannot resolve grain interface ID={0} to a grain class because of multiple implementations of it: {1}",
                    grainInterfaceId, Utils.IEnumerableToString(implementations, d => d.GrainClass, ",", false)));
            }

            if (implementations.Length == 1)
            {
                if (implementations[0].GrainClass.StartsWith(grainClassNamePrefix, StringComparison.Ordinal))
                {
                    grainTypeCode = implementations[0].GrainTypeCode;
                    return true;
                }
                    
                return false;
            }

            var matches = implementations.Where(impl => impl.GrainClass.Equals(grainClassNamePrefix)).ToArray(); //exact match?
            if(matches.Length == 0)
                matches = implementations.Where(
                    impl => impl.GrainClass.StartsWith(grainClassNamePrefix, StringComparison.Ordinal)).ToArray(); //prefix matches

            if (matches.Length == 0)
                return false;

            if (matches.Length == 1)
            {
                grainTypeCode = matches[0].GrainTypeCode;
                return true;
            }

            throw new OrleansException(String.Format("Cannot resolve grain interface ID={0}, grainClassNamePrefix={1} to a grain class because of multiple implementations of it: {2}",
                grainInterfaceId, 
                grainClassNamePrefix,
                Utils.IEnumerableToString(matches, d => d.GrainClass, ",", false)));
        }

        public bool TryGetGrainTypeCode(string grainImplementationClassName, out int grainTypeCode)
        {
            grainTypeCode = 0;
            // have to iterate since _primaryImplementations is not serialized.
            foreach (GrainInterfaceData interfaceData in _table.Values)
            {
                foreach(var implClass in interfaceData.Implementations)
                    if (implClass.GrainClass.Equals(grainImplementationClassName))
                    {
                        grainTypeCode = implClass.GrainTypeCode;
                        return true;
                    }
            }
            return false;
        }


        public string GetLoadedGrainAssemblies()
        {
            if(_loadedGrainAsemblies != null)
                return _loadedGrainAsemblies.ToStrings();
            return String.Empty;
        }
    }

    /// <summary>
    /// Metadata for a grain interface
    /// </summary>
    [Serializable]
    internal class GrainInterfaceData
    {
        [NonSerialized]
        private readonly Type _iface;
        [NonSerialized]
        private PlacementStrategy _placementStrategy;
        private readonly HashSet<GrainClassData> _implementations;
        
        internal Type Interface { get { return _iface; } }
        internal int InterfaceId { get; private set; }
        internal string GrainInterface { get; private set; }
        internal PlacementStrategy PlacementStrategy { get { return _placementStrategy; } }
        internal GrainClassData[] Implementations { get { return _implementations.ToArray(); } }
        internal GrainClassData PrimaryImplementation { get; private set; }
    

        internal GrainInterfaceData(int interfaceId, Type iface, string grainInterface, PlacementStrategy placement = null)
        {
            InterfaceId = interfaceId;
            this._iface = iface;
            GrainInterface = grainInterface;
            _placementStrategy = placement ?? PlacementStrategy.GetDefault();
            _implementations = new HashSet<GrainClassData>();
        }

        internal void AddImplementation(GrainClassData implementation, bool primaryImplemenation = false)
        {
            lock (this)
            {
                if (!_implementations.Contains(implementation))
                    _implementations.Add(implementation);

                if (primaryImplemenation)
                    PrimaryImplementation = implementation;
            }
        }

        internal void SetPlacementStrategy(PlacementStrategy placement)
        {
            var actual = placement ?? PlacementStrategy.GetDefault();
            lock (this)
            {
                _placementStrategy = actual;
            }
        }

        public override string ToString()
        {
            return String.Format("{0}:{1}", GrainInterface, InterfaceId);
        }
    }


    /// <summary>
    /// Metadata for a grain class
    /// </summary>
    [Serializable]
    internal class GrainClassData
    {
        [NonSerialized]
        private readonly GrainInterfaceData _interfaceData;
        [NonSerialized]
        private readonly Dictionary<string, string> _genericClassNames;

        internal int GrainTypeCode { get; private set; }
        internal string GrainClass { get; private set; }
        internal GrainInterfaceData InterfaceData { get { return _interfaceData; } }

        internal GrainClassData(int grainTypeCode, string grainClass, GrainInterfaceData interfaceData)
        {
            GrainTypeCode = grainTypeCode;
            GrainClass = grainClass;
            _interfaceData = interfaceData;
            _genericClassNames = new Dictionary<string, string>(); // TODO: initialize only for generic classes
        }

        internal string GetGenericClassName(string typeArguments)
        {
            lock (this)
            {
                if (_genericClassNames.ContainsKey(typeArguments))
                    return _genericClassNames[typeArguments];

                var className = String.Format("{0}[{1}]", GrainClass, typeArguments);
                _genericClassNames.Add(typeArguments, className);
                return className;
            }
        }

        public override string ToString()
        {
            return String.Format("{0}:{1}", GrainClass, GrainTypeCode);
        }

        public override int GetHashCode()
        {
            return GrainTypeCode;
        }

        public override bool Equals(object obj)
        {
            if(!(obj is GrainClassData))
                return false;

            return GrainTypeCode == ((GrainClassData) obj).GrainTypeCode;
        }
    }
}
