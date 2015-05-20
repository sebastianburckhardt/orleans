using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using GrainClientGenerator;
using Orleans.Runtime.Coordination;


namespace Orleans.Runtime
{
    // grain type meta data
    [Serializable]
    internal class GrainTypeData
    {
        internal Type Type { get; private set; }
        internal string GrainClass { get; private set; }
        internal Dictionary<int, string> ServiceInterfaces { get; private set; }
        internal List<Type> ServiceInterfaceTypes { get; private set; }
        internal HashSet<Tuple<int, int>> ReadOnlyMethods { get; private set; }
        internal Type StateObjectType { get; private set; }
        

        public GrainTypeData(Type type, Type stateObjectType)
        {
            Type = type;
            IsReentrant = Type.GetCustomAttributes(typeof (ReentrantAttribute), true).Length > 0;
            GrainClass = TypeUtils.GetFullName(type);
            List<Type> interfaceTypes;
            ServiceInterfaces = GetOrleansServiceInterfaces(type, out interfaceTypes);
            ServiceInterfaceTypes = interfaceTypes;
            ReadOnlyMethods = GetReadOnlyMethods(ServiceInterfaceTypes);
            StateObjectType = stateObjectType;
        }

        private static HashSet<Tuple<int, int>> GetReadOnlyMethods(List<Type> serviceInterfaces)
        {
            return serviceInterfaces.SelectMany(service =>
                service.GetMethods().Where(method => GrainClientGenerator.GrainInterfaceData.IsReadOnly(method))
                .Select(method => Tuple.Create(GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(service), GrainClientGenerator.GrainInterfaceData.ComputeMethodId(method))))
                .ToSet();
        }

        public bool IsReentrant { get; private set; }

        /// <summary>
        /// Get a list of implemented service interfaces (those derived from IAddressable)
        /// for the grain class and any super classes
        /// </summary>
        /// <param name="grainType">Grain service class</param>
        /// <returns>All service interfaces implemented by the grain class</returns>
        private static Dictionary<int, string> GetOrleansServiceInterfaces(Type grainType, out List<Type> interfaceTypes)
        {
            Dictionary<int, string> implementedInterfaces = new Dictionary<int, string>();
            interfaceTypes = new List<Type>();

            Type grainClass = grainType;

            // Add the list of implemented service interfaces (those derived from IAddressable)

            while (grainClass != typeof(GrainBase) && grainClass != typeof(Object))
            {
                foreach (Type t in grainClass.GetInterfaces())
                {
                    if (t == typeof(IAddressable))
                        continue;

                    if (GrainClientGenerator.GrainInterfaceData.IsGrainInterface(t))
                    {
                        int interfaceId = GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(t);
                        if (!implementedInterfaces.ContainsKey(interfaceId))
                        {
                            implementedInterfaces.Add(interfaceId, TypeUtils.GetFullName(t));
                            interfaceTypes.Add(t);
                        }

                        if (GrainClientGenerator.GrainInterfaceData.IsTaskBasedInterface(t))
                        {
                            // This grain implements a Task-based inferface, so add the corresponding AsyncCompletion version of the grain interface too
                            Type acInterfaceType = GrainClientGenerator.GrainInterfaceData.GetTaskGrainAcInterface(t);
                            if (acInterfaceType == null)
                                continue; // There is no AsyncCompletion version of this Task-based grain interface

                            int acInterfaceId = GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(acInterfaceType);
                            if (!implementedInterfaces.ContainsKey(acInterfaceId))
                            {
                                implementedInterfaces.Add(acInterfaceId, TypeUtils.GetFullName(acInterfaceType));
                                interfaceTypes.Add(acInterfaceType);
                            }
                        }
                    }
                }

                // ALWAYS add default implicit grain class interface
                implementedInterfaces.Add(GrainClientGenerator.GrainInterfaceData.GetGrainInterfaceId(grainClass), TypeUtils.GetFullName(grainClass));

                // Traverse the class hierarchy
                grainClass = grainClass.BaseType;
            }

            return implementedInterfaces;
        }

    }


    [Serializable]
    internal class GenericGrainTypeData : GrainTypeData
    {
        readonly Type activationType;
        readonly Type stateObjectType;

        public GenericGrainTypeData(Type activationType, Type stateObjectType) :
            base(activationType, stateObjectType)
        {
            if (!activationType.IsGenericTypeDefinition)
                throw new ArgumentException("Activation type is not generic: " + activationType.GetType());
            
            this.activationType = activationType;
            this.stateObjectType = stateObjectType;
        }

        public GrainTypeData MakeGenericType(Type[] typeArgs)
        {
            // Need to make a non-generic instance of the class to access the static data field. The field itself is independent of the instantiated type.
            var concreteActivationType = activationType.MakeGenericType(typeArgs);
            var concreteStateObjectType = stateObjectType.IsGenericType ? stateObjectType.MakeGenericType(typeArgs) : stateObjectType;
            
            return new GrainTypeData(concreteActivationType, concreteStateObjectType);
        }
    }
}
