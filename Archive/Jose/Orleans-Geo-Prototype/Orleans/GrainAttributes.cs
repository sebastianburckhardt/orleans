using System;


using System.Linq;

namespace Orleans
{
    /// <summary>
    /// The ReadOnly attribute is used to mark methods that do not modify the state of a grain.
    /// <para>
    /// Marking methods as ReadOnly allows the run-time system to perform a number of optimizations
    /// that may significantly improve the performance of your application.
    /// </para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property)]
    internal sealed class ReadOnlyAttribute : Attribute
    {
    }

    /// <summary>
    /// The Reentrant attribute is used to mark grain implementation classes that allow request interleaving within a task.
    /// <para>
    /// This is an advanced feature and should not be used unless the implications are fully understood.
    /// That said, allowing request interleaving allows the run-time system to perform a number of optimizations
    /// that may significantly improve the performance of your application. 
    /// </para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class ReentrantAttribute : Attribute
    {
    }

    /// <summary>
    /// The Unordered attribute is used to mark grain interface in which the delivery order of
    /// messages is not significant.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class UnorderedAttribute : Attribute
    {
    }

    /// <summary>
    /// The StatelessWorker attribute is used to mark grain interface in which there is no expectation
    /// of preservation of grain state between requests. 
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class StatelessWorkerAttribute : Attribute
    {
    }

    /// <summary>
    /// The AlwaysInterleaveAttribute attribute is used to mark methods that can interleave with any other method type, including write (non ReadOnly) requests.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method | AttributeTargets.Property)]
    public sealed class AlwaysInterleaveAttribute : Attribute
    {
    }

    /// <summary>
    /// The TypeCodeOverrideAttribute attribute allows to specify the grain interface ID or the grain class type code
    /// to override the default ones to avoid hash collisions
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface | AttributeTargets.Class)]
    public sealed class TypeCodeOverrideAttribute : Attribute
    {
        /// <summary>
        /// Use a specific grain interface ID or grain class type code (e.g. to avoid hash collisions)
        /// </summary>
        public int TypeCode { get; private set; }

        public TypeCodeOverrideAttribute(int typeCode)
        {
            TypeCode = typeCode;
        }
    }

    /// <summary>
    /// Base for all placement policy marker attributes.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public abstract class PlacementAttribute : Attribute
    {
        internal PlacementStrategy PlacementStrategy { get; private set; }

        internal PlacementAttribute(PlacementStrategy placement)
        {
            PlacementStrategy = placement ?? PlacementStrategy.GetDefault();
        }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>RandomPlacement</c> policy.
    /// </summary>
    /// <remarks>
    /// This is the default placement policy, so this attribute does not need to be used for normal grains.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class RandomPlacementAttribute : PlacementAttribute
    {
        public RandomPlacementAttribute() :
            base(RandomPlacement.Singleton)
        { }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>PreferLocalPlacement</c> policy.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class PreferLocalPlacementAttribute : PlacementAttribute
    {
        public PreferLocalPlacementAttribute() :
            base(PreferLocalPlacement.Singleton)
        { }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>GraphPartitionPlacement</c> policy.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    internal sealed class GraphPartitionPlacementAttribute : PlacementAttribute
    {
        public GraphPartitionPlacementAttribute() :
            base(GraphPartitionPlacement.Singleton)
        { }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>LoadAwarePlacement</c> policy.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class LoadAwarePlacementAttribute : PlacementAttribute
    {
        public LoadAwarePlacementAttribute() :
            base(LoadAwarePlacement.Singleton)
        { }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>LocalPlacement</c> policy.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class LocalPlacementAttribute : PlacementAttribute
    {
        public LocalPlacementAttribute(int minActivations = -1, int maxActivations = -1) :
            base(new LocalPlacement(minActivations, maxActivations))
        { }
    }

    /// <summary>
    /// Marks a grain interface as using the <c>ExplicitPlacement</c> policy.
    /// </summary>
    [AttributeUsage(AttributeTargets.Interface)]
    internal sealed class ExplicitPlacementAttribute : PlacementAttribute
    {
        public ExplicitPlacementAttribute() :
            base(ExplicitPlacement.Tbd)
        { }
    }

    /// <summary>
    /// The Cacheable attribute is used to mark properties whose values may be cached by clients for a maximum of the specified amount of time.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property)]
    public sealed class CacheableAttribute : Attribute
    {
        private TimeSpan _cacheDuration;

        public string Duration
        {
            get { return _cacheDuration.ToString(); }
            set { _cacheDuration = TimeSpan.Parse(value); }
        }

        public TimeSpan DurationAsTimeSpan()
        {
            return _cacheDuration;
        }
    }


    /// <summary>
    /// The Immutable attribute indicates that instances of the marked class or struct are never modified
    /// after they are created.
    /// </summary>
    /// <remarks>
    /// Note that this implies that sub-objects are also not modified after the instance is created.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Struct | AttributeTargets.Class)]
    public sealed class ImmutableAttribute : Attribute
    {
    }

    /// <summary>
    /// The ActivationIncluded attribute is used to mark assemblies generated by the ClientGenerator tool.
    /// It is for internal use only.
    /// </summary>
    [AttributeUsage(AttributeTargets.Assembly, AllowMultiple = false)]
    public sealed class ActivationIncludedAttribute : Attribute
    {
    }

    /// <summary>
    /// Used to mark a method as providing a copier function for that type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class CopierMethodAttribute : Attribute
    {
    }

    /// <summary>
    /// Used to mark a method as providinga serializer function for that type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class SerializerMethodAttribute : Attribute
    {
    }

    /// <summary>
    /// Used to mark a method as providing a deserializer function for that type.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method)]
    public sealed class DeserializerMethodAttribute : Attribute
    {
    }

    /// <summary>
    /// Used to make a class for auto-registration as a serialization helper.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class RegisterSerializerAttribute : Attribute
    {
    }

    /// <summary>
    /// The [StorageProvider] attribute is used to define which storage provider to use for persistence of grain state.
    /// <para>
    /// Specifying [StorageProvider] property is recommended for all grains which extend GrainBase&lt;T&gt;.
    /// If no [StorageProvider] attribute is  specified, then a "Default" strorage provider will be used.
    /// If a suitable storage provider cannot be located for this grain, then the grain will fail to load into the Silo.
    /// </para>
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public sealed class StorageProviderAttribute : Attribute
    {
        public StorageProviderAttribute()
        {
            ProviderName = Constants.DEFAULT_STORAGE_PROVIDER_NAME;
        }
        /// <summary>
        /// The name of the storage provider to ne used for persisting state for this grain.
        /// </summary>
        public string ProviderName { get; set; }
    }

    /// <summary>
    /// Used to make a grain interface as using extended keys.
    /// </summary>
    /// <remarks>
    /// If a grain interface uses extended keys, then an additional set of grain reference 
    /// factory methods will be generated which accept both primary and extended key parts.
    /// </remarks>
    [AttributeUsage(AttributeTargets.Interface)]
    public sealed class ExtendedPrimaryKeyAttribute : Attribute
    {}

    [AttributeUsage(AttributeTargets.Interface)]
    internal sealed class FactoryAttribute : Attribute
    {
        public enum FactoryTypes
        {
            Grain,
            ClientObject,
            Both
        };

        private FactoryTypes factoryType = FactoryTypes.Grain;

        public FactoryAttribute(FactoryTypes factoryType)
        {
            this.factoryType = factoryType;
        }

        internal static FactoryTypes CollectFactoryTypesSpecified(Type type)
        {
            var attribs = type.GetCustomAttributes(typeof(FactoryAttribute), inherit: true);

            // [mlr] if no attributes are specified, we default to FactoryTypes.Grain.
            if (0 == attribs.Length)
                return FactoryTypes.Grain;
            
            // [mlr] otherwise, we'll consider all of them and aggregate the specifications
            // like flags.
            FactoryTypes? result = null;
            foreach (var i in attribs)
            {
                var a = (FactoryAttribute)i;
                if (result.HasValue)
                {
                    if (a.factoryType == FactoryTypes.Both)
                        result = a.factoryType;
                    else if (a.factoryType != result.Value)
                        result = FactoryTypes.Both;
                }
                else
                    result = a.factoryType;
            }

            // [mlr][todo] see exception description.
            if (result.Value == FactoryTypes.Both)
            {
                throw 
                    new NotSupportedException(
                        "Orleans doesn't currently support generating both a grain and a client object factory but we really want to!");
            }
            
            return result.Value;
        }

        public static FactoryTypes CollectFactoryTypesSpecified<T>()
        {
            return CollectFactoryTypesSpecified(typeof(T));
        }
    }
}
