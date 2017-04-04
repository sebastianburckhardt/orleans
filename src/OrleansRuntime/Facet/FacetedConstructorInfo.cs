
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Orleans.Facet
{
    /// <summary>
    /// Facet construction logic based on type
    /// </summary>
    internal class FacetedConstructorInfo
    {
        private static readonly List<Factory<IServiceProvider, object>> EmptyFactories = new List<Factory<IServiceProvider,object>>();
        private readonly List<Factory<IServiceProvider, object>> constructorParameterFactories;

        public Type[] FacetParameters { get; }

        public FacetedConstructorInfo(Type type)
        {
            ConstructorInfo constructor = FindConstructor(type);
            this.constructorParameterFactories = CreateConstructorParameterFactories(constructor);
            this.FacetParameters = GetConstructorFacetParameters(constructor);
        }

        public object[] CreateConstructorParameterFacets(IServiceProvider serviceProvider)
        {
            return this.constructorParameterFactories.Select(factory => factory(serviceProvider)).ToArray();
        }

        private static ConstructorInfo FindConstructor(Type type)
        {
            ConstructorInfo[] constructors = type.GetConstructors(BindingFlags.Public | BindingFlags.Instance);
            return constructors.FirstOrDefault();
        }

        private static Type[] GetConstructorFacetParameters(ConstructorInfo constructor)
        {
            return constructor == null
                ? Type.EmptyTypes
                : constructor
                    .GetParameters()
                    .Where(pi => typeof(IGrainFacet).IsAssignableFrom(pi.ParameterType))
                    .Select(pi => pi.ParameterType)
                    .ToArray();
        }


        private static List<Factory<IServiceProvider, object>> CreateConstructorParameterFactories(ConstructorInfo constructor)
        {
            return constructor == null
                ? EmptyFactories
                : constructor
                    .GetParameters()
                    .Where(pi => typeof(IGrainFacet).IsAssignableFrom(pi.ParameterType))
                    .Select(CreateConstructorParameterFactory)
                    .ToList();
        }

        private static Factory<IServiceProvider, object> CreateConstructorParameterFactory(ParameterInfo parameter)
        {
            FacetAttribute attribute = parameter.GetCustomAttribute<FacetAttribute>();
            if (attribute != null)
            {
                return sp => attribute.GetFactory(sp, parameter.ParameterType, parameter.Name)();
            }
            var config = new FacetConfiguration(parameter.Name);
            return sp =>
            {
                var facet = sp.GetService(parameter.ParameterType);
                var configurable = facet as IConfigurableFacet;
                configurable?.Configure(config);
                return facet;
            };
        }
    }
}
