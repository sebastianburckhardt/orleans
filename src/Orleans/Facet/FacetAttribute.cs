
using System;

namespace Orleans.Facet
{
    /// <summary>
    /// Base class for any attribution of grain facets
    /// </summary>
    [AttributeUsage(AttributeTargets.Parameter)]
    public abstract class FacetAttribute : Attribute
    {
        /// <summary>
        /// Aquires factory deligate for the type of facet being created.
        /// </summary>
        public abstract Factory<object> GetFactory(IServiceProvider serviceProvider, Type propertyType, string propertyName);
    }
}
