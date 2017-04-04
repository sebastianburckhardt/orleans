
namespace Orleans.Facet
{
    /// <summary>
    /// Configuration object with name of parameter facet is associated with.
    /// </summary>
    public class FacetConfiguration
    {
        public string ParameterName { get; }

        public FacetConfiguration(string parameterName)
        {
            ParameterName = parameterName;
        }
    }

    /// <summary>
    /// Marker interface for factets than need be configured
    /// </summary>
    public interface IConfigurableFacet
    {
        void Configure(FacetConfiguration configuration);
    }
}
