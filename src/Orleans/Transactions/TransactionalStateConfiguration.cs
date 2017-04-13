
using Orleans.Facet;

namespace Orleans.Transactions
{
    public class TransactionalStateConfiguration
    {
        public TransactionalStateConfiguration(FacetConfiguration facetConfig)
        {
            this.StateName = facetConfig.ParameterName;
        }

        public TransactionalStateConfiguration(string storageProviderName, string stateName)
        {
            this.StorageProviderName = storageProviderName;
            this.StateName = stateName;
        }

        public string StorageProviderName { get; }
        public string StateName { get; }
    }

    public interface IConfigurableTransactionalState : IConfigurableFacet
    {
        void Configure(TransactionalStateConfiguration config);
    }
}
