
using Orleans.Facet;

namespace Orleans.Transactions
{
    internal class TransactionalStateConfiguration
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

    internal interface IConfigurableTransactionalState : IConfigurableFacet
    {
        void Configure(TransactionalStateConfiguration config);
    }
}
