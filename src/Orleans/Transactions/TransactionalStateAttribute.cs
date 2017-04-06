
using System;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Facet;

namespace Orleans.Transactions
{
    public class TransactionalStateAttribute : FacetAttribute
    {
        public string StorageProviderName { get; }
        public string StateName { get; }

        public TransactionalStateAttribute(string storageProviderName = null, string stateName = null)
        {
            this.StorageProviderName = storageProviderName;
            this.StateName = stateName;
        }

        public override Factory<object> GetFactory(IServiceProvider serviceProvider, Type parameterType, string parameterName)
        {
            var config = new TransactionalStateConfiguration(this.StorageProviderName, this.StateName ?? parameterName);
            return () =>
            {
                object facet = serviceProvider.GetRequiredService(parameterType);
                IConfigurableTransactionalState configurable = facet as IConfigurableTransactionalState;
                configurable?.Configure(config);
                return facet;
            };
        }
    }
}
