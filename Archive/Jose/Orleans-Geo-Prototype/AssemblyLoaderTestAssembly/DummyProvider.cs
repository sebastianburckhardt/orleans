using System;
using System.Threading.Tasks;
using Orleans.Providers;

namespace AssemblyLoaderTestAssembly
{
    public class DummyProvider : IOrleansProvider
    {
        public string Name
        {
            get { return "DummyProvider"; }
        }

        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            throw new NotImplementedException();
        }
    }
}
