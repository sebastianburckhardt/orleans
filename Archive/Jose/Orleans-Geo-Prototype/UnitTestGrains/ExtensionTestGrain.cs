using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime.Providers;
using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class ExtensionTestGrain : GrainBase, IExtensionTestGrain
    {
        public string ExtensionProperty { get; private set; }
        private TestExtension extender;

        public override Task ActivateAsync()
        {
            ExtensionProperty = "";
            extender = null;
            return base.ActivateAsync();
        }

        public Task InstallExtension(string name)
        {
            if (extender == null)
            {
                extender = new TestExtension(this);
                if (!SiloProviderRuntime.Instance.TryAddExtension(extender))
                {
                    throw new SystemException("Unable to add new extension");
                }
            }
            ExtensionProperty = name;
            return TaskDone.Done;
        }

        public Task RemoveExtension()
        {
            SiloProviderRuntime.Instance.RemoveExtension(extender);
            extender = null;
            return TaskDone.Done;
        }
    }

    public class GenericExtensionTestGrain<T> : GrainBase, IGenericExtensionTestGrain<T>
    {
        public T ExtensionProperty { get; private set; }
        private GenericTestExtension<T> extender;

        public override Task ActivateAsync()
        {
            ExtensionProperty = default(T);
            extender = null;
            return base.ActivateAsync();
        }

        public Task InstallExtension(T name)
        {
            if (extender == null)
            {
                extender = new GenericTestExtension<T>(this);
                if (!SiloProviderRuntime.Instance.TryAddExtension(extender))
                {
                    throw new SystemException("Unable to add new extension");
                }
            }
            ExtensionProperty = name;
            return TaskDone.Done;
        }

        public Task RemoveExtension()
        {
            SiloProviderRuntime.Instance.RemoveExtension(extender);
            extender = null;
            return TaskDone.Done;
        }
    }
}