using System;

namespace Orleans.Providers
{
    [AttributeUsage(AttributeTargets.Class, AllowMultiple=false, Inherited=false)]
    public sealed class ProviderNameAttribute : Attribute
    {
        public string Name { get; private set; }

        public ProviderNameAttribute(string name)
        {
            Name = name;
        }
    }
}