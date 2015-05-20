using System;
using System.Reflection;

namespace Orleans.Providers
{
    internal class AssemblyLoaderReflectionCriterion : AssemblyLoaderCriterion
    {
        internal new delegate bool Predicate(Assembly input, out string[] complaint);

        internal static AssemblyLoaderReflectionCriterion NewAssemblyLoaderReflectionCriterion(Predicate predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return 
                new AssemblyLoaderReflectionCriterion(predicate);
        }

        private AssemblyLoaderReflectionCriterion(Predicate predicate) :
            base((object input, out string[] complaints) =>
                    predicate((Assembly)input, out complaints))
        {}
    }
}
