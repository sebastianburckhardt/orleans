using System;

namespace Orleans.Providers
{
    internal class AssemblyLoaderPathNameCriterion : AssemblyLoaderCriterion
    {
        internal new delegate bool Predicate(string pathName, out string[] complaints);

        internal static AssemblyLoaderPathNameCriterion NewCriterion(Predicate predicate)
        {
            if (predicate == null)
                throw new ArgumentNullException("predicate");
            return 
                new AssemblyLoaderPathNameCriterion(predicate);
        }

        private AssemblyLoaderPathNameCriterion(Predicate predicate) :
            base((object input, out string[] complaints) =>
                    predicate((string)input, out complaints))
        {}
    }
}
