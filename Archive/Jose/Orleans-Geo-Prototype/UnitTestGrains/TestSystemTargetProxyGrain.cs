using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;


using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class TestSystemTargetProxyGrain : GrainBase, ITestSystemTargetProxyGrain
    {
        public Task SimpleVoidMethod(SiloAddress silo)
        {
            ITestSystemTarget testSystemTarget = GetReference(silo);
            return testSystemTarget.SimpleVoidMethod();
        }

        public Task<int> GetTwo(SiloAddress silo)
        {
            ITestSystemTarget testSystemTarget = GetReference(silo); ;
            return testSystemTarget.GetTwo();
        }

        public Task<int> GetCount(SiloAddress silo, List<SiloAddress> otherSilos)
        {
            ITestSystemTarget testSystemTarget = GetReference(silo);
            return testSystemTarget.GetCount(otherSilos);
        }


        private ITestSystemTarget GetReference(SiloAddress silo)
        {
            return TestSystemTargetFactory.GetSystemTarget(Constants.TestSystemTargetId, silo);
        }
    }
}