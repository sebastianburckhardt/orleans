using System;
using System.Collections.Generic;
using System.Threading.Tasks;



namespace Orleans.Runtime
{
    internal class TestSystemTarget : SystemTarget, ITestSystemTarget
    {
        public TestSystemTarget(Silo currentSilo) : base(Constants.TestSystemTargetId, currentSilo.SiloAddress)
        {
        }

        public Task SimpleVoidMethod()
        {
            return TaskDone.Done;
        }

        public Task<int> GetTwo()
        {
            return Task.FromResult(2);
        }

        public async Task<int> GetCount(List<SiloAddress> otherSilos)
        {
            if (otherSilos.Count > 0)
            {
                SiloAddress nextSilo = otherSilos[0];
                otherSilos.RemoveAt(0);
                ITestSystemTarget testSystemTarget = TestSystemTargetFactory.GetSystemTarget(Constants.TestSystemTargetId, nextSilo);
                return 1 + await testSystemTarget.GetCount(otherSilos);
            }
            else
            {
                return 1;
            }
        }
    }
}