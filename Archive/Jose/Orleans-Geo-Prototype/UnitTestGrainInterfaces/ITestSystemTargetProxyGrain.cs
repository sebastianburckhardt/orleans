using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;


namespace UnitTestGrainInterfaces
{
    public interface ITestSystemTargetProxyGrain : IGrain
    {
        Task SimpleVoidMethod(SiloAddress silo);

        Task<int> GetTwo(SiloAddress silo);

        Task<int> GetCount(SiloAddress silo, List<SiloAddress> otherSilos);
    }
}