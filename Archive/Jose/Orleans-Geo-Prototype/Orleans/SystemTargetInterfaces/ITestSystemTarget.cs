using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;


namespace Orleans
{
    internal interface ITestSystemTarget : ISystemTarget
    {
        Task SimpleVoidMethod();
    
        Task<int> GetTwo();

        Task<int> GetCount(List<SiloAddress> otherSilos);
    }
}