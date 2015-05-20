using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;



namespace Orleans
{
    /// <summary>
    /// Client gateway interface for obtaining the grain interface/type map.
    /// </summary>
    internal interface ITypeManager : ISystemTarget
    {
        Task<GrainInterfaceMap> GetTypeCodeMap(SiloAddress silo);
    }
}
