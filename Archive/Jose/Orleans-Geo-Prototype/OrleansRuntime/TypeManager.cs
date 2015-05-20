using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;




namespace Orleans.Runtime
{
    internal class TypeManager : SystemTarget, ITypeManager
    {
        private readonly GrainTypeManager grainTypeManager;

        internal TypeManager(SiloAddress myAddr, GrainTypeManager grainTypeManager)
            : base(Constants.TypeManagerId, myAddr)
        {
            this.grainTypeManager = grainTypeManager;
        }


        public Task<GrainInterfaceMap> GetTypeCodeMap(SiloAddress silo)
        {
            return Task.FromResult(grainTypeManager.GetTypeCodeMap());
        }
    }
}


