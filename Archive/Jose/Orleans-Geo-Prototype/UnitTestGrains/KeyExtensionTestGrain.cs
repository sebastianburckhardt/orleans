using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    internal class KeyExtensionTestGrain : GrainBase, IKeyExtensionTestGrain
    {
        public Task<GrainId> GetGrainId()
        {
            return Task.FromResult(Identity);
        }

        public Task<ActivationId> GetActivationId()
        {
            return Task.FromResult(_Data.ActivationId);
        }
    }
}
