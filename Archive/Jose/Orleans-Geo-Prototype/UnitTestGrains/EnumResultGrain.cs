using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class EnumResultGrain : GrainBase, IEnumResultGrain
    {
        public Task<CampaignEnemyTestType> GetEnemyType()
        {
            return Task.FromResult(CampaignEnemyTestType.Enemy2);
        }

        public Task<OrleansConfiguration> GetConfiguration()
        {
            throw new NotImplementedException();
        }
    }
}
