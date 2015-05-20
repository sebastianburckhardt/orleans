using System;
using Orleans;

using UnitTestGrainInterfaces;
using System.Threading.Tasks;

namespace UnitTestGrains
{
    public class ResultIsolationGrain : GrainBase, IResultIsolationGrain
    {
        public async Task CheckResultIsolation(IResultIsolationGrain2 grain2)
        {
            var list = await grain2.GetList();
            if (list.Count != 0)
            {
                throw new Exception("List is not empty on creation");
            }
            list.Add(4);

            var list2 =  await grain2.GetList();
            if (list2.Count != 0)
            {
                throw new Exception("Result list is shared between grains in the same silo");
            }
        }
    }
}