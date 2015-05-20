using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class ResultIsolationGrain2 : GrainBase, IResultIsolationGrain2
    {
        private List<int> list;

        public override Task ActivateAsync()
        {
            list = new List<int>();
            return TaskDone.Done;
        }

        public Task<List<int>> GetList()
        {
            return Task.FromResult(list);
        }
    }
}