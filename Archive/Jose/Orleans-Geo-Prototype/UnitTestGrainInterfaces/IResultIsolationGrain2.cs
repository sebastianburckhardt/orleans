using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface IResultIsolationGrain2 : IGrain
    {
        Task<List<int>> GetList();
    }
}