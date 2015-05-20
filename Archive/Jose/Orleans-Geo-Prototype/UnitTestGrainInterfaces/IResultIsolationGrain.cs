using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface IResultIsolationGrain : IGrain
    {
        Task CheckResultIsolation(IResultIsolationGrain2 grain2);
    }
}