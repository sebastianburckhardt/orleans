using System;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface ICachedPropertiesGrain : IGrain
    {
        [Cacheable(Duration = "00:00:10")]
        Task<DateTime> A { get; }

        Task<DateTime> B { get; }

        Task<DateTime> GetA();

        Task SetA(DateTime a);

        Task<DateTime> GetB();

        Task SetB(DateTime b);
    }
}
