using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;


namespace UnitTestGrainInterfaces
{
    internal interface ICollectionTestGrain : IGrain
    {
        Task<TimeSpan> GetAge();

        Task DeactivateSelf();

        Task SetOther(ICollectionTestGrain other);

        Task<TimeSpan> GetOtherAge();

        Task<GrainId> GetGrainId();

        Task<string> GetRuntimeInstanceId();
    }
}
