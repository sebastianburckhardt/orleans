using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface IIdleActivationGcTestGrain1 : IGrain
    {
        Task Nop();
    }

    public interface IIdleActivationGcTestGrain2 : IGrain
    {
        Task Nop();
    }

    public interface IBusyActivationGcTestGrain1 : IGrain
    {
        Task Nop();
    }

    public interface IBusyActivationGcTestGrain2 : IGrain
    {
        Task Nop();
    }
}
