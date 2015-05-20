using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Runtime;

using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class IdleActivationGcTestGrain1: GrainBase, IIdleActivationGcTestGrain1
    {
        public Task Nop()
        {
            return TaskDone.Done;
        }
    }

    public class IdleActivationGcTestGrain2: GrainBase, IIdleActivationGcTestGrain2
    {
        public Task Nop()
        {
            return TaskDone.Done;
        }
    }

    public class BusyActivationGcTestGrain1: GrainBase, IBusyActivationGcTestGrain1
    {
        public Task Nop()
        {
            return TaskDone.Done;
        }
    }

    public class BusyActivationGcTestGrain2: GrainBase, IBusyActivationGcTestGrain2
    {
        public Task Nop()
        {
            return TaskDone.Done;
        }
    }
}
