using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;
using SimpleGrain;
using System.Collections;

namespace UnitTestGrains
{
    public interface IObserverGrain : IGrain
    {
        Task SetTarget(ISimpleGrain target);
        Task Subscribe(ISimpleGrainObserver observer);
    }
}