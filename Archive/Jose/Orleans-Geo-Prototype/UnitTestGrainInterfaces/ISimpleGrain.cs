using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;

using System.Collections;


namespace SimpleGrain
{
    public interface ISimpleGrain : IGrain
    {
        Task SetA(int a);
        Task SetB(int a);
        Task IncrementA();
        Task<int> GetAxB();
        Task<int> GetAxB(int a, int b);
        Task<int> GetA();
        Task<int> A { get; }
        //[ReadOnly]
        Task ReadOnlyInterlock(int timeout);
        Task ExclusiveWait(int timeout);
        Task Subscribe(ISimpleGrainObserver observer);
        Task Unsubscribe(ISimpleGrainObserver observer);
    }

    public interface ISimpleGrainObserver : IGrainObserver
    {
        void StateChanged(int a, int b);
    }

    public interface ISimpleCLIGrain : ISimpleGrain
    {
    }

    //public interface ISimpleOrleansManagedGrain : IAddressable
    //{
    //    Task SetA(int a);
    //    Task SetB(int a);
    //    Task IncrementA();
    //    Task<int> GetAxB();
    //    Task<int> GetAxB(int a, int b);
    //    Task<int> GetA();
    //    Task<int> A { get; }
    //    [ReadOnly]
    //    Task ReadOnlyInterlock(int timeout);
    //    Task ExclusiveWait(int timeout);
    //    Task Subscribe(ISimpleGrainObserver observer);
    //    Task Unsubscribe(ISimpleGrainObserver observer);
    //}
}