using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using System.Threading;
using SimpleGrain;

namespace UnitTestGrains
{
    /// <summary>
    /// A simple grain that allows to set two agruments and then multiply them.
    /// </summary>
    public class SimpleGrainWithAsyncMethods : SimpleGrain.SimpleGrain, ISimpleGrainWithAsyncMethods
    {
        AsyncValueResolver<int> resolver;

        public Task<int> GetAxB_Async()
        {
            return AsyncValue<int>.StartNew(() =>
            {
                return AsyncValue<int>.StartNew(() =>
                {
                    Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                    return GetAxB().Result;
                });
            }).AsTask();
        }

        public Task<int> GetAxB_Async(int a, int b)
        {
            return AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                return base.GetAxB(a, b).Result;
            }).AsTask();
        }
        public Task SetA_Async(int a)
        {
            return AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                base.SetA(a).Wait();
            }).AsTask();
        }
        public Task SetB_Async(int b)
        {
            return AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                base.SetB(b).Wait(); ;
            }).AsTask();
        }

        public Task IncrementA_Async()
        {
            return AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                base.IncrementA().Wait();
            }).AsTask();
        }

        public Task<int> GetA_Async()
        {
            return AsyncValue<int>.StartNew(() =>
            {
                Thread.Sleep(1000); // just to delay resolution of the promise for testing purposes
                return base.GetA().Result;
            }).AsTask();
        }

        public Task<int> GetX()
        {
            resolver = new AsyncValueResolver<int>();
            return resolver.AsyncValue.AsTask();
        }

        public Task SetX(int x)
        {
            resolver.Resolve(x);
            return TaskDone.Done;
        }
    }
}
