using System;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public interface ICachedPropertiesGrainState : IGrainState
    {
        DateTime A { get; set; }
        DateTime B { get; set; }
    }
    public class CachedPropertiesGrain : GrainBase<ICachedPropertiesGrainState>, ICachedPropertiesGrain
    {
        public Task<DateTime> GetA()
        {
            return Task.FromResult(State.A);
        }

        public Task SetA(DateTime a)
        {
            State.A = a;
            return TaskDone.Done;
        }

        public Task<DateTime> GetB()
        {
            return Task.FromResult(State.B);
        }

        public Task SetB(DateTime b)
        {
            State.B = b;
            return TaskDone.Done;
        }

        public Task<DateTime> A
        {
            get { return Task.FromResult(State.A); }
        }

        public Task<DateTime> B
        {
            get { return Task.FromResult(State.B); }
        }
    }
}
