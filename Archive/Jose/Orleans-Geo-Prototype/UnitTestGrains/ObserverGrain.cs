using System.Threading.Tasks;
using Orleans;
using SimpleGrain;

namespace UnitTestGrains
{
    public class ObserverGrain : GrainBase, IObserverGrain, ISimpleGrainObserver
    {
        protected  ISimpleGrainObserver Observer { get; set; } // supports only a single observer

        protected  ISimpleGrain Target { get; set; }

        #region IObserverGrain Members

        public Task SetTarget(ISimpleGrain target)
        {
            Target = target;
            return target.Subscribe(this);
        }

        public Task Subscribe(ISimpleGrainObserver observer)
        {
            this.Observer = observer;
            return TaskDone.Done;
        }

        #endregion

        #region ISimpleGrainObserver Members

        public void StateChanged(int a, int b)
        {
            Observer.StateChanged(a, b);
        }

        #endregion
    }
}