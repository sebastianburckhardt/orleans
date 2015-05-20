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
    public interface ISimpleGrainState : IGrainState
    {
        int A { get; set; }
        int EventDelay { get; set; }
        ObserverSubscriptionManager<ISimpleGrainObserver> Observers { get; set; }
    }
    /// <summary>
    /// A simple grain that allows to set two arguments and then multiply them.
    /// </summary>
    public class SimpleGrain : GrainBase<ISimpleGrainState>, ISimpleGrain 
    {
        protected int b = 0;

        private AsyncCompletionResolver interlock;

        private bool waiting;

        protected OrleansLogger logger;

        public override Task ActivateAsync()
        {
            interlock = null;
            waiting = false;
            State.EventDelay = 1000;
            logger = GetLogger(String.Format("SimpleGrain-{0}-{1}", base.Identity, base.RuntimeIdentity));
            logger.Info("Activate.");
            return TaskDone.Done;
        }

        public Task SetA(int a)
        {
            logger.Info("SetA={0}", a);
            this.State.A = a;
            AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(State.EventDelay);
                RaiseStateUpdateEvent();
            }).Ignore();
            return TaskDone.Done;
        }

        public Task SetB(int b)
        {
            this.b = b;
            AsyncCompletion.StartNew(() =>
            {
                Thread.Sleep(State.EventDelay);
                RaiseStateUpdateEvent();
            }).Ignore();
            return TaskDone.Done;
        }

        public Task IncrementA()
        {
            SetA(State.A + 1);
            //RaiseStateUpdateEvent(); -- done by SetA
            return TaskDone.Done;
        }

        public Task<int> GetAxB()
        {
            return Task.FromResult(State.A * b);
        }

        public Task<int> GetAxB(int a, int b)
        {
            return Task.FromResult(a * b);
        }

        public Task<int> GetA()
        {
            return Task.FromResult(State.A);
        }

        // todo: simplify this...
        Task<int> ISimpleGrain.A { get { return Task.FromResult(State.A); } }

        // for testing - must be called twice concurrently to release lock
        public Task ReadOnlyInterlock(int timeout)
        {
            if (interlock == null)
            {
                // set up interlock to wait for another message
                interlock = new AsyncCompletionResolver();
                return interlock.AsyncCompletion.AsTask();
            }
            // release existing interlock and clear it for next time
            interlock.Resolve();
            interlock = null;
            return TaskDone.Done;
        }

        public Task ExclusiveWait(int timeout)
        {
            if (waiting)
                throw new InvalidOperationException("Should not be called concurrently");
            waiting = true;
            //var result = new AsyncCompletionResolver();
            //new Timer(o => { waiting = false; result.Resolve(); }, null, timeout, -1);
            //return result.AsyncCompletion;
            Thread.Sleep(timeout);
            waiting = false;
            return TaskDone.Done;
        }

        public Task Subscribe(ISimpleGrainObserver observer)
        {
            State.Observers.Subscribe(observer);
            return TaskDone.Done;
        }

        public Task Unsubscribe(ISimpleGrainObserver observer)
        {
            State.Observers.Unsubscribe(observer);
            return TaskDone.Done;
        }

        protected void RaiseStateUpdateEvent()
        {
            State.Observers.Notify((ISimpleGrainObserver observer) =>
                {
                    observer.StateChanged(State.A, b);
                });
        }
    }
}
