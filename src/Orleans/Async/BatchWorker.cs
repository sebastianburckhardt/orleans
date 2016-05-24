using Orleans;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace Orleans
{
    /// <summary>
    /// General pattern for an asynchronous worker that performs a work task, when notified,
    /// to service queued work. Each work cycle handles ALL the queued work. 
    /// If new work arrives during a work cycle, another cycle is scheduled. 
    /// The worker never executes more than one instance of the work cycle at a time, 
    /// and consumes no resources when idle. It uses TaskScheduler.Current 
    /// to schedule the work cycles.
    /// </summary>
    public abstract class BatchWorker
    {
        // Subclass overrides this to define what constitutes a work cycle
        protected abstract Task Work();

        /// <summary>
        /// Notify the worker that there is more work.
        /// </summary>
        public void Notify()
        {
            lock (this)
            {
                if (currentworkcycle != null)
                {
                    // lets the current work cycle know that there is more work
                    morework = true;
                }
                else
                {
                    // start a work cycle
                    Start();
                }
            }
        }

        // task for the current work cycle, or null if idle
        private Task currentworkcycle;
 
        // flag is set to indicate that more work has arrived during execution of the task
        private volatile bool morework;

        // is non-null if some task is waiting for the next work cycle to finish
        private TaskCompletionSource<Task> nextworkcyclepromise;

        private void Start()
        {
            // start the task that is doing the work
            currentworkcycle = Work();

            // chain a continuation that checks for more work, on the same scheduler
            currentworkcycle.ContinueWith(t => this.CheckForMoreWork(), TaskScheduler.Current);
        }

        // executes at the end of each work cycle
        // on the same task scheduler
        private void CheckForMoreWork()
        {
            Action signal_thunk = null;

            lock (this)
            {
                if (morework)
                {
                    morework = false;

                    // see if someone created a promise for waiting for the next work cycle
                    // if so, take it and remove it
                    var x = this.nextworkcyclepromise;
                    this.nextworkcyclepromise = null;

                    // start the next work cycle
                    Start();

                    // if someone is waiting, signal them
                    if (x != null)
                        signal_thunk = () => { x.SetResult(currentworkcycle); };
                }
                else
                {
                    currentworkcycle = null;
                }
            }

            // to be safe, must do the signalling out here so it is not under the lock
            if (signal_thunk != null)
                signal_thunk();
        }

        /// <summary>
        /// Check if this worker is busy.
        /// </summary>
        /// <returns></returns>
        public bool IsIdle()
        {
            lock (this)
            {
                return currentworkcycle == null;
            }
        }

        /// <summary>
        /// Notify the worker that there is more work, and wait for that work to be serviced
        /// </summary>
        public async Task NotifyAndWait()
        {
            Task<Task> waitfortasktask = null;
            Task waitfortask = null;

            lock (this)
            {
                if (currentworkcycle != null)
                {
                    morework = true;
                    if (nextworkcyclepromise == null)
                        nextworkcyclepromise = new TaskCompletionSource<Task>();
                    waitfortasktask = nextworkcyclepromise.Task;
                }
                else
                {
                    Start();
                    waitfortask = currentworkcycle;
                }
            }

            if (waitfortasktask != null)
                await await waitfortasktask;

            else if (waitfortask != null)
                await waitfortask;
        }


        /// <summary>
        /// Wait for the current work cycle, and also the next work cycle if there is currently unserviced work.
        /// </summary>
        /// <returns></returns>
        public async Task WaitForCurrentWorkToBeServiced()
        {
            Task<Task> waitfortasktask = null;
            Task waitfortask = null;

            // figure out exactly what we need to wait for
            lock (this)
            {
                if (!morework)
                    // just wait for current work cycle
                    waitfortask = currentworkcycle;
                else
                {
                    // we need to wait for the next work cycle
                    // but that task does not exist yet, so we use a promise that signals when the next work cycle is launched
                    if (nextworkcyclepromise == null)
                        nextworkcyclepromise = new TaskCompletionSource<Task>();
                    waitfortasktask = nextworkcyclepromise.Task;
                }
            }

            // now do the actual waiting outside of the lock

            if (waitfortasktask != null)
                await await waitfortasktask;

            else if (waitfortask != null)
                await waitfortask;
        }
    }

    /// A convenient variant of a batch worker 
    /// that allows the work function to be passed as a constructor argument
    public class BatchWorkerFromDelegate : BatchWorker
    {
        public BatchWorkerFromDelegate(Func<Task> work)
        {
            this.work = work;
        }

        private Func<Task> work;

        protected override Task Work()
        {
            return work();
        }
    }
}