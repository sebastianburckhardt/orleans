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
    /// General pattern for a async background worker that performs a task repeatedly when notified.
    /// Never executes more than one instance of the task, and consumes no resources when idle.
    /// </summary>
    public class BackgroundWorker
    {
        protected Func<Task> taskfactory;
        Task task;
        TaskCompletionSource<Task> nextworkcyclepromise;
        volatile bool morework;


        /// <summary>
        /// Create a worker that performs a single task repeatedly, when notified. 
        /// </summary>
        /// <param name="taskfactory">a function that, when called, starts and returns the task to be peformed</param>
        public BackgroundWorker(Func<Task> taskfactory)
        {
            this.taskfactory = taskfactory;
        }

        protected BackgroundWorker() { }

        /// <summary>
        /// call this to notify the worker that there may be more work.
        /// </summary>
        public void Notify()
        {
            lock (this)
            {
                if (task != null)
                {
                    morework = true;
                }
                else
                {
                    Start();
                }
            }
        }

        /// <summary>
        /// call this to notify the worker that there may be more work, and wait for the next work cycle
        /// </summary>
        public async Task NotifyAndWait()
        {
            Task<Task> waitfortasktask = null;
            Task waitfortask = null;

            lock (this)
            {
                if (task != null)
                {
                    morework = true;
                    if (nextworkcyclepromise == null)
                        nextworkcyclepromise = new TaskCompletionSource<Task>();
                    waitfortasktask = nextworkcyclepromise.Task;
                }
                else
                {
                    waitfortask = Start();
                }
            }

            if (waitfortasktask != null)
                await await waitfortasktask;

            else if (waitfortask != null)
                await waitfortask;
        }


        private Task Start()
        {
            var nextworkcycle = taskfactory();
            task = nextworkcycle.ContinueWith(t => this.CheckForMoreWork());
            return nextworkcycle;
        }


        public async Task WaitForCurrentWorkToBeServiced()
        {
            Task<Task> waitfortasktask = null;
            Task waitfortask = null;

            lock (this)
            {
                if (!morework)
                    waitfortask = task;
                else
                {
                    if (nextworkcyclepromise == null)
                        nextworkcyclepromise = new TaskCompletionSource<Task>();
                    waitfortasktask = nextworkcyclepromise.Task;
                }
            }

            if (waitfortasktask != null)
                await await waitfortasktask;

            else if (waitfortask != null)
                await waitfortask;
        }

        private void CheckForMoreWork()
        {
            Action signal_thunk = null;

            lock (this)
            {
                if (morework)
                {
                    morework = false;
                    var x = this.nextworkcyclepromise;
                    this.nextworkcyclepromise = null;
                    var nextworkcycle = Start();
                    if  (x != null)
                        signal_thunk = () => { x.SetResult(nextworkcycle); };
                }
                else
                {
                    task = null;
                }
            }

            // to be safe, do the signal here so it is not under the lock
            if (signal_thunk != null)
                signal_thunk();
        }


        public async Task WaitForQuiescence()
        {

            while (true)
            {
                Task t;
                lock (this)
                {
                    t = this.task;
                }
                if (t == null)
                    return;
                await t;
            }
        }
      

    }


}