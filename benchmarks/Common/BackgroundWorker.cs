using Common;
using Orleans;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;

namespace Common
{
    /// <summary>
    /// General pattern for a async background worker that performs a task repeatedly when notified.
    /// Never executes more than one instance of the task, and consumes no resources when idle.
    /// </summary>
    public class BackgroundWorker
    {
        protected Func<Task> taskfactory;
        Task task;
        TaskCompletionSource<int> signal;
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
                    // signal current task
                    morework = true;
                else
                    Start();
            }
        }

        public Task CurrentTask()
        {
            lock (this)
                return task;
        }

        private void Start()
        {
            using (new TraceInterval("WorkerThread"))
            {
                signal = new TaskCompletionSource<int>();
                task = (taskfactory()).ContinueWith(t => this.CheckForMoreWork());
            }
        }


        private void CheckForMoreWork()
        {
            using (new TraceInterval("Background - checkmorework", 0))
            {
                lock (this)
                {
                    if (morework)
                    {
                        morework = false;
                        Start();
                    }
                    else
                    {
                        task = null;
                    }
                }
            }
        }


        public async Task WaitForCompletion()
        {
            using (new TraceInterval("WaitForCompletion")) { 
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


}