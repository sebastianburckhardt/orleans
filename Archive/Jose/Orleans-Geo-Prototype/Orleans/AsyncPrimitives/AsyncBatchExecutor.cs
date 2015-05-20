using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    /// <summary>
    /// For internal use only.
    /// Utility functions.
    /// </summary>
    internal class AsyncBatchExecutor
    {
        private List<TaskCompletionSource<bool>> actionPromises;

        public AsyncBatchExecutor()
        {
            this.actionPromises = new List<TaskCompletionSource<bool>>();
        }

        public Task SubmitNext()
        {
            TaskCompletionSource<bool> resolver = new TaskCompletionSource<bool>();
            actionPromises.Add(resolver);
            return resolver.Task;
        }

        public void Flush()
        {
            foreach (var tcs in actionPromises)
            {
                tcs.TrySetResult(true);
            }
            actionPromises.Clear();
        }
    }
}

