using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans
{
    /// <summary>
    /// Utility functions for dealing with Task's.
    /// </summary>
    public static class PublicOrleansTaskExtentions
    {
        /// <summary>
        /// Observes and ignores a potential exception on a given Task.
        /// If a Task fails and throws an exception which is never observed, it will be caught by the .NET finalizer thread.
        /// This function awaits the given task and if the exception is thrown, it observes this exception and simply ignores it.
        /// This will prevent the escalation of this exception to the .NET finalizer thread.
        /// </summary>
        /// <param name="task">The task to be ignored.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "ignored")]
        public static async void Ignore(this Task task)
        {
            try
            {
                await task;
            }
            catch (Exception)
            {
                var ignored = task.Exception; // Observe exception
            }
        }

        // Better to use await and not CW!
        //[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1804:RemoveUnusedLocals", MessageId = "ignored")]
        //public static void Ignore(this Task t)
        //{
        //    t.ContinueWith(task =>
        //    {
        //        var ignored = task.Exception; // Observe exception
        //    },
        //        TaskContinuationOptions.OnlyOnFaulted
        //        | TaskContinuationOptions.ExecuteSynchronously);
        //}
    }

    internal static class OrleansTaskExtentions
    {
        internal static String ToString(this Task t)
        {
            if (t == null) return "null";
            //String s = "id=" + t.Id + " Status=" + t.Status + " P.id=" + ((t.Parent != null) ? t.Parent.Id.ToString() : "null");
            return string.Format("[Id={0}, Status={1}]", t.Id, Enum.GetName(typeof(TaskStatus), t.Status));
        }

        internal static String ToString<T>(this Task<T> t)
        {
            if (t == null) return "null";
            return string.Format("[Id={0}, Status={1}]", t.Id, Enum.GetName(typeof(TaskStatus), t.Status));
        }


        internal static void WaitWithThrow(this Task task, TimeSpan timeout)
        {
            if (!task.Wait(timeout))
            {
                throw new TimeoutException(String.Format("Task.WaitWithThrow has timed out after {0}.", timeout));
            }
        }

        internal static T WaitForResultWithThrow<T>(this Task<T> task, TimeSpan timeout)
        {
            if (!task.Wait(timeout))
            {
                throw new TimeoutException(String.Format("Task<T>.WaitForResultWithThrow has timed out after {0}.", timeout));
            }
            return task.Result;
        }

        /// <summary>
        /// This will apply a timeout delay to the task, allowing us to exit early
        /// </summary>
        /// <param name="taskToComplete">The task we will timeout after timeSpan</param>
        /// <param name="timeout">Amount of time to wait before timing out</param>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <returns>The completed task</returns>
        internal static async Task WithTimeout(this Task taskToComplete, TimeSpan timeout)
        {
            if (taskToComplete.IsCompleted)
            {
                await taskToComplete;
                return;
            }

            await Task.WhenAny(taskToComplete, Task.Delay(timeout));

            // We got done before the timeout, or were able to complete before this code ran, return the result
            if (taskToComplete.IsCompleted)
            {
                // Await this so as to propagate the exception correctly
                await taskToComplete;
                return;
            }

            // We did not complete before the timeout, we fire and forget to ensure we observe any exceptions that may occur
            taskToComplete.Ignore();
            throw new TimeoutException(String.Format("WithTimeout has timed out after {0}.", timeout));
        }

        /// <summary>
        /// This will apply a timeout delay to the task, allowing us to exit early
        /// </summary>
        /// <param name="taskToComplete">The task we will timeout after timeSpan</param>
        /// <param name="timeout">Amount of time to wait before timing out</param>
        /// <exception cref="TimeoutException">If we time out we will get this exception</exception>
        /// <returns>The value of the completed task</returns>
        public static async Task<T> WithTimeout<T>(this Task<T> taskToComplete, TimeSpan timeSpan)
        {
            if (taskToComplete.IsCompleted)
            {
                return await taskToComplete;
            }

            await Task.WhenAny(taskToComplete, Task.Delay(timeSpan));

            // We got done before the timeout, or were able to complete before this code ran, return the result
            if (taskToComplete.IsCompleted)
            {
                // Await this so as to propagate the exception correctly
                return await taskToComplete;
            }

            // We did not complete before the timeout, we fire and forget to ensure we observe any exceptions that may occur
            taskToComplete.Ignore();
            throw new TimeoutException(String.Format("WithTimeout has timed out after {0}.", timeSpan));
        }

        internal static Task<T> FromException<T>(Exception exception)
        {
            TaskCompletionSource<T> tcs = new TaskCompletionSource<T>(exception);
            tcs.TrySetException(exception);
            return tcs.Task;
        }
    }

    /// <summary>
    /// A special void 'Done' Task that is already in the RunToCompletion state.
    /// Equivalent to Task.FromResult(1).
    /// </summary>
    public static class TaskDone
    {
        private static readonly Task<int> DoneConstant = Task.FromResult(1);

        /// <summary>
        /// A special 'Done' Task that is already in the RunToCompletion state
        /// </summary>
        public static Task Done
        {
            get
            {
                return DoneConstant;
            }
        }
    }
}
