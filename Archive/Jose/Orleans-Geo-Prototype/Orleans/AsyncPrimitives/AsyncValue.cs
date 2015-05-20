#define TRACKING
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;


namespace Orleans
{
    // Summary:
    //     Represents the current stage in the lifecycle of a AsyncCompletion/AsyncValue.
    /// <summary>
    /// This enumeration represents the possible states of a promise.
    /// </summary>
    internal enum AsyncCompletionStatus
    {
        //
        // Summary:
        //     The AsyncCompletion has not been resolved yet (still being executed)
        /// <summary>
        /// The promise is unresolved because the underlying operation has not completed.
        /// </summary>
        Running = 1,
        //
        // Summary:
        //     The AsyncCompletion has been resolved successfully.
        /// <summary>
        /// The promise has been successfully resolved.
        /// </summary>
        CompletedSuccessfully = 2,
        //
        // Summary:
        //     The AsyncCompletion has been broken.
        /// <summary>
        /// The promise has been broken.
        /// </summary>
        Faulted = 3,

        // we still do not supper TTLs and cancelations

        //
        // Summary:
        //     The AsyncCompletion has been broken execution has timed-out.
        //TimedOut = 4,
        //
        // Summary:
        //     The AsyncCompletion has been broken execution has been cancelled.
        //Canceled = 5,
    }

    // all asynchronous invocations must return a promise rather than a concrete type,
    // all asynchronous invocations cannot throw exceptions. Instead they can return a broken promise.
    // only blocking APIs (Wait, getValue) can throw OrleansRuntime exceptions.
    /// <summary>
    /// An instance of the <c>AsyncCompletion</c> class represents a completion promise with no value associated.
    /// Resolution of an <c>AsyncCompletion</c> indicates that the original asynchronous request was 
    /// completed successfully, but returns no other data.
    /// Thus, an <c>AsyncCompletion</c> is effectively the asynchronous equivalent of a void function.
    /// <para>
    /// Because successful <c>AsyncCompletion</c>s have no state and so are all identical, 
    /// there is no public constructor for a successful <c>AsyncCompletion</c>. 
    /// Instead, a method that completes successfully should return <c>AsyncCompletion.Done</c>,
    /// which is a pre-built successful <c>AsyncCompletion</c>.
    /// </para>
    /// </summary>
    internal class AsyncCompletion
    {
        internal readonly OrleansTask task;

        internal static ISchedulingContext Context
        {
            get
            {
                return RuntimeContext.Current != null ? RuntimeContext.Current.ActivationContext : null;
            }
        }

        internal static bool TrackObservations { get; set; }

        protected static Dictionary<AsyncCompletion, string> unobservedPromises;

        private static readonly AsyncCompletion AsyncCompletionDone = new AsyncCompletion(OrleansTask.Done(AsyncCompletion.Context));

        protected static readonly Object lockObject = new Object();

        private static readonly Logger logger = Logger.GetLogger("AsyncCompletion");

        static AsyncCompletion()
        {
            lock (lockObject)
            {
                unobservedPromises = new Dictionary<AsyncCompletion, string>();
            }
        }

        internal AsyncCompletion(OrleansTask task)
        {
            if (task == null)
                throw new ArgumentNullException("task");
            this.task = task;
#if TRACKING
            if (TrackObservations)
            {
                lock (lockObject)
                {
                    unobservedPromises[this] = new StackTrace(1, true).ToString();
                }
            }
#endif
        }

        /// <summary>
        /// Creates a new, broken <c>AsyncCompletion</c> with the given <c>Exception</c> as the reason.
        /// This constructor should be used by a grain method that wants to return an error to its caller.
        /// </summary>
        /// <param name="exception">The <c>Exception</c> that describes why the promise was broken.</param>
        public AsyncCompletion(Exception exception)
            : this(exception, OrleansTask.FromException(exception, AsyncCompletion.Context))
        {
#if TRACKING
            if (TrackObservations)
            {
                lock (lockObject)
                {
                    unobservedPromises[this] = new StackTrace(1, true).ToString();
                }
            }
#endif
        }

        internal AsyncCompletion(Exception exception, OrleansTask task)
        {
            if (exception == null)
                throw new ArgumentNullException("exception");
            if (task == null)
                throw new ArgumentNullException("task");
            this.task = task;
#if TRACKING
            if (TrackObservations)
            {
                lock (lockObject)
                {
                    unobservedPromises[this] = new StackTrace(1, true).ToString();
                }
            }
#endif
        }

        /// <summary>
        /// Generate an <c>AsyncCompletion</c> from a given <c>Exception</c> with stack trace.
        /// This should be used by a code that wants to return an error to its caller with an embedded stack trace.
        /// </summary>
        /// <param name="exc">The <c>Exception</c> that describes why the promise was broken.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion GenerateFromException(Exception exc)
        {
            try
            {
                throw exc;
            }
            catch (Exception exc2)
            {
                return new AsyncCompletion(exc2);
            }
        }
#if TRACKING
        internal void Observe()
        {
            if (TrackObservations)
            {
                lock (lockObject)
                {
                    unobservedPromises.Remove(this);
                }
            }
        }
#endif

        /// <summary>
        /// Queues up an action to run asynchronously and returns the promise associated with its completion.
        /// The returned <c>AsyncCompletion</c> is resolved when the action completes executing.
        /// </summary>
        /// <param name="action">The action to execute asynchronously.</param>
        /// <returns>The <c>AsyncCompletion</c> associated with the action's completion.</returns>
        public static AsyncCompletion StartNew(Action action)
        {
            ISchedulingContext context = AsyncCompletion.Context;
            return new AsyncCompletion(OrleansTask.StartNew(action, context));
        }

        /// <summary>
        /// Queues up a function to run asynchronously and returns the promise associated with its completion.
        /// The returned <c>AsyncCompletion</c> is resolved when function's returned promise is resolved.
        /// </summary>
        /// <param name="function">The function to execute asynchronously.</param>
        /// <returns>The <c>AsyncCompletion</c> associated with the function's returned promise.</returns>
        public static AsyncCompletion StartNew(Func<AsyncCompletion> function)
        {
            ISchedulingContext context = AsyncCompletion.Context;
            return new AsyncCompletion(OrleansTask.StartNew(function, context));
        }

        /// <summary>
        /// Execute a work item on a .NET thread pool thread, returning an AsyncCompletion promise that will be triggered when execution of the work item is completed.
        /// </summary>
        /// <param name="action">Action to be executed using .NET Thread Pool</param>
        /// <returns>An AsyncCompletion promise which will be </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion ExecuteOnThreadPool(Action action)
        {
            AsyncCompletionResolver ar = new AsyncCompletionResolver();
            WaitCallback DoWork = (data) =>
            {
                try
                {
                    action(); // Do work
                    ar.Resolve(); // Resolve promise
                }
                catch (Exception exc)
                {
                    ar.Break(exc); // Break promise
                }
            };
            ThreadPool.QueueUserWorkItem(DoWork, null);
            return ar.AsyncCompletion;
        }

        /// <summary>
        /// Execute a work item on a .NET thread pool thread, returning an AsyncCompletion promise that will be triggered when execution of the work item is completed.
        /// </summary>
        /// <param name="action">Action to be executed using .NET Thread Pool</param>
        /// <param name="state">Any state data to be passed to the Action closure when executing</param>
        /// <returns>An AsyncCompletion promise which will be </returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion ExecuteOnThreadPool(Action<object> action, object state)
        {
            AsyncCompletionResolver ar = new AsyncCompletionResolver();
            WaitCallback DoWork = (data) =>
            {
                try
                {
                    action(data); // Do work
                    ar.Resolve(); // Resolve promise
                }
                catch (Exception exc)
                {
                    ar.Break(exc); // Break promise
                }
            };
            ThreadPool.QueueUserWorkItem(DoWork, state);
            return ar.AsyncCompletion;
        }

        /// <summary>
        /// Constructs a promise from an IAsyncResult and an end delegate that follow the .NET asynchronous programming model.
        /// The end delegate is guaranteed to run in a separate new turn, never inline with the begin, 
        /// even if IAsyncResult is already resolved (unlike the TPL implementation, which may run it inline or not).
        /// </summary>
        /// <param name="asyncResult"></param>
        /// <param name="endDelegate"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion FromAsync(IAsyncResult asyncResult, Action<IAsyncResult> endDelegate)
        {
            //var t = OrleansTask.GetFactory().FromAsync(asyncResult, endDelegate);
            //return new AsyncCompletion(new OrleansTask(t));
            var resolver = new AsyncCompletionResolver();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(asyncResult, iar =>
                    OrleansTask.StartNew(() =>
                    {
                        try
                        {
                            endDelegate(iar);
                            resolver.TryResolve();
                        }
                        catch (Exception ex)
                        {
                            resolver.TryBreak(ex);
                        }
                    }, context));
            return resolver.AsyncCompletion;
        }

        /// <summary>
        /// Constructs a promise from a pair of begin/end delegates that follow the .NET asynchronous programming model.
        /// </summary>
        /// <param name="beginDelegate"></param>
        /// <param name="endDelegate"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion FromAsync(Func<AsyncCallback, object, IAsyncResult> beginDelegate, Action<IAsyncResult> endDelegate, object param)
        {
            var resolver = new AsyncCompletionResolver();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                    {
                        try
                        {
                            endDelegate(iar);
                            resolver.TryResolve();
                        }
                        catch (Exception ex)
                        {
                            resolver.TryBreak(ex);
                        }
                    }, context), param);
            return resolver.AsyncCompletion;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion FromAsync<TArg1>(Func<TArg1, AsyncCallback, object, IAsyncResult> beginDelegate, Action<IAsyncResult> endDelegate, TArg1 arg1, object param)
        {
            var resolver = new AsyncCompletionResolver();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        endDelegate(iar);
                        resolver.TryResolve();
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, param);
            return resolver.AsyncCompletion;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion FromAsync<TArg1, TArg2>(Func<TArg1, TArg2, AsyncCallback, object, IAsyncResult> beginDelegate, Action<IAsyncResult> endDelegate,
            TArg1 arg1, TArg2 arg2, object param)
        {
            var resolver = new AsyncCompletionResolver();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        endDelegate(iar);
                        resolver.TryResolve();
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, arg2, param);
            return resolver.AsyncCompletion;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion FromAsync<TArg1, TArg2, TArg3>(Func<TArg1, TArg2, TArg3, AsyncCallback, object, IAsyncResult> beginDelegate, Action<IAsyncResult> endDelegate,
            TArg1 arg1, TArg2 arg2, TArg3 arg3, object param)
        {
            var resolver = new AsyncCompletionResolver();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        endDelegate(iar);
                        resolver.TryResolve();
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, arg2, arg3, param);
            return resolver.AsyncCompletion;
        }

        /// <summary>
        /// Whether this AsyncCompletion is already completed - <c>true</c> if it been run to completion and has Status == CompletedSuccessfully or Faulted
        /// </summary>
        public bool IsCompleted
        {
            get { return task.Status != AsyncCompletionStatus.Running; }
        }

        /// <summary>
        /// Whether this AsyncCompletion is faulted - <c>true</c> if it been run to completion and has Status == Faulted
        /// </summary>
        public bool IsFaulted
        {
            get { return task.Status == AsyncCompletionStatus.Faulted; }
        }

        /// <summary>
        /// Convert this AsyncCompletion to a Task
        /// </summary>
        /// <returns>Task-based async version of this AsyncCompletion</returns>
        public Task AsTask()
        {
            return OrleansTask.AsyncCompletionToTask(this);
        }

        /// <summary>
        /// Convert this Task into an AsyncCompletion
        /// </summary>
        /// <returns>AsyncCompletion wrapper for this Task</returns>
        public static AsyncCompletion FromTask(Task task)
        {
            if (task == null) return AsyncCompletion.Done;

            var resolver = new AsyncCompletionResolver();

            if (task.Status == TaskStatus.RanToCompletion)
            {
                resolver.Resolve();
            }
            else if (task.IsFaulted)
            {
                resolver.Break(task.Exception.Flatten());
            }
            else if (task.IsCanceled)
            {
                resolver.Break(new TaskCanceledException(task));
            }
            else
            {
                if (task.Status == TaskStatus.Created) task.Start();

                task.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        resolver.Break(t.Exception.Flatten());
                    }
                    else if (t.IsCanceled)
                    {
                        resolver.Break(new TaskCanceledException(t));
                    }
                    else
                    {
                        resolver.Resolve();
                    }
                });
            }

            return resolver.AsyncCompletion;
        }

        /// <summary>
        /// Explicitly ignores the resolution of an <c>AsyncCompletion</c>.
        /// This "observes" the resolution of the promise.
        /// <para>Every <c>AsyncCompletion</c> should have its resolution "observed" by invoking either this method,
        /// the <c>ContinueWith</c> method, the <c>Wait</c> method, or the <c>TryWait</c> method.
        /// This method is appropriate when the caller doesn't care whether the original asynchronous request
        /// succeeded or failed, and so whether the promise is resolved successfully or is broken has no effect
        /// on the caller's processing.</para>
        /// </summary>
        public void Ignore()
        {
#if TRACKING
            Observe();
#endif
            task.Ignore(logger);
        }

        /// <summary>
        /// Waits forever for the promise's original asynchronous request to complete.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. 
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        internal void Wait()
        {
#if TRACKING
            Observe();
#endif
            task.Wait(Constants.INFINITE_TIMESPAN);
        }

        /// <summary>
        /// Waits a given period of time for the promise's original asynchronous request to complete.
        /// If the request doesn't complete in time, a <c>TimeoutException</c> is thrown.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. 
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="timeout">The period of time to wait.</param>
        public void Wait(TimeSpan timeout)
        {
#if TRACKING
            Observe();
#endif
            if (!task.Wait(timeout))
            {
                Ignore();
                throw new TimeoutException();
            }
        }

        /// <summary>
        /// Waits for a given period of time for the promise's original asynchronous request to complete.
        /// If the request doesn't complete in time, <c>false</c> is returned.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. 
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="timeout">The period of time to wait.</param>
        /// <returns><c>true</c> if the request completed in time, <c>false</c> if not.</returns>
        internal bool TryWait(TimeSpan timeout)
        {
#if TRACKING
            Observe();
#endif
            return task.Wait(timeout);
        }

        /// <summary>
        /// The exception that describes the reason that this promise was broken.
        /// If the request succeeded, then this will be <c>null</c>.
        /// </summary>
        public Exception Exception { get { return task.Exception; } }


        // If the target (this) promise finishes without throwing (was resolved successfully) – run the continuationAction, Otherwise, run the pExceptionAction.
        // If the continuationAction has thrown an exception -  run pExceptionAction on this exception.
        // pExceptionAction can either recover the error and return a new value, in which case the ContinueWith returned AsyncCompletion will be resolved successfully, 
        // OR it can throw or re-throw by itself, in which case it will be broken.
        /// <summary>
        /// Schedules an action to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another action to be executed if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The action to run if and when this promise is successfully resolved.</param>
        /// <param name="exceptionAction">An optional action to run when this promise is broken. 
        /// This action gets passed the exception related to the breaking this promise.
        /// This action is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this action is not passed, then if this promise is broken, or if the continuation action throws an exception,
        /// then the returned promise is broken.</param>
        /// <returns>A promise that is resolved when the continuation or exception action completes.
        /// If the exception action throws an exception (or, if there is no exception action and either the promise is broken or if the continuation action throws an exception),
        /// then the returned promise is broken.</returns>
        public AsyncCompletion ContinueWith(Action continuationAction, Action<Exception> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncCompletion(task.ContinueWithAction(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        internal AsyncCompletion SystemContinueWith(Action continuationAction, Action<Exception> pExceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            return new AsyncCompletion(task.ContinueWithAction(continuationAction, pExceptionAction, null)); // system continuation runs outside the context.
        }

        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        internal AsyncValue<TResult> SystemContinueWith<TResult>(Func<TResult> continuationAction, Func<Exception, TResult> pExceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            return new AsyncValue<TResult>(task.ContinueWithFunction(continuationAction, pExceptionAction, null)); // system continuation runs outside the context.
        }

        /// <summary>
        /// Schedules a function to be executed (asynchronously) if and when this promise is resolved successfully.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The function to run when this promise is resolved.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This function gets passed the exception related to the breaking this promise, and must
        /// return <c>AsyncCompletion</c>.
        /// This function is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this function is not passed, then if this promise is broken, or if the continuation function throws an exception,
        /// then the returned promise is broken.</param>
        /// <returns>The <c>AsyncCompletion</c> that is the result of the function.
        /// If this promise (the one that ContinueWith is invoked on) is broken 
        /// (or, if there is no exception function and either the promise is broken or if the continuation function throws an exception), 
        /// or if the function throws an exception, then a broken promise is returned.</returns>
        public AsyncCompletion ContinueWith(Func<AsyncCompletion> continuationAction, Func<Exception, AsyncCompletion> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncCompletion(task.ContinueWithAsyncCompletion(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// Schedules a function to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another function to be execute if and when this promise is broken.
        /// This method is useful as a way to specify a default result if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The function to run if and when this promise is successfully resolved.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This action gets passed the exception related to the breaking this promise, and must
        /// return the same data type as the <c>continuationAction</c>.
        /// This action is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this function is not passed, then if this promise is broken, or if the continuation function throws an exception,
        /// then the returned promise is broken.</param>
        /// <returns>A promise for the result of the continuation or exception action.
        /// If the exception action throws an exception (or, if there is no exception action and either the promise is broken or if the continuation function throws an exception),
        /// then the returned promise is broken.</returns>
        public AsyncValue<TResult> ContinueWith<TResult>(Func<TResult> continuationAction, Func<Exception, TResult> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncValue<TResult>(task.ContinueWithFunction(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// Schedules a function to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another function to be execute if and when this promise is broken.
        /// This method is useful as a way to specify a default result if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The function to run if and when this promise is successfully resolved.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This action gets passed the exception related to the breaking this promise, and must
        /// return the same data type as that promised by the <c>continuationAction</c>.
        /// This action is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this function is not passed, then if this promise is broken, or if the 
        /// continuation function throws an exception, then the returned promise is broken.</param>
        /// <returns>The promise that is the result of the continuation, or a promise for the result of the exception action.
        /// If the exception action throws an exception (or, if there is no exception action and either the promise is broken or if the continuation function throws an exception), 
        /// then the returned promise is broken.</returns>
        public AsyncValue<TResult> ContinueWith<TResult>(Func<AsyncValue<TResult>> continuationAction, Func<Exception, AsyncValue<TResult>> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncValue<TResult>(task.ContinueWithFunction(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// This static property provides a resolved <c>AsyncCompletion</c> suitable for returning from a successful method call.
        /// </summary>
        public static AsyncCompletion Done { get { return AsyncCompletionDone; } }

        /// <summary>
        /// This static property provides a resolved <c>Task</c> suitable for returning from a successful method call.
        /// </summary>
        public static Task TaskDone { get { return Orleans.TaskDone.Done; } }

        internal virtual object GetObjectValue() { return null; }

        /// <summary>
        /// This property returns the status of the promise. See <see cref="Orleans.AsyncCompletionStatus"/> for possible return values.
        /// </summary>
        public AsyncCompletionStatus Status { get { return task.Status; } }

        /// <summary>
        /// Joins two promises into a single combined promise.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="ac1">The first promise to join.</param>
        /// <param name="ac2">The second promise to join.</param>
        /// <returns>A new promise that resolves when both of the two joining promises are resolved.
        /// The new promise is broken if either of the two combining promises break.</returns>
        public static AsyncCompletion Join(AsyncCompletion ac1, AsyncCompletion ac2)
        {
            if (ac1 == null)
                throw new ArgumentNullException("ac1");
            if (ac2 == null)
                throw new ArgumentNullException("ac2");
            return JoinAll(new AsyncCompletion[] { ac1, ac2 });
        }

        /// <summary>
        /// Joins multiple promises into a single combined promise.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="acs">An array that holds the promises to join.</param>
        /// <returns>A new promise that resolves when all of the joining promises are resolved.
        /// The new promise is broken if one or more of the combining promises break.</returns>
        public static AsyncCompletion JoinAll(AsyncCompletion[] acs)
        {
            if (acs == null)
                throw new ArgumentNullException("acs");
            for (int i = 0; i < acs.Length; i++)
            {
                if (acs[i] == null)
                    throw new ArgumentNullException(String.Format("acs[{0}]", i));
            }

            if (acs.Length == 0)
            {
                return Done;
            }
            var context = AsyncCompletion.Context;
#if TRACKING
            foreach (var ac in acs)
            {
                ac.Observe();
            }
#endif
            return new AsyncCompletion(OrleansTask.JoinAll(acs, context));
        }

        /// <summary>
        /// Joins multiple promises into a single combined promise.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="acs">A collection that holds the promises to join.</param>
        /// <returns>A new promise that resolves when all of the joining promises are resolved.
        /// The new promise is broken if one or more of the combining promises break.</returns>
        public static AsyncCompletion JoinAll(IEnumerable<AsyncCompletion> acs)
        {
            if (acs == null)
                throw new ArgumentNullException("acs");

            return JoinAll(acs.ToArray());
        }

        /// <summary>
        /// Joins multiple grain reference promises into a single combined promise.
        /// See <see cref="IAddressable"/> for details on waiting for unresolved grain references.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="grains">A collection that holds the grain references to join.</param>
        /// <returns>A new promise that resolves when all of the joining references are resolved.
        /// The new promise is broken if one or more of the combining references break.</returns>
        public static AsyncCompletion JoinAll(IEnumerable<IAddressable> grains)
        {
            if (grains == null)
                throw new ArgumentNullException("grains");
            return JoinAll(grains.ToArray());
        }

        /// <summary>
        /// Waits a given period of time for multiple promises to resolve.
        /// If any of the requests don't complete in time, a <c>TimeoutException</c> is thrown.
        /// <para>This method does not fail fast. That is, even if one of the promises gets broken, the method will wait 
        /// (up until timeout) until all promises complete (resolve or break) and throw an AggregateException.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. </para>
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the combining promises.</para>
        /// </summary>
        /// <param name="acs">An array that holds the promises to wait for.</param>
        /// <param name="timeout">The period of time to wait.</param>
        public static void WaitAll(AsyncCompletion[] acs, TimeSpan timeout)
        {
            // JoinAll will"observes" the resolution of the combining promises. 
            AsyncCompletion promise = AsyncCompletion.JoinAll(acs);
#if TRACKING
            foreach (var ac in acs)
            {
                ac.Observe();
            }
#endif
            promise.Wait(timeout);

            //AsyncCompletion.JoinAll(acs).Wait(timeout);
            //throw new NotImplementedException("WaitAll should be implemented for the new scheduler.");
            //Task[] tasks = OrleansTask.AsyncCompletionsToTasks(acs);
            //bool allFinished = false;
            //try
            //{
            //    allFinished = Task.WaitAll(tasks, timeout);
            //    if (!allFinished)
            //    {
            //        throw new TimeoutException();
            //    }
            //}
            //finally
            //{
            //    // we wil execute IgnoreAll in 2 cases:
            //    // one of the waited tasks had thrown an exception, not all tasks have finished on time and we had thrown TimeoutException
            //    if (!allFinished)
            //    {
            //        IgnoreAll(acs);
            //    }
            //}
        }

        //        /// <summary>
        //        /// Waits a given period of time for the first of multiple promises to resolve.
        //        /// If none of the requests complete in time, a <c>TimeoutException</c> is thrown.
        //        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        //        /// may execute. 
        //        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        //        /// <para>This method "observes" the resolution of the combining promises.</para>
        //        /// </summary>
        //        /// <param name="acs">An array that holds the promises to wait for.</param>
        //        /// <param name="timeout">The period of time to wait.</param>
        //        internal static int WaitAny(AsyncCompletion[] acs, TimeSpan timeout)
        //        {
        //#if TRACKING
        //            foreach (var ac in acs)
        //            {
        //                ac.Observe();
        //            }
        //#endif
        //            Task[] tasks = OrleansTask.AsyncCompletionsToTasks(acs);
        //            int finished = -1;
        //            try
        //            {
        //                finished = Task.WaitAny(tasks, timeout);
        //                if (finished == -1)
        //                {
        //                    throw new TimeoutException();
        //                }
        //                return finished;
        //            }
        //            finally
        //            {
        //                // we wil execute IgnoreAll in 2 cases:
        //                // one of the waited tasks had thrown an exception, no task has finished on time and we had thrown TimeoutException
        //                IgnoreAll(acs);
        //            }
        //        }

        //        /// <summary>
        //        /// Waits for the first of multiple promises to resolve.
        //        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        //        /// may execute. 
        //        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        //        /// <para>This method "observes" the resolution of the combining promises.</para>
        //        /// </summary>
        //        /// <param name="acs">An array that holds the promises to wait for.</param>
        //        internal static int WaitAny(params AsyncCompletion[] acs)
        //        {
        //            return WaitAny(acs, System.Threading.Timeout.Infinite);
        //        }

        public static List<string> GetUnobservedPromises()
        {
#if TRACKING
            if (TrackObservations)
            {
                List<string> result;
                lock (lockObject)
                {
                    result = unobservedPromises.Values.ToList();
                    foreach (var promise in unobservedPromises.Keys.ToList())
                    {
                        promise.Ignore();
                    }
                    unobservedPromises.Clear();
                }
                return result;
            }
            else
            {
                return new List<string>();
            }
#else
            return new List<string>();
#endif
        }

        internal void IntentionallyUnobserved()
        {
#if TRACKING
            Observe();
#endif
        }
    }

    /// <summary>
    /// The <c>AsyncResolver</c> class is the base class for <c>AsyncCompletionResolver</c> and
    /// <c>AsyncValueResolver</c>.
    /// <para>
    /// These classes provide a mechanism for developers to create promises whose resolution they
    /// control explicitly, rather than resolving when some request completes.
    /// There are relatively few occasions where this is necessary, but sometimes it is the only way to
    /// get the desired behavior.
    /// In general, though, if you think you need to use an explicit resolver/promise pair, you should think
    /// your scenario through carefully, because there's probably a better way to get the flow you want with
    /// a simple request-linked promise.
    /// </para>
    /// </summary>
    internal abstract class AsyncResolver
    {
        /// <summary>
        /// The AsyncCompletion that is resolved by this resolver.
        /// </summary>
        public abstract AsyncCompletion AsyncCompletion { get; }

        /// <summary>
        /// Breaks the promise associated with this resolver.
        /// Note that this method may throw an <c>InvalidOperationException</c> if the promise has already
        /// been broken or has already been resolved.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        public abstract void Break(Exception exc);

        /// <summary>
        /// Breaks the promise associated with this resolver if the promise is not already broken or resolved.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        /// <returns><c>true</c> if the promise was successfully broken, or <c>false</c> if the promise was already
        /// broken or resolved.</returns>
        public abstract bool TryBreak(Exception exc);
    }

    /// <summary>
    /// The <c>AsyncCompletionResolver</c> class supports explicit resolver/promise pairs with void (data-less) promises.
    /// <para>
    /// This class provides a mechanism for developers to create promises whose resolution they
    /// control explicitly, rather than resolving when some request completes.
    /// There are relatively few occasions where this is necessary, but sometimes it is the only way to
    /// get the desired behavior.
    /// In general, though, if you think you need to use an explicit resolver/promise pair, you should think
    /// your scenario through carefully, because there's probably a better way to get the flow you want with
    /// a simple request-linked promise.
    /// </para>
    /// </summary>
    internal class AsyncCompletionResolver : AsyncResolver
    {
        private readonly TaskCompletionSource<bool> tcs;
        private AsyncCompletion ac;

        /// <summary>
        /// Creates a new resolver/promise pair.
        /// </summary>
        public AsyncCompletionResolver()
        {
            tcs = new TaskCompletionSource<bool>(AsyncCompletion.Context);
        }

        /// <summary>
        /// The promise associated with this resolver.
        /// </summary>
        public override AsyncCompletion AsyncCompletion { get
            {
                if (ac == null)
                {
                    lock (this)
                    {
                        if (ac == null)
                        {
                            OrleansTask ot = new OrleansTask(tcs.Task);
                            ac = new AsyncCompletion(ot);
#if TRACKING
                            // TODO: Should not auto-observe this promise - it is the caller's responbsibility to do that.
                            ac.Observe();
#endif
                        }
                    }
                }
                return ac;
        } }

        /// <summary>
        /// Successfully resolves the promise associated with this resolver.
        /// Note that this method may throw an <c>InvalidOperationException</c> if the promise has already
        /// been broken or has already been resolved.
        /// </summary>
        public void Resolve() { tcs.SetResult(true); }

        /// <summary>
        /// Successfully resolves the promise associated with this resolver if the promise has not already
        /// been broken or resolved.
        /// </summary>
        /// <returns><c>true</c> if the promise was successfully resolved, or <c>false</c> if the promise was already
        /// broken or resolved.</returns>
        public bool TryResolve() { return tcs.TrySetResult(true); }

        /// <summary>
        /// Breaks the promise associated with this resolver.
        /// Note that this method may throw an <c>InvalidOperationException</c> if the promise has already
        /// been broken or has already been resolved.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        public override void Break(Exception exc) { tcs.SetException(exc); }

        /// <summary>
        /// Breaks the promise associated with this resolver if the promise is not already broken or resolved.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        /// <returns><c>true</c> if the promise was successfully broken, or <c>false</c> if the promise was already
        /// broken or resolved.</returns>
        public override bool TryBreak(Exception exc) { return tcs.TrySetException(exc); }
    }

    /// <summary>
    /// An instance of the <c>AsyncValue</c> class represents a promise for a value of the given type.
    /// When the promise is resolved, the value becomes available and is provided to any scheduled
    /// continuation actions.
    /// </summary>
    /// <typeparam name="TResult">The type of the promised value.</typeparam>
    internal class AsyncValue<TResult> : AsyncCompletion
    {
        internal AsyncValue(OrleansTask<TResult> task) : base(task) { }

        /// <summary>
        /// Constructs an <c>AsyncValue</c> that is already resolved with the provided value.
        /// </summary>
        /// <param name="value">The value for the promise.</param>
        public AsyncValue(TResult value)
            : base(OrleansTask<TResult>.FromResult(value, AsyncCompletion.Context))
        { }

        /// <summary>
        /// Constructs an <c>AsyncValue</c> that is already broken.
        /// </summary>
        /// <param name="exc">An exception that describes the reason that the promise is broken.</param>
        public AsyncValue(Exception exc)
            : base(exc, OrleansTask<TResult>.FromException<TResult>(exc, AsyncCompletion.Context))
        { }

        /// <summary>
        /// Generate an <c>AsyncValue</c> from a given <c>Exception</c> with stack trace.
        /// This should be used by a code that wants to return an error to its caller with an embedded stack trace.
        /// </summary>
        /// <param name="exc">The <c>Exception</c> that describes why the promise was broken.</param>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static new AsyncValue<TResult> GenerateFromException(Exception exc)
        {
            try
            {
                throw exc;
            }
            catch (Exception exc2)
            {
                return new AsyncValue<TResult>(exc2);
            }
        }

        /// <summary>
        /// Queues the provided function to run asynchronously and returns a promise
        /// for the function's result.
        /// </summary>
        /// <param name="function">The function to be executed.</param>
        /// <returns>A promise for the function's result. 
        /// This promise is resolved to the function's return value when the function's execution is complete.
        /// This promise is broken if the function throws an exception.</returns>
        public static AsyncValue<TResult> StartNew(Func<TResult> function)
        {
            return StartNew(function, AsyncCompletion.Context);
        }

        /// <summary>
        /// Queues the provided function to run asynchronously and returns a promise
        /// for the function's result.
        /// </summary>
        /// <param name="function">The function to be executed.</param>
        /// <param name="context">The scheduling context under which the function should run.</param>
        /// <returns>A promise for the function's result. 
        /// This promise is resolved to the function's return value when the function's execution is complete.
        /// This promise is broken if the function throws an exception.</returns>
        internal static AsyncValue<TResult> StartNew(Func<TResult> function, ISchedulingContext context)
        {
            return new AsyncValue<TResult>(OrleansTask<TResult>.StartNew(function, context));
        }

        /// <summary>
        /// Queues the provided function to run asynchronously and returns a promise
        /// for the function's result.
        /// </summary>
        /// <param name="function">The function to be executed.</param>
        /// <returns>A promise for the function's result. 
        /// This promise is resolved when the function's return promise is resolved.
        /// This promise is broken if the function throws an exception.</returns>
        public static AsyncValue<TResult> StartNew(Func<AsyncValue<TResult>> function)
        {
            return StartNew(function, AsyncCompletion.Context);
        }

        /// <summary>
        /// Queues the provided function to run asynchronously and returns a promise
        /// for the function's result.
        /// </summary>
        /// <param name="function">The function to be executed.</param>
        /// <param name="context">The scheduling context under which the function should run.</param>
        /// <returns>A promise for the function's result. 
        /// This promise is resolved when the function's return promise is resolved.
        /// This promise is broken if the function throws an exception.</returns>
        internal static AsyncValue<TResult> StartNew(Func<AsyncValue<TResult>> function, ISchedulingContext context)
        {
            return new AsyncValue<TResult>(OrleansTask<TResult>.StartNew(function, context));
        }

        /// <summary>
        /// Execute a work item on a .NET thread pool thread, returning an AsyncValue promise that will be triggered when execution of the work item is completed.
        /// </summary>
        /// <param name="func">Function to be executed using .NET Thread Pool</param>
        /// <returns>An AsyncValue promise which will be triggered when execution of the work item is completed.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<T> ExecuteOnThreadPool<T>(Func<T> func)
        {
            AsyncValueResolver<T> ar = new AsyncValueResolver<T>();
            WaitCallback DoWork = data =>
            {
                try
                {
                    T retval = func(); // Do work
                    ar.Resolve(retval); // Resolve promise
                }
                catch (Exception exc)
                {
                    ar.Break(exc); // Break promise
                }
            };
            ThreadPool.QueueUserWorkItem(DoWork, null);
            return ar.AsyncValue;
        }

        /// <summary>
        /// Execute a work item on a .NET thread pool thread, returning an AsyncCompletion promise that will be triggered when execution of the work item is completed.
        /// </summary>
        /// <param name="action">Function to be executed using .NET Thread Pool</param>
        /// <param name="state">Any state data to be passed to the function when executing</param>
        /// <returns>An AsyncValue promise which will be triggered when execution of the work item is completed.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<T> ExecuteOnThreadPool<T>(Func<object, T> func, object state)
        {
            AsyncValueResolver<T> ar = new AsyncValueResolver<T>();
            WaitCallback DoWork = data =>
            {
                try
                {
                    T retval = func(data); // Do work
                    ar.Resolve(retval); // Resolve promise
                }
                catch (Exception exc)
                {
                    ar.Break(exc); // Break promise
                }
            };
            ThreadPool.QueueUserWorkItem(DoWork, state);
            return ar.AsyncValue;
        }

        /// <summary>
        /// Creates a promise from an existing IAsyncResult and an End method.
        /// </summary>
        /// <param name="asyncResult">The async result returned from a call to the Begin part of a Begin/End pair.
        /// The Begin method should have been called with a null async callback.</param>
        /// <param name="endDelegate">A delegate that calls the End part of a Begin/End pair.
        /// This delegate takes the IAsyncResult object returned from the Begin method and
        /// returns the actual result value.
        /// The end delegate is guaranteed to run in a separate new turn, never inline with the begin, 
        /// even if IAsyncResult is already resolved (unlike the TPL implementation, which may run it inline or not). </param>
        /// <returns>A promise for the result of the Begin/End pair.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<TResult> FromAsync(IAsyncResult asyncResult, Func<IAsyncResult, TResult> endDelegate)
        {
            var resolver = new AsyncValueResolver<TResult>();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(asyncResult, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        resolver.TryResolve(endDelegate(iar));
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context));
            return resolver.AsyncValue;
        }

        /// <summary>
        /// Creates a promise from a Begin/End method pair.
        /// </summary>
        /// <param name="beginDelegate">A delegate that calls the Begin part of a Begin/End pair.
        /// This delegate takes two parameters: an async callback delegate, to be passed to the Begin async method,
        /// and an object that allows a parameter to be passed to the delgate.
        /// This object may be used as the state object for the Begin method, but does not need to be; 
        /// its purpose is to allow the begin delegate to be reused.
        /// The delegate must return the IAsyncResult that is returned by the Begin method.</param>
        /// <param name="endDelegate">A delegate that calls the End part of a Begin/End pair.
        /// This delegate takes the IAsyncResult object returned from the Begin method and
        /// returns the actual result value.
        /// The end delegate is guaranteed to run in a separate new turn, never inline with the begin, 
        /// even if IAsyncResult is already resolved (unlike the TPL implementation, which may run it inline or not). </param>
        /// <param name="param">The object parameter to be passed to the begin delegate.</param>
        /// <returns>A promise for the result of the Begin/End pair.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<TResult> FromAsync(Func<AsyncCallback, object, IAsyncResult> beginDelegate, Func<IAsyncResult, TResult> endDelegate, object param)
        {
            var resolver = new AsyncValueResolver<TResult>();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        resolver.TryResolve(endDelegate(iar));
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), param);
            return resolver.AsyncValue;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<TResult> FromAsync<TArg1>(Func<TArg1, AsyncCallback, object, IAsyncResult> beginDelegate, Func<IAsyncResult, TResult> endDelegate,
            TArg1 arg1, object param)
        {
            var resolver = new AsyncValueResolver<TResult>();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        resolver.TryResolve(endDelegate(iar));
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, param);
            return resolver.AsyncValue;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<TResult> FromAsync<TArg1, TArg2>(Func<TArg1, TArg2, AsyncCallback, object, IAsyncResult> beginDelegate, Func<IAsyncResult, TResult> endDelegate,
            TArg1 arg1, TArg2 arg2, object param)
        {
            var resolver = new AsyncValueResolver<TResult>();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        resolver.TryResolve(endDelegate(iar));
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, arg2, param);
            return resolver.AsyncValue;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<TResult> FromAsync<TArg1, TArg2, TArg3>(Func<TArg1, TArg2, TArg3, AsyncCallback, object, IAsyncResult> beginDelegate,
            Func<IAsyncResult, TResult> endDelegate, TArg1 arg1, TArg2 arg2, TArg3 arg3, object param)
        {
            var resolver = new AsyncValueResolver<TResult>();
            var context = AsyncCompletion.Context;
            Task.Factory.FromAsync(beginDelegate, iar => OrleansTask.StartNew(() =>
                {
                    try
                    {
                        resolver.TryResolve(endDelegate(iar));
                    }
                    catch (Exception ex)
                    {
                        resolver.TryBreak(ex);
                    }
                }, context), arg1, arg2, arg3, param);
            return resolver.AsyncValue;
        }

        /// <summary>
        /// Waits forever for this promise to resolve and returns its value.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. 
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <returns>The resolved value of this promise.</returns>
        internal TResult GetValue()
        {
            if (!IsCompleted) Wait(Constants.INFINITE_TIMESPAN);
#if TRACKING
            else Observe();
#endif
            return TypedTask.Result;
        }

//        // without calling Wait.
//        internal TResult GetTypedResult()
//        {
//#if TRACKING
//            Observe();
//#endif

//            return TypedTask.Result;
//        }

        /// <summary>
        /// Convert this AsyncValue&lt;TResult&gt; to a corresponding Task&lt;TResult&gt; "/>
        /// </summary>
        /// <returns>Task-based async version of this AsyncValue</returns>
        public new Task<TResult> AsTask()
        {
            return OrleansTask.AsyncValueToTask(this);

            //var a = AsyncTaskMethodBuilder<TResult>.Create();
            //this.FastContinueWith(
            //    (TResult val) => a.SetResult(val),
            //    (Exception exc) => a.SetException(exc)
            //).Ignore();
            //return a.Task;
        }

        /// <summary>
        /// Convert this Task&lt;T&gt; into an AsyncValue&lt;T&gt;
        /// This function is primarily for use with .NET 4.0. With .NET 4.5 you can use the Task&lt;T&gt;.FromResult method to do the same thing.
        /// </summary>
        /// <returns>AsyncValue wrapper for this Task</returns>
        public static AsyncValue<TResult> FromTask(Task<TResult> task)
        {
            if (task == null) return new AsyncValue<TResult>(default(TResult));

            var resolver = new AsyncValueResolver<TResult>();

            if (task.Status == TaskStatus.RanToCompletion)
            {
                resolver.Resolve(task.Result);
            }
            else if (task.IsFaulted)
            {
                resolver.Break(task.Exception.Flatten());
            }
            else if (task.IsCanceled)
            {
                resolver.Break(new TaskCanceledException(task));
            }
            else
            {
                if (task.Status == TaskStatus.Created) task.Start();

                task.ContinueWith(t =>
                {
                    if (t.IsFaulted)
                    {
                        resolver.Break(t.Exception.Flatten());
                    }
                    else if (t.IsCanceled)
                    {
                        resolver.Break(new TaskCanceledException(t));
                    }
                    else
                    {
                        resolver.Resolve(t.Result);
                    }
                });
            }

            return resolver.AsyncValue;
        }

        /// <summary>
        /// Waits for the given amount of time for this promise to resolve and returns its value.
        /// If the promise does not resolve in time, a <c>TimeoutException</c> is thrown.
        /// <para>While waiting, the current method execution is blocked, but other turns for the activation
        /// may execute. 
        /// See the Programmer's Guide for more information on turns and the Orleans scheduling model.</para>
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="timeout">The length of time to wait for this promise to resolve.</param>
        /// <returns>The resolved value of this promise.</returns>
        internal TResult GetValue(TimeSpan timeout)
        {
            if (!IsCompleted) Wait(timeout);
#if TRACKING
            else Observe();
#endif
            return TypedTask.Result;
        }

        private OrleansTask<TResult> TypedTask { get { return (OrleansTask<TResult>)task; } }

        /// <summary>
        /// Schedules an action to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another action to be executed if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The action to run if and when this promise is successfully resolved.
        /// When invoked, the action receives the resolved value of this promise as a parameter.</param>
        /// <param name="exceptionAction">An optional action to run when this promise is broken. 
        /// This action gets passed the exception related to the breaking this promise.
        /// This action is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this action is not passed, then if this promise is broken, or if the 
        /// continuation action throws an exception, then the returned promise is broken.</param>
        /// <returns>A promise that is resolved when the continuation or exception action completes.
        /// If the exception action throws an exception (or, if there is no exception action and either the promise is broken or if the continuation action throws an exception), 
        /// then the returned promise is broken.</returns>
        public AsyncCompletion ContinueWith(Action<TResult> continuationAction, Action<Exception> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncCompletion(TypedTask.ContinueWithAction(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// Schedules an action to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another action to be executed if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The function to run if and when this promise is successfully resolved.
        /// When invoked, the action receives the resolved value of this promise as a parameter.
        /// The function must return an <c>AsyncCompletion</c>.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This function gets passed the exception related to the breaking this promise.
        /// This function is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception.
        /// This action must return an <c>AsyncCompletion</c>. If this function is not passed, then if this promise is broken, or if the 
        /// continuation function throws an exception, then the returned promise is broken.</param>
        /// <returns>A promise that is resolved when the promise returned by the continuation or exception action completes.
        /// If the exception action throws an exception (or, if there is no exception function and either the promise is broken or if the continuation function throws an exception), 
        /// then the returned promise is broken.</returns>
        public AsyncCompletion ContinueWith(Func<TResult, AsyncCompletion> continuationAction, Func<Exception, AsyncCompletion> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            return new AsyncCompletion(TypedTask.ContinueWithAsyncCompletion(continuationAction, exceptionAction, context));
        }

        /// <summary>
        /// Schedules a function to be executed (asynchronously) if and when this promise is resolved successfully
        /// and another function to be execute if and when this promise is broken.
        /// This method is useful as a way to specify a default result if this promise is broken.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The function to run if and when this promise is successfully resolved.
        /// When invoked, the action receives the resolved value of this promise as a parameter.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This action gets passed the exception related to the breaking this promise, and must
        /// return the same data type as the <c>continuationAction</c>.
        /// This action is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception. If this function is not passed, then if this promise is broken, or if the 
        /// continuation function throws an exception, then the returned promise is broken.</param>
        /// <returns>A promise for the result of the continuation or exception action.
        /// If the exception function throws an exception (or, if there is no exception function and either the promise is broken or if the continuation function throws an exception), 
        /// then the returned promise is broken.</returns>
        public AsyncValue<TNewResult> ContinueWith<TNewResult>(Func<TResult, TNewResult> continuationAction, Func<Exception, TNewResult> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            OrleansTask<TNewResult> ot = TypedTask.ContinueWithFunction(continuationAction, exceptionAction, context);
            return new AsyncValue<TNewResult>(ot);
        }

        /// <summary>
        /// Schedules an action to be executed (asynchronously) if and when this promise is resolved successfully.
        /// <para>This method "observes" the resolution of the promise.</para>
        /// </summary>
        /// <param name="continuationAction">The action to run when this promise is resolved.
        /// When invoked, the action receives the resolved value of this promise as a parameter.
        /// The action must return a promise for a value as its result.</param>
        /// <param name="exceptionAction">An optional function to run when this promise is broken. 
        /// This function gets passed the exception related to the breaking this promise.
        /// This function is also executed if this promise completes successfully and the <c>continuationAction</c>
        /// throws an exception.
        /// This action must return the same data type as the <c>continuationAction</c>. If this function is not passed, then if this promise is broken, or if the 
        /// continuation function throws an exception, then the returned promise is broken.</param>
        /// <returns>A promise that is resolved when the promise returned by the action is resolved, with the
        /// same value as that promise.
        /// If this promise (the one that ContinueWith is invoked on) is broken, or if the action throws an exception,
        /// or if the action's returned promise is broken, then the returned promise is broken.</returns>
        public AsyncValue<TNewResult> ContinueWith<TNewResult>(Func<TResult, AsyncValue<TNewResult>> continuationAction, Func<Exception, AsyncValue<TNewResult>> exceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            var context = AsyncCompletion.Context;
            OrleansTask<TNewResult> ot = TypedTask.ContinueWithFunction(continuationAction, exceptionAction, context);
            return new AsyncValue<TNewResult>(ot);
        }

        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        internal AsyncValue<TNewResult> SystemContinueWith<TNewResult>(Func<TResult, TNewResult> continuationAction, Func<Exception, TNewResult> pExceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            return new AsyncValue<TNewResult>(TypedTask.ContinueWithFunction(continuationAction, pExceptionAction, null)); // system continuation runs outside the context.
        }

        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        internal AsyncValue<TNewResult> SystemContinueWith<TNewResult>(Func<TResult, AsyncValue<TNewResult>> continuationAction, Func<Exception, AsyncValue<TNewResult>> pExceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            return new AsyncValue<TNewResult>(TypedTask.ContinueWithFunction(continuationAction, pExceptionAction, null)); // system continuation runs outside the context.
        }

        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        internal AsyncCompletion SystemContinueWith(Action<TResult> continuationAction, Action<Exception> pExceptionAction = null)
        {
#if TRACKING
            Observe();
#endif
            return new AsyncCompletion(task.ContinueWithAction(continuationAction, pExceptionAction, null)); // system continuation runs outside the context.
        }

        /// <summary>
        /// Joins multiple promises into a single combined promise.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="acs">A collection that holds the promises to join.</param>
        /// <returns>A new promise that resolves when all of the joining promises are resolved.
        /// The new promise is broken if one or more of the combining promises break.</returns>
        public static AsyncValue<TResult[]> JoinAll(IEnumerable<AsyncValue<TResult>> acs)
        {
            if (acs == null)
                throw new ArgumentNullException("acs");

            return JoinAll(acs.ToArray());
        }

        /// <summary>
        /// Joins multiple promises into a single combined promise.
        /// <para>This method "observes" the resolution of the joining promises.</para>
        /// </summary>
        /// <param name="acs">An array that holds the promises to join.</param>
        /// <returns>A new promise that resolves when all of the joining promises are resolved.
        /// The new promise is broken if one or more of the combining promises break.</returns>
        public static AsyncValue<TResult[]> JoinAll(AsyncValue<TResult>[] acs)
        {
#if TRACKING
            foreach (var ac in acs)
            {
                ac.Observe();
            }

#endif
            if (acs == null)
                throw new ArgumentNullException("acs");
            for (int i = 0; i < acs.Length; i++)
            {
                if (acs[i] == null)
                    throw new ArgumentNullException(String.Format("acs[{0}]", i));
            }
            if (acs.Length == 0)
            {
                return new TResult[0];
            }
            var context = AsyncCompletion.Context;
            return new AsyncValue<TResult[]>(OrleansTask.JoinAll(acs, context));
        }


        /// <summary>
        /// Casts a prompt value to a resolved promise for that value.
        /// This implicit cast is very useful for the return value of asynchronous methods; it allows the code to read
        /// <c>return 5;</c> instead of <c>return new AsyncValue<int>(5);</int></c>.
        /// </summary>
        /// <param name="value">The resolved value of the new promise.</param>
        /// <returns>A pre-resolved promise for the given value.</returns>
        public static implicit operator AsyncValue<TResult>(TResult value)
        {
            return new AsyncValue<TResult>(value);
        }

        internal override object GetObjectValue() { return GetValue(); }
    }

    /// <summary>
    /// The <c>AsyncValueResolver</c> class supports explicit resolver/promise pairs for data-carrying promises.
    /// <para>
    /// This class provides a mechanism for developers to create promises whose resolution they
    /// control explicitly, rather than resolving when some request completes.
    /// There are relatively few occasions where this is necessary, but sometimes it is the only way to
    /// get the desired behavior.
    /// In general, though, if you think you need to use an explicit resolver/promise pair, you should think
    /// your scenario through carefully, because there's probably a better way to get the flow you want with
    /// a simple request-linked promise.
    /// </para>
    /// </summary>
    /// <typeparam name="TResult">The type of the promised value.</typeparam>
    internal class AsyncValueResolver<TResult> : AsyncResolver
    {
        internal readonly TaskCompletionSource<TResult> tcs;
        protected AsyncValue<TResult> av;

        /// <summary>
        /// Constructs a new, unresolved resolver/promise pair.
        /// </summary>
        public AsyncValueResolver()
        {
            tcs = new TaskCompletionSource<TResult>(AsyncCompletion.Context);
        }

        /// <summary>
        /// The promise associated with this resolver.
        /// </summary>
        public AsyncValue<TResult> AsyncValue { get
            {
                if (av == null)
                {
                    lock (this)
                    {
                        if (av == null)
                        {
                            av = AllocateAsyncValue();
#if TRACKING
                            // TODO: Should not auto-observe this promise - it is the caller's responsibility to do that.
                            av.Observe();
#endif
                        }
                    }
                }
                return av;
        } }

        protected virtual AsyncValue<TResult> AllocateAsyncValue()
        {
            OrleansTask<TResult> ot = new OrleansTask<TResult>(tcs.Task);
            return new AsyncValue<TResult>(ot);
        }

        /// <summary>
        /// A "void" version of the promise associated with this resolver.
        /// </summary>
        public override AsyncCompletion AsyncCompletion { get { return AsyncValue; } }

        /// <summary>
        /// Resolves the associated promise to the provided value.
        /// This method will throw an <c>InvalidOperationException</c> if the promise has already been resolved or broken.
        /// </summary>
        /// <param name="value">The value to resolve the promise to.</param>
        public void Resolve(TResult value) { tcs.SetResult(value); }

        /// <summary>
        /// Tries to resolve the associated promise to the provided value.
        /// </summary>
        /// <param name="value">The value to resolve the promise to.</param>
        /// <returns><c>true</c> if the promise was successfully resolved, or <c>false</c> if the promise has already been
        /// resolved or broken.</returns>
        public bool TryResolve(TResult value) { return tcs.TrySetResult(value); }

        /// <summary>
        /// Breaks the associated promise.
        /// Note that this method may throw an <c>InvalidOperationException</c> if the promise has already
        /// been broken or has already been resolved.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        public override void Break(Exception exc) { tcs.SetException(exc); }

        /// <summary>
        /// Tries to break the associated promise.
        /// </summary>
        /// <param name="exc">An exception that describes the reason for breaking the promise.</param>
        /// <returns><c>true</c> if the promise was successfully resolved, or <c>false</c> if the promise has already been
        /// resolved or broken.</returns>
        public override bool TryBreak(Exception exc) { return tcs.TrySetException(exc); }
    }

    internal static class AsyncValue
    {
        public static AsyncValue<T> FromTask<T>(Task<T> task)
        {
            return AsyncValue<T>.FromTask(task);
        }
    }
}
