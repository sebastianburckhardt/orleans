//#define VALIDATE
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace Orleans
{
    internal class OrleansTask
    {
        protected Task m_Task { get; private set; }
        protected static AbstractOrleansTaskScheduler meta_scheduler;
        private static readonly Logger logger = Logger.GetLogger("OrleansTask", Logger.LoggerType.Runtime);

        internal static void Initialize(AbstractOrleansTaskScheduler scheduler)
        {
            meta_scheduler = scheduler;
        }

        internal static void Reset()
        {
            meta_scheduler = null;
        }

        public static TaskFactory GetFactory(ISchedulingContext context)
        {
            // On the silo the meta_scheduler is always set.
            // On the client it is null, so we use the default Task.Factory (whcih queues to ThreadPool)
            if (meta_scheduler != null)
            {
                TaskScheduler scheduler = meta_scheduler.GetTaskScheduler(context);
                return new TaskFactory(scheduler);
            }
            else
            {
                return Task.Factory;
            }
        }

        internal OrleansTask(Task pTask)
        {
            m_Task = pTask;
        }

        internal static OrleansTask StartNew(Action action, ISchedulingContext context)
        {
            return OrleansTask<bool>.HelperStartNew((object obj) =>
                                                        {
                                                            action();
                                                            return true;
                                                        }, context);
        }

        internal static OrleansTask StartNew(Func<AsyncCompletion> function, ISchedulingContext context)
        {
            return OrleansTask<bool>.HelperStartNew((object obj) =>
                                                        {
                                                            return function().ContinueWith(() => true);
                                                        }, context);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Task is IDisposable but cannot call Dispose yet as we need to return it to caller.")]
        private static Task StartNewTPLTask(Action<object> action, ISchedulingContext context)
        {
            // we create new tasks in a Detached form, meaning parent can finish BEFORE its children are isFinished.
            // errors will not be propagated from child to parent
            // On the silo the meta_scheduler is always set.
            // On the client it is null, so we use the default Task.Factory (whcih queues to ThreadPool)
            Task task = new Task(action, context);
            if (meta_scheduler != null)
            {
                TaskScheduler scheduler = meta_scheduler.GetTaskScheduler(context);
                task.Start(scheduler);
            }
            else
            {
                task.Start();
            }
            return task;
        }

        internal static Task[] AsyncCompletionsToTasks(AsyncCompletion[] acs)
        {
            if (acs == null) throw new ArgumentException();
            Task[] tasks = new Task[acs.Length];
            for (int i = 0; i < acs.Length; i++)
            {
                tasks[i] = acs[i].task.m_Task;
            }
            return tasks;
        }

        [DebuggerHidden]
        internal static Task AsyncCompletionToTask(AsyncCompletion ac)
        {
            if (ac == null) throw new ArgumentException();
            ac.Ignore();
            Task t = ac.task.m_Task;

            return t.ContinueWith(task =>
            {
                if (task.IsFaulted) throw task.Exception.Flatten();
                if (task.IsCanceled) throw new TaskCanceledException(task);
            });
        }

        [DebuggerHidden]
        internal static Task<TResult> AsyncValueToTask<TResult>(AsyncValue<TResult> av)
        {
            if (av == null) throw new ArgumentException();
            av.Ignore();
            Task<TResult> t = (Task<TResult>) av.task.m_Task;

            return t.ContinueWith(task =>
            {
                if (task.IsFaulted) throw task.Exception.Flatten();
                if (task.IsCanceled) throw new TaskCanceledException(task);
                return task.Result;
            });
        }

        // ContinueWith methods
        internal OrleansTask ContinueWithAction<T>(Action<T> pContinuationAction, Action<Exception> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithAction(pContinuationAction, pExceptionAction, context);
        }

        internal OrleansTask ContinueWithAction(Action pContinuationAction, Action<Exception> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithAction<object>((object o) => pContinuationAction(), pExceptionAction, context);
        }

        internal OrleansTask ContinueWithAsyncCompletion(Func<AsyncCompletion> pContinuationAction, Func<Exception, AsyncCompletion> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithAsyncCompletion((object o) => pContinuationAction(), pExceptionAction, context);
        }

        internal OrleansTask<TNewResult> ContinueWithFunction<TNewResult>(Func<TNewResult> pContinuationAction, Func<Exception, TNewResult> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithFunction((object o) => pContinuationAction(), null, true, pExceptionAction, null, context);
        }

        internal OrleansTask<TNewResult> ContinueWithFunction<TNewResult>(Func<AsyncValue<TNewResult>> pContinuationAction, Func<Exception, AsyncValue<TNewResult>> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithFunction(null, (object o) => pContinuationAction(), false, null, pExceptionAction, context);
        }

#if VALIDATE
        private static SafeRandom _Random = new SafeRandom();
        private static List<int> _Ids = new List<int>();
        protected static int BeginMatch()
        {
            var id = _Random.Next();
            _Ids.Add(id);
            logger.Verbose("StartContinueWith {0} from {1}", id, new StackTrace());
            return id;
        }
        protected static void EndMatch(int id)
        {
            _Ids.Remove(id);
            logger.Verbose("EndContinueWith {0}:{1}", id, Utils.IEnumerableToString(_Ids.ToList(), i => i.ToString()));
        }
#endif
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected OrleansTask HelperContinueWithAction<T>(Action<T> pContinuationAction, Action<Exception> pExceptionAction, ISchedulingContext context)
        {
#if VALIDATE
            var id = BeginMatch();
#endif
            TaskCompletionSource<object> resolver = new TaskCompletionSource<object>(context);
            m_Task.ContinueWith((Task t) =>
            {
                // We run the continuation task inside a new TPL task since we need to pass the context to the TPL task at its creation time,
                // so that it is accessible via AsyncState. ContinueWith does not allow to do that directly.
                StartNewTPLTask((object obj) =>
                    {
                        try
                        {
                            if (t.Status == TaskStatus.Faulted)
                            {
                                // Break the Continuation task
                                BreakResolver_Action<T>(resolver, pExceptionAction, t.Exception.Flatten());
                                return;
                            }
                            else
                            {
                                T res;
                                if (t is Task<T>)
                                {
                                    res = ((Task<T>)t).Result;
                                }
                                else
                                {
                                    res = default(T);
                                }
                                try
                                {
                                    pContinuationAction(res);
                                    resolver.TrySetResult(default(T));
                                }
                                catch (Exception exc)
                                {
                                    BreakResolver_Action<T>(resolver, pExceptionAction, exc);
                                }
                            }
                        }
                        finally
                        {
#if VALIDATE
                            EndMatch(id);
#endif
                        }
                    }, context);
            });
            return new OrleansTask(resolver.Task);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected OrleansTask HelperContinueWithAsyncCompletion<T>(Func<T, AsyncCompletion> pContinuationFunction, Func<Exception, AsyncCompletion> pExceptionAction, ISchedulingContext context)
        {
#if VALIDATE
            var id = BeginMatch();
#endif
            TaskCompletionSource<object> resolver = new TaskCompletionSource<object>(context);
            m_Task.ContinueWith((Task t1) =>
            {
                StartNewTPLTask((object obj) =>
                    {
                        try
                        {
                            if (t1.Status == TaskStatus.Faulted)
                            {
                                BreakResolver_AsyncCompletion<T>(resolver, pExceptionAction, t1.Exception.Flatten());
                                return;
                            }
                            else
                            {
                                T taskResult;
                                if (t1 is Task<T>)
                                {
                                    taskResult = ((Task<T>)t1).Result;
                                }
                                else
                                {
                                    taskResult = default(T);
                                }
                                try
                                {
                                    AsyncCompletion contResultPromise = pContinuationFunction(taskResult);
                                    if (contResultPromise != null)
                                    {
                                        Task contCompletion = contResultPromise.task.m_Task.ContinueWith(
                                            (Task t2) =>
                                            {
                                                contResultPromise.Ignore();
                                                if (t2.Status == TaskStatus.Faulted)
                                                {
                                                    BreakResolver_AsyncCompletion<T>(resolver, pExceptionAction, t2.Exception.Flatten());
                                                }
                                                else
                                                {
                                                    T taskResult2;
                                                    if (t2 is Task<T>)
                                                    {
                                                        taskResult2 = ((Task<T>)t2).Result;
                                                    }
                                                    else
                                                    {
                                                        taskResult2 = default(T);
                                                    }
                                                    resolver.TrySetResult(taskResult2);
                                                }
                                            });
                                    }
                                    else
                                    {
                                        BreakResolver_AsyncCompletion<T>(resolver, pExceptionAction,
                                            new NullReferenceException("Null promise returned from success branch of ContinueWith; do you need to cast null to the result type?"));
                                    }
                                }
                                catch (Exception exc3)
                                {
                                    BreakResolver_AsyncCompletion<T>(resolver, pExceptionAction, exc3);
                                }
                            }
                        }
                        finally
                        {
#if VALIDATE
                            EndMatch(id);
#endif
                        }
                    }, context);
            });
            return new OrleansTask(resolver.Task);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        protected OrleansTask<TNewResult> HelperContinueWithFunction<T, TNewResult>(
                                                                                Func<T, TNewResult> pContinuationFunction1,
                                                                                Func<T, AsyncValue<TNewResult>> pContinuationFunction2,
                                                                                bool useFunc1,
                                                                                Func<Exception, TNewResult> pExceptionAction1,
                                                                                Func<Exception, AsyncValue<TNewResult>> pExceptionAction2,
                                                                                ISchedulingContext context)
        {
#if VALIDATE
            var id = BeginMatch();
#endif
            TaskCompletionSource<TNewResult> resolver = new TaskCompletionSource<TNewResult>(context);
            m_Task.ContinueWith((Task t1) =>
            {
                StartNewTPLTask((object obj) =>
                    {
                        try
                        {
                            if (t1.Status == TaskStatus.Faulted)
                            {
                                BreakResolver_Function(resolver, pExceptionAction1, pExceptionAction2, useFunc1, t1.Exception.Flatten());
                                return;
                            }
                            else
                            {
                                T taskResult;
                                if (t1 is Task<T>)
                                {
                                    taskResult = ((Task<T>)t1).Result;
                                }
                                else
                                {
                                    taskResult = default(T);
                                }

                                try
                                {
                                    if (useFunc1)
                                    {
                                        TNewResult result = pContinuationFunction1(taskResult);
                                        resolver.TrySetResult(result);
                                    }
                                    else
                                    {
                                        // the pContinuationFunction itself returned AsyncValue so we need to wait for this promise to be resolved first
                                        AsyncValue<TNewResult> contResultPromise = pContinuationFunction2(taskResult);
                                        // Make sure that the returned promise isn't null
                                        if (contResultPromise != null)
                                        {
                                            Task contCompletion = contResultPromise.task.m_Task.ContinueWith(
                                                (Task t2) =>
                                                {
                                                    contResultPromise.Ignore();
                                                    if (t2.Status == TaskStatus.Faulted)
                                                    {
                                                        BreakResolver_Function(resolver, pExceptionAction1, pExceptionAction2, useFunc1, t2.Exception.Flatten());
                                                    }
                                                    else
                                                    {
                                                        resolver.TrySetResult(((Task<TNewResult>)t2).Result);
                                                    }
                                                });
                                        }
                                        else
                                        {
                                            BreakResolver_Function(resolver, pExceptionAction1, pExceptionAction2, useFunc1, GetNullPromiseException("success"));
                                        }
                                    }
                                }
                                catch (Exception exc3)
                                {
                                    BreakResolver_Function(resolver, pExceptionAction1, pExceptionAction2, useFunc1, exc3);
                                }
                            }
                        }
                        finally
                        {
#if VALIDATE
                            EndMatch(id);
#endif
                        }
                    }, context);
            });
            return new OrleansTask<TNewResult>(resolver.Task);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void BreakResolver_Action<T>(TaskCompletionSource<object> resolver, Action<Exception> pExceptionAction, Exception exception)
        {
            if (pExceptionAction == null)
            {
                resolver.TrySetException(exception);
                return;
            }
            try
            {
                pExceptionAction(exception);
                resolver.TrySetResult(default(T));
            }
            catch (Exception exc1)
            {
                resolver.TrySetException(exc1);
            }
        }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void BreakResolver_AsyncCompletion<T>(TaskCompletionSource<object> resolver, Func<Exception, AsyncCompletion> pExceptionAction, Exception exception)
        {
            if (pExceptionAction == null)
            {
                resolver.TrySetException(exception);
                return;
            }
            try
            {
                AsyncCompletion excPromise = pExceptionAction(exception);
                Task contCompletion = excPromise.task.m_Task.ContinueWith(
                    (Task t1) =>
                    {
                        excPromise.Ignore();
                        if (t1.Status == TaskStatus.Faulted)
                        {
                            resolver.TrySetException(t1.Exception.Flatten());
                        }
                        else
                        {
                            T taskResult;
                            if (t1 is Task<T>)
                            {
                                taskResult = ((Task<T>)t1).Result;
                            }
                            else
                            {
                                taskResult = default(T);
                            }
                            resolver.TrySetResult(taskResult);
                        }
                    });
            }
            catch (Exception exc2)
            {
                resolver.TrySetException(exc2);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void BreakResolver_Function<TNewResult>(TaskCompletionSource<TNewResult> resolver, Func<Exception, TNewResult> pExceptionAction1,
            Func<Exception, AsyncValue<TNewResult>> pExceptionAction2, bool useFunc1, Exception exception)
        {
            if (useFunc1)
            {
                if (pExceptionAction1 == null)
                {
                    resolver.TrySetException(exception);
                    return;
                }
                try
                {
                    TNewResult excResult = pExceptionAction1(exception);
                    resolver.TrySetResult(excResult);
                }
                catch (Exception exc1)
                {
                    resolver.TrySetException(exc1);
                }
            }
            else
            {
                if (pExceptionAction2 == null)
                {
                    resolver.TrySetException(exception);
                    return;
                }
                try
                {
                    AsyncValue<TNewResult> excPromise = pExceptionAction2(exception);
                    if (excPromise != null)
                    {
                        Task contCompletion = excPromise.task.m_Task.ContinueWith(
                            (Task t1) =>
                            {
                                excPromise.Ignore();
                                if (t1.Status == TaskStatus.Faulted)
                                {
                                    resolver.TrySetException(t1.Exception.Flatten());
                                }
                                else
                                {
                                    TNewResult taskResult1;
                                    if (t1 is Task<TNewResult>)
                                    {
                                        taskResult1 = ((Task<TNewResult>)t1).Result;
                                    }
                                    else
                                    {
                                        taskResult1 = default(TNewResult);
                                    }
                                    resolver.TrySetResult(taskResult1);
                                }
                            });
                    }
                    else
                    {
                        resolver.TrySetException(GetNullPromiseException("failure"));
                    }
                }
                catch (Exception exc3)
                {
                    resolver.TrySetException(exc3);
                }
            }
        }

        private static NullReferenceException GetNullPromiseException(string branch)
        {
            return new NullReferenceException("Null promise returned from " + branch + 
                " branch of ContinueWith; perhaps you need to return 'new AsyncValue(null)' instead of null?");
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static OrleansTask JoinAll(AsyncCompletion[] acs, ISchedulingContext context)
        {
            TaskCompletionSource<bool> resolver = new TaskCompletionSource<bool>(context);
            Task[] tasks = OrleansTask.AsyncCompletionsToTasks(acs);

            OrleansTask.GetFactory(context).
                ContinueWhenAll(tasks,
                    (Task[] done) =>
                    {
                        try
                        {
                            List<Exception> failures = null;
                            foreach (Task task in done)
                            {
                                if (task.Status == TaskStatus.RanToCompletion)
                                {
                                    // OK
                                }
                                else if (task.Status == TaskStatus.Faulted)
                                {
                                    failures = failures ?? new List<Exception>();
                                    failures.Add(task.Exception.Flatten());
                                }
                                else
                                {
                                    failures = failures ?? new List<Exception>();
                                    failures.Add(new TaskCanceledException("Task finished in bad state: " + task.Status));
                                }
                            }
                            if (failures != null)
                            {
                                resolver.TrySetException(new AggregateException(failures));
                            }
                            else
                            {
                                resolver.TrySetResult(true);
                            }
                        }
                        catch (Exception exc)
                        {
                            resolver.TrySetException(exc);
                        }
                    });

            return new OrleansTask(resolver.Task);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static OrleansTask<TResult[]> JoinAll<TResult>(AsyncValue<TResult>[] avs, ISchedulingContext context)
        {
            TaskCompletionSource<TResult[]> resolver = new TaskCompletionSource<TResult[]>(context);
            Task[] tasks = OrleansTask.AsyncCompletionsToTasks(avs);

            OrleansTask.GetFactory(context).
                ContinueWhenAll(tasks,
                    (Task[] done) =>
                    {
                        try
                        {
                            List<Exception> failures = null;
                            List<TResult> results = null;
                            foreach (Task<TResult> task in done)
                            {
                                if (task.Status == TaskStatus.RanToCompletion)
                                {
                                    results = results ?? new List<TResult>();
                                    results.Add(task.Result);
                                }
                                else if (task.Status == TaskStatus.Faulted)
                                {
                                    failures = failures ?? new List<Exception>();
                                    failures.Add(task.Exception.Flatten());
                                }
                                else
                                {
                                    failures = failures ?? new List<Exception>();
                                    failures.Add(new TaskCanceledException("Task finished in bad state: " + task.Status));
                                }
                            }
                            if (failures != null)
                            {
                                resolver.TrySetException(new AggregateException(failures));
                            }
                            else
                            {
                                resolver.TrySetResult(results.ToArray<TResult>());
                            }
                        }
                        catch (Exception exc)
                        {
                            resolver.TrySetException(exc);
                        }
                    });
            return new OrleansTask<TResult[]>(resolver.Task);
        }

        internal void Ignore(Logger log)
        {
            m_Task.ContinueWith((Task c) =>
                {
                    Exception exc = c.Exception;
                    if ((log != null) && (c.Status == TaskStatus.Faulted) && log.IsVerbose)
                    {
                        log.Verbose("OrleansTask:Ignore() - explicitly ignoring a broken Task {0}. Exception={1}",
                                    OrleansTaskExtentions.ToString(c), Logger.PrintException(exc));
                    }
                },
                TaskContinuationOptions.OnlyOnFaulted |
                TaskContinuationOptions.ExecuteSynchronously);
        }

        internal void Wait()
        {
            Wait(Constants.INFINITE_TIMESPAN);
        }

        internal bool Wait(TimeSpan timeout)
        {
            TaskStatus status = m_Task.Status;

            // Warn about synchronous Wait calls blocking server-side execution thread(s)
            if (GrainClient.Current != null && !(GrainClient.Current is OutsideGrainClient))
            {
                // This is a server-side Wait call
                
                if (GrainClient.Current.CurrentGrain != null)
                {
                    // Being called from user code
                    // It is OK to call Wait inside grain code on an already resolved promise.

                    if (status == TaskStatus.RanToCompletion || status == TaskStatus.Faulted || status == TaskStatus.Canceled)
                    {
                        return m_Task.Wait(timeout);
                    }

                    // Being called from user code on Unresolved promise - Log warning, and optionally throw if Logger.ThrowOnGrainWait==true
                    const string msgWaitCalledInsideGrain =
                        "Wait called from inside an Orleans Grain will block server execution thread and will reduce system efficiency";
                    var st = new StackTrace();
                    logger.Warn(ErrorCode.WaitCalledInsideGrain, "{0} -- Backtrace: {1}", msgWaitCalledInsideGrain, st);

                        throw new InvalidOperationException(msgWaitCalledInsideGrain);
                    }
                else if (SystemStatus.Current == SystemStatus.Running)
                {
                    // Being called from system code in the Running phase (not silo startup / shutdown) - log Error and always throw

                    const string msgWaitCalledBlockingThread = "Wait call will block server execution thread and will reduce system efficiency";
                    var st = new StackTrace();
                    logger.Error(ErrorCode.WaitCalledInServerCode, string.Format(
                        "{0} Task.Id={1} -- Backtrace: {2}", msgWaitCalledBlockingThread, Task.CurrentId, st));

                    throw new InvalidOperationException(msgWaitCalledBlockingThread);
                }
                // else, we ignore any Wait calls during silo startup / shutdown
            }

            // an optimization for fast path. Do not tell the scheduler to wait for an already completed task.
            // still needs to wait on the Task to fail with exception if the task is TaskStatus.Faulted.
            if (status == TaskStatus.RanToCompletion || status == TaskStatus.Faulted || status == TaskStatus.Canceled)
            {
                return m_Task.Wait(timeout);
            }

            try
            {
                return m_Task.Wait(timeout);
            }
            catch (AggregateException e)
            {
                // [mlr] AggregateExceptions thrown by a task should be caught and flattened before being rethrown.
                // see <http://msdn.microsoft.com/en-us/library/dd537614.aspx>.
                throw e.Flatten();
            }
        }

        internal AsyncCompletionStatus Status
        {
            get
            {
                switch (m_Task.Status)
                {
                    case TaskStatus.Created: return AsyncCompletionStatus.Running;
                    case TaskStatus.WaitingForActivation: return AsyncCompletionStatus.Running;
                    case TaskStatus.WaitingToRun: return AsyncCompletionStatus.Running;
                    case TaskStatus.Running: return AsyncCompletionStatus.Running;

                    case TaskStatus.WaitingForChildrenToComplete:
                        string msg = "Unexpected Task state -= WaitingForChildrenToComplete";
                        logger.Assert(ErrorCode.Runtime_Error_100127, false, msg + " - We should not see this state, since we always create detached children");
                        throw new OrleansException(msg);
                    //return AsyncCompletionStatus.Running;

                    case TaskStatus.RanToCompletion: return AsyncCompletionStatus.CompletedSuccessfully;
                    case TaskStatus.Faulted: return AsyncCompletionStatus.Faulted;
                    // we still do not supper TTLs and cancelations
                    //case TaskStatus.Canceled: return AsyncCompletionStatus.Canceled;
                    //case TaskStatus.TimedOut: return AsyncCompletionStatus.TimedOut;
                    default:
                        msg = "Unexpected Task state - not supported TPL task state: " + m_Task.Status;
                        logger.Assert(ErrorCode.Runtime_Error_100128, false, msg);
                        throw new OrleansException(msg);
                    //return AsyncCompletionStatus.Faulted;
                }
            }
        }

        internal bool IsCompleted { get { return m_Task.IsCompleted; } }
        internal bool IsFaulted { get { return m_Task.IsFaulted; } }
        internal Exception Exception
        {
            get { return m_Task.Exception == null ? null : m_Task.Exception.Flatten(); }
        }

        internal static OrleansTask FromException(Exception exception, ISchedulingContext state)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(state);
            tcs.TrySetException(exception);
            return new OrleansTask(tcs.Task);
        }

        internal static OrleansTask Done(ISchedulingContext state)
        {
            TaskCompletionSource<bool> tcs = new TaskCompletionSource<bool>(state);
            tcs.TrySetResult(true);
            return new OrleansTask<bool>(tcs.Task);
        }
    }

    internal class OrleansTask<TResult> : OrleansTask
    {
        internal OrleansTask(Task<TResult> pTask) : base(pTask) { }

        internal static OrleansTask<TResult> StartNew(Func<TResult> function, ISchedulingContext context)
        {
            return OrleansTask<TResult>.HelperStartNew((object obj) => { return function(); }, context);
        }

        internal static OrleansTask<TResult> StartNew(Func<AsyncValue<TResult>> function, ISchedulingContext context)
        {
            return OrleansTask<TResult>.HelperStartNew((object obj) => { return function(); }, context);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope",
            Justification = "Task is IDisposable but cannot call Dispose yet as we need to return it to caller.")]
        // DO NOT create TypedFactory = new TaskFactory<TResult>(Factory.Scheduler)
        // since we can't reset it later. Just use scheduler inside the base OrleansTask class.
        private static Task<TResult> StartNewTPLTask(Func<object, TResult> function, ISchedulingContext context)
        {
            // On the silo the meta_scheduler is always set.
            // On the client it is null, so we use the default Task.Factory (which queues to ThreadPool)
            Task<TResult> task = new Task<TResult>(function, context);
            if (meta_scheduler != null)
            {
                TaskScheduler scheduler = meta_scheduler.GetTaskScheduler(context);
                task.Start(scheduler);
            }
            else
            {
                task.Start();
            }
            return task;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static OrleansTask<TResult> HelperStartNew(Func<object, TResult> function, ISchedulingContext context)
        {
#if VALIDATE
            var id = BeginMatch();
#endif
            TaskCompletionSource<TResult> resolver = new TaskCompletionSource<TResult>(context);
            OrleansTask<TResult>.StartNewTPLTask(obj =>
                {
                    TResult result = default(TResult);
                    try
                    {
                        result = function(obj);
                        resolver.TrySetResult(result);
                    }
                    catch (Exception exc)
                    {
                        resolver.TrySetException(exc);
                    }
                    finally
                    {
#if VALIDATE
                        EndMatch(id);
#endif
                    }
                    return result;
                },
                context);
            return new OrleansTask<TResult>(resolver.Task);
        }

        internal static OrleansTask<TResult> HelperStartNew(Func<object, AsyncValue<TResult>> function, ISchedulingContext context)
        {
#if VALIDATE
            var id = BeginMatch();
#endif
            TaskCompletionSource<TResult> resolver = new TaskCompletionSource<TResult>(context);
            Task<AsyncValue<TResult>> task = OrleansTask<AsyncValue<TResult>>.StartNewTPLTask(obj =>
            {
                try
                {
                    return function(obj);
                }
                finally
                {
#if VALIDATE
                    EndMatch(id);
#endif
                }
            }, context);

            task.ContinueWith((Task<AsyncValue<TResult>> t1) =>
                {
                    try
                    {
                        if (t1.Status == TaskStatus.Faulted)
                        {
                            resolver.TrySetException(t1.Exception.Flatten());
                        }
                        else
                        {
                            t1.Result.ContinueWith((TResult res) =>
                            {
                                resolver.TrySetResult(res);
                            }, (Exception exc) =>
                            {
                                resolver.TrySetException(exc);
                            }).Ignore();
                        }
                    }
                    finally
                    {
#if VALIDATE
                    EndMatch(id);
#endif
                    }
                });
            return new OrleansTask<TResult>(resolver.Task);
        }

        internal TResult Result { get { return ((Task<TResult>)m_Task).Result; } }

        // ContinueWith methods

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1061:DoNotHideBaseClassMethods",
            Justification = "Gabi: I prefer not  to remove this method now, as it gives more structured API of the Orleans Task (which is any way internal class, not visible by Orleans users). Both methods call the same implementation method, so no problems.")]
        // CA1061 : Microsoft.Design : Change or remove 'OrleansTask<TResult>.ContinueWithAction(Action<TResult>, Action<Exception>, object)' because it hides a more specific base class method: 'OrleansTask.ContinueWithAction<T>(Action<T>, Action<Exception>, object)'.
        internal OrleansTask ContinueWithAction(Action<TResult> pContinuationAction, Action<Exception> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithAction(pContinuationAction, pExceptionAction, context);
        }

        internal OrleansTask ContinueWithAsyncCompletion(Func<TResult, AsyncCompletion> pContinuationAction, Func<Exception, AsyncCompletion> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithAsyncCompletion(pContinuationAction, pExceptionAction, context);
        }

        internal OrleansTask<TNewResult> ContinueWithFunction<TNewResult>(Func<TResult, TNewResult> pContinuationAction, Func<Exception, TNewResult> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithFunction(pContinuationAction, null, true, pExceptionAction, null, context);
        }

        internal OrleansTask<TNewResult> ContinueWithFunction<TNewResult>(Func<TResult, AsyncValue<TNewResult>> pContinuationAction, Func<Exception, AsyncValue<TNewResult>> pExceptionAction, ISchedulingContext context)
        {
            return HelperContinueWithFunction(null, pContinuationAction, false, null, pExceptionAction, context);
        }

        internal static OrleansTask<T> FromResult<T>(T value, ISchedulingContext state)
        {
            var tcs = new TaskCompletionSource<T>(state);
            tcs.TrySetResult(value);
            return new OrleansTask<T>(tcs.Task);
        }

        internal static OrleansTask<T> FromException<T>(Exception exception, ISchedulingContext state)
        {
            var tcs = new TaskCompletionSource<T>(state);
            tcs.TrySetException(exception);
            return new OrleansTask<T>(tcs.Task);
        }

        public override String ToString()
        {
            return OrleansTaskExtentions.ToString(m_Task);
        }
    }

    internal class DummyLimitsConfiguration : ILimitsConfiguration
    {
        internal DummyLimitsConfiguration()
        {
            LimitValues = new Dictionary<string, LimitValue>();
        }

        public IDictionary<string, LimitValue> LimitValues { get; private set; }

        public LimitValue GetLimit(string name)
        {
            return null;
        }

    }
}
