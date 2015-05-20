using System;
using System.Threading.Tasks;


namespace Orleans.Scheduler
{
    internal static class SchedulerExtensions
    {
        internal static AsyncCompletion QueueAction(this OrleansTaskScheduler scheduler, Action action, ISchedulingContext targetContext)
        {
            return scheduler.QueueAsyncCompletion(() =>
            {
                action();
                return AsyncCompletion.Done;
            }, targetContext);
        }

        internal static AsyncCompletion QueueAsyncCompletion(this OrleansTaskScheduler scheduler, Func<AsyncCompletion> acFunc, ISchedulingContext targetContext)
        {
            var resolver = new AsyncCompletionResolver();
            scheduler.QueueWorkItem(new ClosureWorkItem(
                () =>
                {
                    try
                    {
                        AsyncCompletion ac = acFunc();
                        ac.ContinueWith(
                            () => resolver.TryResolve(),
                            ex => resolver.TryBreak(ex)
                        ).Ignore();
                    }
                    catch (Exception exc)
                    {
                        resolver.TryBreak(exc);
                    }
                }), targetContext);
            return resolver.AsyncCompletion;
        }

        internal static AsyncValue<T> QueueAsyncValue<T>(this OrleansTaskScheduler scheduler, Func<AsyncValue<T>> avFunc, ISchedulingContext targetContext)
        {
            var resolver = new AsyncValueResolver<T>();
            scheduler.QueueWorkItem(new ClosureWorkItem(
                () =>
                {
                    try
                    {
                        AsyncValue<T> av = avFunc();
                        av.ContinueWith(
                            res => resolver.TryResolve(res),
                            ex => resolver.TryBreak(ex)
                        ).Ignore();
                    }
                    catch (Exception exc)
                    {
                        resolver.TryBreak(exc);
                    }
                }), targetContext);
            return resolver.AsyncValue;
        }

        internal static AsyncValue<T> QueueTask<T>(this OrleansTaskScheduler scheduler, Func<Task<T>> taskFunc, ISchedulingContext targetContext)
        {
            var resolver = new AsyncValueResolver<T>();
            Func<Task> asyncFunc =
                async () =>
                {
                    try
                    {
                        T result = await taskFunc();
                        resolver.TryResolve(result);
                    }
                    catch (Exception exc)
                    {
                        resolver.TryBreak(exc);
                    }
                };
            // it appears that it's not important that we fire-and-forget asyncFunc() because we wait on the
            // AsyncValueResolver().
            scheduler.QueueWorkItem(new ClosureWorkItem(() => asyncFunc().Ignore()), targetContext);
            return resolver.AsyncValue;
        }

        internal static AsyncCompletion QueueTask(this OrleansTaskScheduler scheduler, Func<Task> taskFunc, ISchedulingContext targetContext)
        {
            var resolver = new AsyncCompletionResolver();
            Func<Task> asyncFunc =
                async () =>
                {
                    try
                    {
                        await taskFunc();
                        resolver.TryResolve();
                    }
                    catch (Exception exc)
                    {
                        resolver.TryBreak(exc);
                    }
                };
            // it appears that it's not important that we fire-and-forget asyncFunc() because we wait on the
            // AsyncValueResolver().
            scheduler.QueueWorkItem(new ClosureWorkItem(() => asyncFunc().Ignore()), targetContext);
            return resolver.AsyncCompletion;
        }

        /// <summary>
        /// Execute a closure ensuring that it has a runtime context (e.g. to send messages from an arbitrary thread)
        /// </summary>
        /// <param name="scheduler"></param>
        /// <param name="action"></param>
        /// <param name="targetContext"></param>
        internal static AsyncCompletion RunOrQueueAction(this OrleansTaskScheduler scheduler, Action action, ISchedulingContext targetContext)
        {
            return scheduler.RunOrQueueAsyncCompletion(() =>
            {
                action();
                return AsyncCompletion.Done;
            }, targetContext);
        }

        internal static AsyncCompletion RunOrQueueAsyncCompletion(this OrleansTaskScheduler scheduler, Func<AsyncCompletion> acFunc, ISchedulingContext targetContext)
        {
            ISchedulingContext currentContext = AsyncCompletion.Context;
            if (SchedulingUtils.IsAddressableContext(currentContext)
                && currentContext.Equals(targetContext))
            {
                try { return acFunc(); }
                catch (Exception exc) { return new AsyncCompletion(exc); }
            }
            else
            {
                return scheduler.QueueAsyncCompletion(acFunc, targetContext);
            }
        }

        internal static AsyncValue<T> RunOrQueueAsyncValue<T>(this OrleansTaskScheduler scheduler, Func<AsyncValue<T>> avFunc, ISchedulingContext targetContext)
        {
            ISchedulingContext currentContext = AsyncCompletion.Context;
            if (SchedulingUtils.IsAddressableContext(currentContext)
                && currentContext.Equals(targetContext))
            {
                try { return avFunc(); }
                catch (Exception exc) { return new AsyncValue<T>(exc); }
            }
            else
            {
                return scheduler.QueueAsyncValue(avFunc, targetContext);
            }
        }

        internal static AsyncValue<T> RunOrQueueTask<T>(this OrleansTaskScheduler scheduler, Func<Task<T>> taskFunc, ISchedulingContext targetContext)
        {
            ISchedulingContext currentContext = AsyncCompletion.Context;
            if (SchedulingUtils.IsAddressableContext(currentContext)
                && currentContext.Equals(targetContext))
            {
                try { return AsyncValue<T>.FromTask(taskFunc()); }
                catch (Exception exc) { return new AsyncValue<T>(exc); }
            }
            else
            {
                return scheduler.QueueTask(taskFunc, targetContext);
            }
        }

        internal static AsyncCompletion RunOrQueueTask(this OrleansTaskScheduler scheduler, Func<Task> taskFunc, ISchedulingContext targetContext)
        {
            ISchedulingContext currentContext = AsyncCompletion.Context;
            if (SchedulingUtils.IsAddressableContext(currentContext)
                && currentContext.Equals(targetContext))
            {
                try { return AsyncCompletion.FromTask(taskFunc()); }
                catch (Exception exc) { return new AsyncCompletion(exc); }
            }
            else
            {
                return scheduler.QueueTask(taskFunc, targetContext);
            }
        }
    }
}
