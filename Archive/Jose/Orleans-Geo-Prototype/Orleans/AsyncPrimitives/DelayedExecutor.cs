using System;


namespace Orleans
{
    /// <summary>
    /// For internal use only.
    /// Utility functions.
    /// </summary>
    internal static class DelayedExecutor
    {
        [Obsolete("This method is deprecated; use Execute(Func<AsyncValue<T>> function, TimeSpan delay) instead of int delay. " +
            "It will know by itself where to execute the function: inside grain if Execute() is called inside grain code, or outside, if called outside grain code.")]
        public static AsyncValue<T> ExecuteInsideGrain<T>(Func<AsyncValue<T>> function, int delay)
        {
            return ExecuteInsideGrain(function, TimeSpan.FromMilliseconds(delay));
        }

        [Obsolete("This method is deprecated; use Execute(Func<AsyncValue<T>> function, TimeSpan delay) instead of int delay. " +
            "It will know by itself where to execute the function: inside grain if Execute() is called inside grain code, or outside, if called outside grain code.")]
        public static AsyncCompletion ExecuteInsideGrain(Func<AsyncCompletion> function, int delay)
        {
            return ExecuteInsideGrain(function, TimeSpan.FromMilliseconds(delay));
        }

        [Obsolete("This method is deprecated; use Execute(Func<AsyncValue<T>> function, TimeSpan delay) instead. " +
            "It will know by itself where to execute the function: inside grain if Execute() is called inside grain code, or outside, if called outside grain code.")]
        public static AsyncValue<T> ExecuteInsideGrain<T>(Func<AsyncValue<T>> function, TimeSpan delay)
        {
            return Execute(function, delay);
        }

        [Obsolete("This method is deprecated; use Execute(Func<AsyncCompletion> function, TimeSpan delay) instead. " +
            "It will know by itself where to execute the function: inside grain if Execute() is called inside grain code, or outside, if called outside grain code.")]
        public static AsyncCompletion ExecuteInsideGrain(Func<AsyncCompletion> function, TimeSpan delay)
        {
            return Execute(function, delay);
        }

        /// <summary>
        /// Execute the provided function after the given delay.
        /// The provided function will run inside grain context if Execute() is called inside grain code, or outside, if called outside grain code.
        /// When Execute&lt;T&gt; is called from the grain code, the provided function will execute in the context of the current request.
        /// That means that if the grain in Non-Reentrant (default behavior), the grain will be locked and will not allow new requests, 
        /// until the delayed function finishes executing.
        /// </summary>
        public static AsyncValue<T> Execute<T>(Func<AsyncValue<T>> function, TimeSpan delay)
        {
            var resolver = new AsyncCompletionTimeoutExtensions.AsyncValueResolver_Ext<T>();

            OneTimeSafeTimer timer = new OneTimeSafeTimer(obj =>
                {
                    ((AsyncValueResolver<T>)obj).TryResolve(default(T));
                }, resolver);

            // store the timer inside the returned AV, so it is not GCed.
            resolver.AsyncValue_Ext.SetTimer(timer);
            // start the timer after it has been stored in AsyncValue_Ext
            timer.Start(delay);

            // the function will run in the correct caller context (grain context, if run within a grain), 
            // since ContinueWith captures the AsyncCompletion.Context at the time it is called, 
            // and thus the CW delegate will run in this Context.
            return resolver.AsyncValue.ContinueWith<T>(function);
        }

        /// <summary>
        /// Execute the provided function after the given delay.
        /// The provided function will run inside grain context if Execute() is called inside grain code, or outside, if called outside grain code.
        /// When Execute&lt;T&gt; is called from the grain code, the provided function will execute in the context of the current request.
        /// That means that if the grain in Non-Reentrant (default behavior), the grain will be locked and will not allow new requests, 
        /// until the delayed function finishes executing.
        /// </summary>
        public static AsyncCompletion Execute(Func<AsyncCompletion> function, TimeSpan delay)
        {
            return Execute<bool>(() =>
            {
                return function().ContinueWith(() => { return true; });
            }, delay);
        }

        /// <summary>
        /// Execute the provided function after the given delay.
        /// The provided function will run inside grain context if Execute() is called inside grain code, or outside, if called outside grain code.
        /// When ExecuteDetached&lt;T&gt; is called from the grain code, the provided function might execute outside the context of the current request.
        /// That means that even if the grain in Reentrant, the provided function might not execute as part of this request. 
        /// In particular, it might execute during the execution of some future request (subject to turn-based single threaded execution).
        /// </summary>
        public static AsyncValue<T> ExecuteDetached<T>(Func<AsyncValue<T>> function, TimeSpan delay)
        {
            var resolver = new AsyncCompletionTimeoutExtensions.AsyncValueResolver_Ext<T>();

            AsyncValue<T> promise = null;
            // The function will run in the grain context, but not necessarily in the current request context.
            OrleansTimerInsideGrain timer = OrleansTimerInsideGrain.FromTimerCallback(obj =>
            {
                try
                {
                    promise = function();
                }
                catch (Exception exc)
                {
                    resolver.TryBreak(exc);
                    return;
                }
                promise.ContinueWith(val => resolver.TryResolve(val), ex => resolver.TryBreak(ex)).Ignore();
            }, null, delay, Constants.INFINITE_TIMESPAN);


            // store the timer inside the returned AV, so it is not GCed.
            resolver.AsyncValue_Ext.SetTimer(timer);
            // start the timer after it has been stored in AsyncValue_Ext
            timer.Start();

            return resolver.AsyncValue;
        }

        /// <summary>
        /// Execute the provided function after the given delay.
        /// The provided function will run inside grain context if Execute() is called inside grain code, or outside, if called outside grain code.
        /// When ExecuteDetached&lt;T&gt; is called from the grain code, the provided function might execute outside the context of the current request.
        /// That means that even if the grain in Reentrant, the provided function might not execute as part of this request. 
        /// In particular, it might execute during the execution of some future request (subject to turn-based single threaded execution).
        /// </summary>
        public static AsyncCompletion ExecuteDetached(Func<AsyncCompletion> function, TimeSpan delay)
        {
            return ExecuteDetached<bool>(() =>
            {
                return function().ContinueWith(() => { return true; });
            }, delay);
        }

    }
}