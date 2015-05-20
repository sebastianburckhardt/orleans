using System;

using System.Threading;

namespace Orleans
{
    internal static class AsyncCompletionTimeoutExtensions
    {
        // A mixin: a derived class from AsyncValue that adds a field to store a timer.
        // Without this mixin we would have to have a reference field in every AV object, which would be a waste of memory.
        // The usage of this mixin allows us to allocate this field only when required, when we really use timer with AV.
        internal class AsyncValue_Ext<TResult> : AsyncValue<TResult>
        {
            private IDisposable Timer;

            internal AsyncValue_Ext(OrleansTask<TResult> task)
                : base(task) { }

            internal void SetTimer(IDisposable timer)
            {
                Timer = timer;
            }

            internal void DisposeTimer()
            {
                try
                {
                    this.Timer.Dispose();
                }
                catch (Exception) { }
                this.Timer = null;
            }
        }

        // Just a regular AsyncValueResolver, but encapsulates AsyncValue_Ext instead of AsyncValue.
        internal class AsyncValueResolver_Ext<TResult> : AsyncValueResolver<TResult>
        {
            protected override AsyncValue<TResult> AllocateAsyncValue()
            {
                OrleansTask<TResult> ot = new OrleansTask<TResult>(tcs.Task);
                return new AsyncValue_Ext<TResult>(ot);
            }

            internal AsyncValue_Ext<TResult> AsyncValue_Ext
            {
                get
                {
                    return (AsyncValue_Ext<TResult>)AsyncValue;
                }
            }
        }

        internal static AsyncValue<T> WithTimeout<T>(this AsyncValue<T> av, TimeSpan timeout)
        {
            if (av.IsCompleted)
            {
                return av;
            }

            AsyncValueResolver_Ext<T> resolver = new AsyncValueResolver_Ext<T>();

            av.ContinueWith( t => resolver.TryResolve(t),
                             ex => resolver.TryBreak(ex)).Ignore();

            OneTimeSafeTimer timer = new OneTimeSafeTimer(obj => 
                {
                    ((AsyncResolver)obj).TryBreak(new TimeoutException(String.Format("WithTimeout has timed out after {0}.", timeout)));
                }, resolver);

            // store the timer inside the returned AV, so it is not GCed.
            resolver.AsyncValue_Ext.SetTimer(timer);
            // start the timer after it has been stored in AsyncValue_Ext
            timer.Start(timeout);

            return resolver.AsyncValue;
        }

        internal static AsyncCompletion WithTimeout(this AsyncCompletion ac, TimeSpan timeout)
        {
            if (ac.IsCompleted)
            {
                return ac;
            }
            return WithTimeout<bool>(ac.ContinueWith(() => { return true; }), timeout);
        }
    }
}
