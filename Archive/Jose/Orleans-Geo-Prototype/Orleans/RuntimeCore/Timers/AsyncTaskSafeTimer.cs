using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{
    internal class AsyncTaskSafeTimer : MarshalByRefObject, IDisposable
    {
        private SafeTimerBase safeTimerBase;
        private Func<object, Task> callbackFunc;

        public AsyncTaskSafeTimer(Func<object, Task> asynTaskCallback, object state)
        {
            this.callbackFunc = asynTaskCallback;
            safeTimerBase = new SafeTimerBase(callbackFunc, state);
        }

        public AsyncTaskSafeTimer(Func<object, Task> asynTaskCallback, object state, TimeSpan dueTime, TimeSpan period)
        {
            this.callbackFunc = asynTaskCallback;
            safeTimerBase = new SafeTimerBase(callbackFunc, state, dueTime, period);
        }

        public void Start(TimeSpan dueTime, TimeSpan period)
        {
            safeTimerBase.Start(dueTime, period);
        }

        #region IDisposable Members

        public void Dispose()
        {
            safeTimerBase.Dispose();
        }

        // Maybe called by finalizer thread with disposing=false. As per guidelines, in such a case do not touch other objects.
        // Dispose() may be called multiple times
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "safeTimerBase")]
        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                safeTimerBase.DisposeTimer();
            }
        }

        #endregion

        //internal string GetFullName()
        //{
        //    return String.Format("AsyncTaskSafeTimer: {0}. ", callbackFunc != null ? callbackFunc.GetType().FullName : "");
        //}

        public bool CheckTimerFreeze(DateTime lastCheckTime, Func<string> callerName)
        {
            return safeTimerBase.CheckTimerFreeze(lastCheckTime, callerName);
        }
    }
}
