using System;


namespace Orleans
{
    internal static class AsyncCompletionExtensions
    {
        public static AsyncCompletion LogErrors(this AsyncCompletion ac, Logger logger, ErrorCode errorCode, string message = null)
        {
            return ac.FastContinueWith(() => AsyncCompletion.Done,
                (Exception exc) => { logger.Error(errorCode, message ?? "AsyncCompletion.LogErrors ", exc); return new AsyncCompletion(exc); });
        }

        public static AsyncCompletion LogWarnings(this AsyncCompletion ac, Logger logger, ErrorCode errorCode, string message)
        {
            return ac.FastContinueWith(() => AsyncCompletion.Done,
                (Exception exc) => { logger.Warn(errorCode, message, exc); return new AsyncCompletion(exc); });
        }

        public static AsyncCompletion Finally(this AsyncCompletion ac, Action finallyAction)
        {
            return ac.ContinueWith(finallyAction, (Exception exc) => { finallyAction(); throw exc; });
        }

        public static AsyncValue<T> Finally<T>(this AsyncValue<T> av, Action finallyAction)
        {
            return av.ContinueWith((T result) => { finallyAction(); return result; }, (Exception exc) => { finallyAction(); throw exc; });
        }

        // Wraps both function execution and the returned promise with the provide catch action.
        // This way we can safely be sure that either if the function throws directly or returns a broken promise,
        // in both cases the same provided catchAction will be executed.
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static AsyncValue<T> ExecuteWithSafeTryCatch<T>(Func<AsyncValue<T>> func, Action<Exception> catchAction)
        {
            try
            {
                return func()
                        .ContinueWith((T result) => new AsyncValue<T>(result),
                            (Exception exc) =>
                            {
                                catchAction(exc);
                                return new AsyncValue<T>(exc);
                            });
            }
            catch (Exception exc1)
            {
                try
                {
                    catchAction(exc1);
                }
                catch (Exception exc2)
                {
                    return new AsyncValue<T>(exc2);
                }
                return new AsyncValue<T>(exc1);
            }
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static AsyncCompletion ExecuteWithSafeTryCatch(Func<AsyncCompletion> func, Action<Exception> catchAction)
        {
            try
            {
                return func()
                        .ContinueWith(
                            () => { },
                            (Exception exc) =>
                            {
                                if (catchAction != null)
                                {
                                    catchAction(exc);
                                }
                                throw exc;
                            });
            }
            catch (Exception exc1)
            {
                try
                {
                    if (catchAction != null)
                    {
                        catchAction(exc1);
                    }
                }
                catch (Exception exc2)
                {
                    return new AsyncCompletion(exc2);
                }
                return new AsyncCompletion(exc1);
            }
        }
    }
}
