using System;


namespace Orleans
{    
    internal static class AsyncCompletion_FastCWExtensions
    {
        /// <summary>
        /// This method is for use by the Orleans runtime only.
        /// It should not be used by user code.
        /// </summary>
        /// <param name="continuationAction"></param>
        /// <param name="pExceptionAction"></param>
        /// <returns></returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        internal static AsyncCompletion FastContinueWith(this AsyncCompletion ac, Action continuationAction, Action<Exception> pExceptionAction = null)
        {
#if TRACKING
            ac.Observe();
#endif

            if (ac.Status == AsyncCompletionStatus.CompletedSuccessfully)
            {
                try
                {
                    continuationAction();
                }
                catch (Exception exc)
                {
                    if (pExceptionAction == null)
                        return new AsyncCompletion(exc);
                    try
                    {
                        pExceptionAction(exc);
                    }
                    catch (Exception exc2)
                    {
                        return new AsyncCompletion(exc2);
                    }
                }
                return AsyncCompletion.Done;
            }
            else if (ac.Status == AsyncCompletionStatus.Faulted)
            {
                Exception exc = ac.Exception;
                if (pExceptionAction == null)
                    return new AsyncCompletion(exc);
                try
                {
                    pExceptionAction(exc);
                }
                catch (Exception exc2)
                {
                    return new AsyncCompletion(exc2);
                }
                return AsyncCompletion.Done;
            }
            else
            {
                return ac.ContinueWith(continuationAction, pExceptionAction);
            }
        }

        internal static AsyncCompletion FastContinueWith(this AsyncCompletion ac, Func<AsyncCompletion> continuationAction, Func<Exception, AsyncCompletion> pExceptionAction = null)
        {
            return FastContinueWithHelper(ac, false, continuationAction, pExceptionAction);
        }

        internal static AsyncCompletion FastSystemContinueWith(this AsyncCompletion ac, Func<AsyncCompletion> continuationAction, Func<Exception, AsyncCompletion> pExceptionAction = null)
        {
            return FastContinueWithHelper(ac, true, continuationAction, pExceptionAction);            
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static AsyncCompletion FastContinueWithHelper(this AsyncCompletion ac, bool system, Func<AsyncCompletion> continuationAction, Func<Exception,AsyncCompletion> pExceptionAction = null)
        {
#if TRACKING
            ac.Observe();
#endif

            if (ac.Status == AsyncCompletionStatus.CompletedSuccessfully)
            {
                try
                {
                    return continuationAction();
                }
                catch (Exception exc)
                {
                    if (pExceptionAction == null)
                        return new AsyncCompletion(exc);
                    try
                    {
                        return pExceptionAction(exc);
                    }
                    catch (Exception exc2)
                    {
                        return new AsyncCompletion(exc2);
                    }
                }
            }
            else if (ac.Status == AsyncCompletionStatus.Faulted)
            {
                Exception exc = ac.Exception;
                if (pExceptionAction == null)
                    return new AsyncCompletion(exc);
                try
                {
                    return pExceptionAction(exc);
                }
                catch (Exception exc2)
                {
                    return new AsyncCompletion(exc2);
                }
            }
            else
            {
                if (system)
                    return ac.SystemContinueWith(continuationAction, pExceptionAction);
                else
                    return ac.ContinueWith(continuationAction, pExceptionAction);
            }
        }

//        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
//        internal static AsyncValue<TOut> FastContinueWith<TResult, TOut>(this AsyncValue<TResult> av, Func<TResult, AsyncValue<TOut>> continuationAction, Func<Exception, AsyncValue<TOut>> pExceptionAction = null)
//        {
//#if TRACKING
//            av.Observe();
//#endif
//            if (av.Status == AsyncCompletionStatus.CompletedSuccessfully)
//            {
//                try
//                {
//                    TResult result = av.GetTypedResult();
//                    return continuationAction(result);
//                }
//                catch (Exception exc)
//                {
//                    if (pExceptionAction == null)
//                        return new AsyncValue<TOut>(exc);
//                    try
//                    {
//                        return pExceptionAction(exc);
//                    }
//                    catch (Exception exc2)
//                    {
//                        return new AsyncValue<TOut>(exc2);
//                    }
//                }
//            }
//            else if (av.Status == AsyncCompletionStatus.Faulted)
//            {
//                Exception exc = av.Exception;
//                if (pExceptionAction == null)
//                    return new AsyncValue<TOut>(exc);
//                try
//                {
//                    return pExceptionAction(exc);
//                }
//                catch (Exception exc2)
//                {
//                    return new AsyncValue<TOut>(exc2);
//                }
//            }
//            else
//            {
//                return av.ContinueWith(continuationAction, pExceptionAction);
//            }
//        }

//        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
//        internal static AsyncCompletion FastSystemContinueWith<TResult>(this AsyncValue<TResult> av, Action<TResult> continuationAction, Action<Exception> pExceptionAction = null)
//        {
//#if TRACKING
//            av.Observe();
//#endif
//            if (av.Status == AsyncCompletionStatus.CompletedSuccessfully)
//            {
//                try
//                {
//                    TResult result = av.GetTypedResult();
//                    continuationAction(result);
//                }
//                catch (Exception exc)
//                {
//                    if (pExceptionAction == null)
//                        return new AsyncCompletion(exc);
//                    try
//                    {
//                        pExceptionAction(exc);
//                    }
//                    catch (Exception exc2)
//                    {
//                        return new AsyncCompletion(exc2);
//                    }
//                }
//                return AsyncCompletion.Done;
//            }
//            else if (av.Status == AsyncCompletionStatus.Faulted)
//            {
//                Exception exc = av.Exception;
//                if (pExceptionAction == null)
//                    return new AsyncCompletion(exc);
//                try
//                {
//                    pExceptionAction(exc);
//                }
//                catch (Exception exc2)
//                {
//                    return new AsyncCompletion(exc2);
//                }
//                return AsyncCompletion.Done;
//            }
//            else
//            {
//                return av.SystemContinueWith(continuationAction, pExceptionAction);
//            }
//        }
    }
}
