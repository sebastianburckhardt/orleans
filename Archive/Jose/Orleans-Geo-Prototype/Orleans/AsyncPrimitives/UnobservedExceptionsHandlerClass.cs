using System;
using System.Threading;
using System.Threading.Tasks;

namespace Orleans
{
    internal class UnobservedExceptionsHandlerClass
    {
        private static readonly Object lockObject = new Object();
        private static readonly Logger logger = Logger.GetLogger("UnobservedExceptionHandler");
        private static UnobservedExceptionDelegate unobservedExceptionHandler;
        private static readonly bool alreadySubscribedToTPLEvent = false;

        internal delegate void UnobservedExceptionDelegate(ISchedulingContext context, Exception exception);
        
        static UnobservedExceptionsHandlerClass()
        {
            lock (lockObject)
            {
                if (!alreadySubscribedToTPLEvent)
                {
                    TaskScheduler.UnobservedTaskException += InternalUnobservedAsyncCompletionExceptionHandler;
                    alreadySubscribedToTPLEvent = true;
                }
            }
        }

        internal static void SetUnobservedExceptionHandler(UnobservedExceptionDelegate handler)
        {
            lock (lockObject)
            {
                if (unobservedExceptionHandler != null && handler != null)
                {
                    throw new InvalidOperationException("Setting AsyncCompletion.SetUnobservedExceptionHandler the second time.");
                }
                unobservedExceptionHandler = handler;
            }
        }

        internal static void ResetUnobservedExceptionHandler()
        {
            lock (lockObject)
            {
                unobservedExceptionHandler = null;
            }
        }

        private static void InternalUnobservedAsyncCompletionExceptionHandler(object sender, UnobservedTaskExceptionEventArgs e)
        {
            AggregateException aggrException = e.Exception;
            Exception baseException = aggrException.GetBaseException();
            Task tplTask = (Task)sender;
            object contextObj = tplTask.AsyncState;
            ISchedulingContext context = contextObj as ISchedulingContext;

            try
            {
                if (unobservedExceptionHandler != null)
                {
                    unobservedExceptionHandler(context, baseException);
                }
            }
            finally
            {
                if (e.Observed)
                {
                    logger.Info(ErrorCode.Runtime_Error_100311, "UnobservedExceptionsHandlerClass caught an UnobservedTaskException which was successfully observed and recovered from. BaseException = {0}. Exception = {1}",
                            baseException.Message, Logger.PrintException(aggrException));
                }
                else
                {
                    string errorStr = String.Format("UnobservedExceptionsHandlerClass Caught an UnobservedTaskException event sent by {0}. Exception = {1}",
                            OrleansTaskExtentions.ToString((Task)sender), Logger.PrintException(aggrException));
                    logger.Error(ErrorCode.Runtime_Error_100005, errorStr);
                    logger.Error(ErrorCode.Runtime_Error_100006, "Exception remained UnObserved!!! The subsequent behaivour depends on the ThrowUnobservedTaskExceptions setting in app config and .NET version.");
                }
            }
        }
    }
}
