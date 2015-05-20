using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Orleans;

using UnitTestGrainInterfaces;

namespace UnitTestGrains
{
    public class RequestContextTestGrain : GrainBase, IRequestContextTestGrain
    {
        #region Implementation of IRequestContextTestGrain

        public Task<string> TraceIdEcho()
        {
            return Task.FromResult(RequestContext.Get("TraceId") as string);
        }

        public Task<string> TraceIdDoubleEcho()
        {
            var grain = RequestContextTestGrainFactory.GetGrain((new Random()).Next());
            return grain.TraceIdEcho();
        }

        public Task<string> TraceIdDelayedEcho1()
        {
            return AsyncValue<string>.StartNew(() => RequestContext.Get("TraceId") as string).AsTask();
        }

        public Task<string> TraceIdDelayedEcho2()
        {
            return AsyncCompletion.FromTask(TaskDone.Done).ContinueWith(() => RequestContext.Get("TraceId") as string).AsTask();
        }

        public Task<Guid> E2EActivityId()
        {
            return Task.FromResult(Trace.CorrelationManager.ActivityId);
        }

        #endregion
    }

    public class RequestContextTaskGrain : GrainBase, IRequestContextTaskGrain
    {
        private OrleansLogger logger;

        public override Task ActivateAsync()
        {
            logger = base.GetLogger();
            return TaskDone.Done;
        }

        #region Implementation of IRequestContextTaskGrain

        public Task<string> TraceIdEcho()
        {
            string traceId = RequestContext.Get("TraceId") as string;
            logger.Info(0, "{0}: TraceId={1}", "TraceIdEcho", traceId);
            return Task.FromResult(traceId);
        }

        public Task<string> TraceIdDoubleEcho()
        {
            var grain = RequestContextTaskGrainFactory.GetGrain((new Random()).Next());
            return grain.TraceIdEcho();
        }

        public Task<string> TraceIdDelayedEcho1()
        {
            string method = "TraceIdDelayedEcho1";
            logger.Info(0, "{0}: Entered", method);
            string traceIdOutside = RequestContext.Get("TraceId") as string;
            logger.Info(0, "{0}: Outside TraceId={1}", method, traceIdOutside);

            return Task.Factory.StartNew(() =>
            {
                string traceIdInside = RequestContext.Get("TraceId") as string;
                logger.Info(0, "{0}: Inside TraceId={1}", method, traceIdInside);
                return traceIdInside;
            });
        }

        public Task<string> TraceIdDelayedEcho2()
        {
            string method = "TraceIdDelayedEcho2";
            logger.Info(0, "{0}: Entered", method);
            string traceIdOutside = RequestContext.Get("TraceId") as string;
            logger.Info(0, "{0}: Outside TraceId={1}", method, traceIdOutside);

            return TaskDone.Done.ContinueWith(task =>
            {
                string traceIdInside = RequestContext.Get("TraceId") as string;
                logger.Info(0, "{0}: Inside TraceId={1}", method, traceIdInside);
                return traceIdInside;
            });
        }

        public async Task<string> TraceIdDelayedEchoAwait()
        {
            string method = "TraceIdDelayedEchoAwait";
            logger.Info(0, "{0}: Entered", method);
            string traceIdOutside = RequestContext.Get("TraceId") as string;
            logger.Info(0, "{0}: Outside TraceId={1}", method, traceIdOutside);

            string traceId = await TaskDone.Done.ContinueWith(task =>
            {
                string traceIdInside = RequestContext.Get("TraceId") as string;
                logger.Info(0, "{0}: Inside TraceId={1}", method, traceIdInside);
                return traceIdInside;
            });
            logger.Info(0, "{0}: After await TraceId={1}", "TraceIdDelayedEchoAwait", traceId);
            return traceId;
        }

        public Task<string> TraceIdDelayedEchoTaskRun()
        {
            string method = "TraceIdDelayedEchoTaskRun";
            logger.Info(0, "{0}: Entered", method);
            string traceIdOutside = RequestContext.Get("TraceId") as string;
            logger.Info(0, "{0}: Outside TraceId={1}", method, traceIdOutside);

            return Task.Run(() =>
            {
                string traceIdInside = RequestContext.Get("TraceId") as string;
                logger.Info(0, "{0}: Inside TraceId={1}", method, traceIdInside);
                return traceIdInside;
            });
        }

        public Task<Guid> E2EActivityId()
        {
            return Task.FromResult(Trace.CorrelationManager.ActivityId);
        }

        public async Task<Tuple<string, string>> TestRequestContext()
        {
            string bar1 = null;
            RequestContext.Set("jarjar", "binks");

            Task task = Task.Factory.StartNew(() =>
            {
                bar1 = (string)RequestContext.Get("jarjar");
                logger.Info("jarjar inside Task.Factory.StartNew = {0}.", bar1);
            });

            string bar2 = null;
            AsyncCompletion ac = AsyncCompletion.StartNew(() =>
            {
                bar2 = (string)RequestContext.Get("jarjar");
                logger.Info("jarjar inside AsyncCompletion.StartNew  = {0}.", bar2);
            });

            await Task.WhenAll(task, ac.AsTask());
            return new Tuple<string, string>(bar1, bar2);
        }


        #endregion
    }

    public class RequestContextProxyGrain : GrainBase, IRequestContextProxyGrain
    {
        public Task<Guid> E2EActivityId()
        {
            var grain = RequestContextTestGrainFactory.GetGrain((new Random()).Next());
            return grain.E2EActivityId();
        }
    }
}
