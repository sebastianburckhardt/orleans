using System;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;

using UnitTestGrains;

namespace StatelessWorkerGrain
{
    public class StatelessWorkerGrain : GrainBase, IStatelessWorkerGrain
    {
        private readonly Guid activationGuid = Guid.NewGuid();
        private readonly List<Tuple<DateTime, DateTime>> calls = new List<Tuple<DateTime, DateTime>>();
        private OrleansLogger logger;
        private static HashSet<Guid> allActivationIds = new HashSet<Guid>();

        public Task LongCall()
        {
            int count = 0;
            lock (allActivationIds)
            {
                if (!allActivationIds.Contains(activationGuid))
                {
                    allActivationIds.Add(activationGuid);
                }
                count = allActivationIds.Count;
            }
            DateTime start = DateTime.UtcNow;
            //var sw = Stopwatch.StartNew();
            AsyncCompletionResolver resolver = new AsyncCompletionResolver();
            RegisterTimer(TimerCallback, resolver, TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(-1));
            return resolver.AsyncCompletion.ContinueWith(
                () =>
                {
                    //sw.Stop();
                    DateTime stop = DateTime.UtcNow;
                    calls.Add(new Tuple<DateTime, DateTime>(start,stop));
                    Trace.WriteLine((stop-start).TotalMilliseconds);
                    if (logger == null)
                    {
                        logger = GetLogger(activationGuid.ToString());
                    }
                    logger.Info("Start {0}, stop {1}, duration {2}. #act {3}", Logger.PrintDate(start), Logger.PrintDate(stop), (stop - start), count);
                }).AsTask();
        }

        private static Task TimerCallback(object state)
        {
            ((AsyncCompletionResolver)state).Resolve();
            return TaskDone.Done;
        }


        public Task<Tuple<Guid, List<Tuple<DateTime, DateTime>>>> GetCallStats()
        {
            Thread.Sleep(200);
            if (logger == null)
            {
                logger = GetLogger(activationGuid.ToString());
            }
            lock (allActivationIds)
            {
                logger.Info("# allActivationIds {0}: {1}", allActivationIds.Count, Utils.IEnumerableToString(allActivationIds));
            }
            return Task.FromResult(new Tuple<Guid, List<Tuple<DateTime, DateTime>>>(activationGuid, calls));
        }
    }
}