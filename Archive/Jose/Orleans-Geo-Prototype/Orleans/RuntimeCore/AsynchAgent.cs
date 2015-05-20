using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Orleans.Counters;

namespace Orleans
{
    internal abstract class AsynchAgent : MarshalByRefObject, IDisposable
    {
        public enum FaultBehavior
        {
            CrashOnFault,   // Crash the process if the agent faults
            RestartOnFault, // Restart the agent if it faults
            IgnoreFault     // Allow the agent to stop if it faults, but take no other action (other than logging)
        }

        private Thread t;
        protected CancellationTokenSource cts;
        protected object lockable;
        protected ThreadState state;
        protected Logger log;
        private readonly string type;
        protected FaultBehavior onFault;

#if TRACK_DETAILED_STATS
        internal protected ThreadTrackingStatistic threadTracking;
#endif

        static protected readonly Dictionary<Type, int> sequenceNumbers = new Dictionary<Type, int>();
        static private readonly object classLockable = new object();

        public ThreadState State { get { return state; } }
        internal string Name { get; private set; }
        internal int ManagedThreadId { get { return t==null ? -1 : t.ManagedThreadId;  } } 

        protected AsynchAgent(string nameSuffix)
        {
            cts = new CancellationTokenSource();
            Type thisType = this.GetType();
            int n = 0;
            lock (classLockable)
            {
                sequenceNumbers.TryGetValue(thisType, out n);
                n++;
                sequenceNumbers[thisType] = n;
            }
            type = thisType.Namespace + "." + thisType.Name;
            if (type.StartsWith("Orleans.", StringComparison.Ordinal))
            {
                type = type.Substring(8);
            }
            if (!string.IsNullOrEmpty(nameSuffix))
            {
                Name = type + "." + nameSuffix + "/" + n;
            }
            else
            {
                Name = type + "/" + n;
            }

            lockable = new object();
            state = ThreadState.Unstarted;
            onFault = FaultBehavior.IgnoreFault;
            log = Logger.GetLogger(Name, Logger.LoggerType.Runtime);
            AppDomain.CurrentDomain.DomainUnload += new EventHandler(CurrentDomain_DomainUnload);

#if TRACK_DETAILED_STATS
            if (StatisticsCollector.CollectThreadTimeTrackingStats)
            {
                threadTracking = new ThreadTrackingStatistic(Name);
            }
#endif

            t = new Thread(AgentThreadProc) { IsBackground = true, Name = this.Name };
        }

        protected AsynchAgent() : this(null)
        {
        }

        protected int GetThreadTypeSequenceNumber()
        {
            Type thisType = this.GetType();
            int n = 0;
            lock (classLockable)
            {
                sequenceNumbers.TryGetValue(thisType, out n);
            }
            return n;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private void CurrentDomain_DomainUnload(object sender, EventArgs e)
        {
            try
            {
                Stop();
            }
            catch (Exception exc)
            {
                // ignore. Just make sure DomainUnload handler does not throw.
                log.Verbose("Ignoring error during Stop: {0}", exc);
            }
        }

        public virtual void Start()
        {
            lock (lockable)
            {
                if (state == ThreadState.Running)
                {
                    return;
                }

                if (state == ThreadState.Stopped)
                {
                    cts = new CancellationTokenSource();
                    t = new Thread(AgentThreadProc) { IsBackground = true, Name = this.Name };
                }

                t.Start(this);
                state = ThreadState.Running;
            }
            if(log.IsVerbose) log.Verbose("Started asynch agent " + this.Name);
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public virtual void Stop()
        {
            try
            {
                lock (lockable)
                {
                    if (state == ThreadState.Running)
                    {
                        state = ThreadState.StopRequested;
                        cts.Cancel();
                        //t.Join(1000);
                        //t.Abort();
                        state = ThreadState.Stopped;
                    }
                }
            }
            catch (Exception exc)
            {
                // ignore. Just make sure stop does not throw.
                log.Verbose("Ignoring error during Stop: {0}", exc);
            }
            log.Verbose("Stopped agent");
        }

        public void Abort(object stateInfo)
        {
            if(t!=null)
                t.Abort(stateInfo);
        }

        protected abstract void Run();

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void AgentThreadProc(Object obj)
        {
            var agent = obj as AsynchAgent;
            if (agent == null)
            {
                var log = Logger.GetLogger("RuntimeCore.AsynchAgent");
                log.Error(ErrorCode.Runtime_Error_100022, "Agent thread started with incorrect parameter type");
                return;
            }

            try
            {
                LogStatus(agent.log, "Starting AsyncAgent {0} on managed thread {1}", agent.Name, Thread.CurrentThread.ManagedThreadId);
                CounterStatistic.SetOrleansManagedThread(); // do it before using CounterStatistic.
                CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_RUNTIME_THREADS_ASYNC_AGENT_PERAGENTTYPE, agent.type)).Increment();
                CounterStatistic.FindOrCreate(StatNames.STAT_RUNTIME_THREADS_ASYNC_AGENT_TOTAL_THREADS_CREATED).Increment();
                agent.Run();
            }
            catch (Exception exc)
            {
                if (agent.State == ThreadState.Running) // If we're stopping, ignore exceptions
                {
                    Logger log = agent.log;
                    switch (agent.onFault)
                    {
                        case FaultBehavior.CrashOnFault:
                            Console.WriteLine(
                                "The {0} agent has thrown an unhandled exception, {1}. The process will be terminated.",
                                agent.Name, exc);
                            log.Error(ErrorCode.Runtime_Error_100023,
                                "AsynchAgent Run method has thrown an unhandled exception. The process will be terminated.",
                                exc);
                            log.Fail(ErrorCode.Runtime_Error_100024, "Terminating process because of an unhandled exception caught in AsynchAgent.Run.");
                            break;
                        case FaultBehavior.IgnoreFault:
                            log.Error(ErrorCode.Runtime_Error_100025, "AsynchAgent Run method has thrown an unhandled exception. The agent will exit.",
                                exc);
                            agent.state = ThreadState.Stopped;
                            break;
                        case FaultBehavior.RestartOnFault:
                            log.Error(ErrorCode.Runtime_Error_100026,
                                "AsynchAgent Run method has thrown an unhandled exception. The agent will be restarted.",
                                exc);
                            agent.state = ThreadState.Stopped;
                            try
                            {
                                agent.Start();
                            }
                            catch (Exception ex)
                            {
                                log.Error(ErrorCode.Runtime_Error_100027, "Unable to restart AsynchAgent", ex);
                                agent.state = ThreadState.Stopped;
                            }
                            break;
                    }
                }
            }
            finally
            {
                CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_RUNTIME_THREADS_ASYNC_AGENT_PERAGENTTYPE, agent.type)).DecrementBy(1);
                agent.log.Warn(ErrorCode.Runtime_Error_100328, "Stopping AsyncAgent {0} that runs on managed thread {1}", agent.Name, Thread.CurrentThread.ManagedThreadId);
            }
        }

        #region IDisposable Members

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposing) return;

            if (cts != null)
            {
                cts.Dispose();
                cts = null;
            }
        }

        #endregion

        public override string ToString()
        {
            return Name;
        }

        private static void LogStatus(Logger log, string msg, params object[] args)
        {
            if (SystemStatus.Current.Equals(SystemStatus.Creating))
            {
                // Reduce log noise during silo startup
                if (log.IsVerbose) log.Verbose(msg, args);
            }
            else
            {
                // Changes in agent threads during all operations aside for initial creation are usually important diag events.
                log.Info(msg, args);
            }
        }
    }
}
