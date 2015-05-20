﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

using Orleans.Counters;


namespace Orleans.Runtime
{

    internal class Watchdog : AsynchAgent
    {
        private static readonly TimeSpan heartbeatPeriod = TimeSpan.FromMilliseconds(1000);
        private TimeSpan healthCheckPeriod;

        private DateTime lastHeartbeat;
        private DateTime lastWatchdogCheck;
        private List<IHealthCheckParticipant> participants;
        private readonly Logger logger;
        private readonly CounterStatistic watchdogChecks;
        private CounterStatistic watchdogFailedChecks;

        public Watchdog(TimeSpan watchdogPeriod, List<IHealthCheckParticipant> watchables)
        {
            this.logger = Logger.GetLogger("Watchdog");
            this.healthCheckPeriod = watchdogPeriod;
            this.participants = watchables;
            this.watchdogChecks = CounterStatistic.FindOrCreate(StatNames.STAT_WATCHDOG_NUM_HEALTH_CHECKS);
        }

        public override void Start()
        {
            logger.Info("Starting Silo Watchdog.");
            this.lastHeartbeat = DateTime.UtcNow;
            this.lastWatchdogCheck = DateTime.UtcNow;
            base.Start();
        }

        #region Overrides of AsynchAgent

        protected override void Run()
        {
            while (!cts.IsCancellationRequested)
            {
                try
                {
                    WatchdogHeartbeatTick(null);
                    Thread.Sleep(heartbeatPeriod);
                }
                catch (ThreadAbortException)
                {
                    // Silo is probably shutting-down, so just ignore and exit
                }
                catch (Exception exc)
                {
                    logger.Error(ErrorCode.Watchdog_InternalError, "Watchdog Internal Error.", exc);
                }
            }
        }

        #endregion

        private void WatchdogHeartbeatTick(object state)
        {
            try
            {
                CheckYourOwnHealth(lastHeartbeat, logger);
            }
            finally
            {
                lastHeartbeat = DateTime.UtcNow;
            }
            
            var timeSinceLastWatchdogCheck = (DateTime.UtcNow - lastWatchdogCheck);
            if (timeSinceLastWatchdogCheck > healthCheckPeriod)
            {
                watchdogChecks.Increment();
                int numFailedChecks = 0;
                foreach (IHealthCheckParticipant participant in participants)
                {
                    try
                    {
                        bool ok = participant.CheckHealth(lastWatchdogCheck);
                        if (!ok)
                        {
                            numFailedChecks++;
                        }

                    }
                    catch (Exception exc) 
                    {
                        logger.Warn(ErrorCode.Watchdog_ParticipantThrownException,
                                    String.Format("HealthCheckParticipant {0} has thrown an exception from its CheckHealth method.", participant.ToString()), exc); 
                    }
                }
                if (numFailedChecks > 0)
                {
                    if (watchdogFailedChecks == null)
                    {
                        watchdogFailedChecks = CounterStatistic.FindOrCreate(StatNames.STAT_WATCHDOG_NUM_FAILED_HEALTH_CHECKS);
                    }
                    watchdogFailedChecks.Increment();
                    logger.Warn(ErrorCode.Watchdog_HealthCheckFailure, String.Format("Watchdog had {0} Health Check Failure(s) out of {1} Health Check Participants.", numFailedChecks, participants.Count)); 
                }
                lastWatchdogCheck = DateTime.UtcNow;
            }
        }

        private static void CheckYourOwnHealth(DateTime lastCheckt, Logger logger)
        {
            var timeSinceLastTick = (DateTime.UtcNow - lastCheckt);
            if (timeSinceLastTick > heartbeatPeriod.Multiply(2))
            {
                var gc = new[] { GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2) };
                logger.Warn(ErrorCode.SiloHeartbeatTimerStalled,
                    ".NET Runtime Platform stalled for {0} - possibly GC? We are now using total of {1}MB memory. gc={2}, {3}, {4}",
                    timeSinceLastTick,
                    GC.GetTotalMemory(false) / (1024 * 1024),
                    gc[0],
                    gc[1],
                    gc[2]);
            }
        }
    }
}

