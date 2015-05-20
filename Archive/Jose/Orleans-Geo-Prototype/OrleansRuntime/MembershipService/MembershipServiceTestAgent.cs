using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;


namespace Orleans.Runtime.MembershipService
{
    /// <summary>
    /// This agent is used in testing of membership service.
    /// It simply decides after some small startup time whether to kill this silo, or keep it running.
    /// It uses configuration from RuntimeTimeouts and from this class, but can also read configuration parameters from
    /// a file 'liveness.txt' located in the same directory with OrleansHost.exe.
    /// The configuration parameters in the file should be arranged in one line separated by spaces in the
    /// order of constanst in RuntimeTimeouts.
    /// </summary>
    internal class MembershipServiceTestAgent : AsynchAgent
    {
        internal static double KILL_FRACTION = 0.2;      // the fraction of nodes that will kill theirselve
        internal static int KILL_TYPE = 0;               // 0 - deterministic, 1 - random
        private MembershipOracle oracle;
        private DateTime globalTime;
        private int TableRefreshTimer = 0;

        private const string configFileName = "liveness.txt"; // this (optional) file is located where OrleansHost.exe is


        internal MembershipServiceTestAgent(MembershipOracle oracle, DateTime globalTime)
        {
            this.oracle = oracle;
            this.globalTime = globalTime;

            Configure();

            log.SetSeverityLevel(Logger.Severity.Verbose3);
        }

        protected void Configure()
        {
            if (File.Exists(configFileName))
            {
                StreamReader file = new StreamReader(configFileName);
                string line = file.ReadLine();
                file.Close();

                if (line != null)
                {
                    string[] livenessParams = line.Split(' ');
                    //RuntimeTimeouts.LIVENESS_GET_TABLE_UPDATE_TIMER = Int32.Parse(livenessParams[0]);
                    //RuntimeTimeouts.LIVENESS_PROBE_OTHER_SILOS_TIMER = Int32.Parse(livenessParams[1]);
                    //RuntimeTimeouts.LIVENESS_MISSED_PROBES_THRESHOLD = Int32.Parse(livenessParams[2]);
                    //RuntimeTimeouts.LIVENESS_NUM_PROBED_SILOS = Int32.Parse(livenessParams[3]);
                    //RuntimeTimeouts.LIVENESS_NUM_VOTES_FOR_DEATH_DECLARATION = Int32.Parse(livenessParams[4]);
                    //RuntimeTimeouts.LIVENESS_DEATH_VOTE_EXPIRATION_TIMER = Int32.Parse(livenessParams[5]);
                    //RuntimeTimeouts.LIVENESS_DO_GOSSIP_OPTIMIZATION = Boolean.Parse(livenessParams[6]);
                    KILL_FRACTION = Double.Parse(livenessParams[7]);
                    KILL_TYPE = Int32.Parse(livenessParams[8]);
                    //TableRefreshTimer = RuntimeTimeouts.LIVENESS_GET_TABLE_UPDATE_TIMER;
                    if (log.IsVerbose) log.Verbose("Updated constants from " + configFileName);
                }
            }

            //log.Verbose("RuntimeTimeouts.LIVENESS_GET_TABLE_UPDATE_TIMER: " + RuntimeTimeouts.LIVENESS_GET_TABLE_UPDATE_TIMER);
            //log.Verbose("RuntimeTimeouts.LIVENESS_PROBE_OTHER_SILOS_TIMER: " + RuntimeTimeouts.LIVENESS_PROBE_OTHER_SILOS_TIMER);
            //log.Verbose("RuntimeTimeouts.LIVENESS_MISSED_PROBES_THRESHOLD: " + RuntimeTimeouts.LIVENESS_MISSED_PROBES_THRESHOLD);
            //log.Verbose("RuntimeTimeouts.LIVENESS_NUM_PROBED_SILOS: " + RuntimeTimeouts.LIVENESS_NUM_PROBED_SILOS);
            //log.Verbose("RuntimeTimeouts.LIVENESS_NUM_VOTES_FOR_DEATH_DECLARATION: " + RuntimeTimeouts.LIVENESS_NUM_VOTES_FOR_DEATH_DECLARATION);
            //log.Verbose("RuntimeTimeouts.LIVENESS_DEATH_VOTE_EXPIRATION_TIMER: " + RuntimeTimeouts.LIVENESS_DEATH_VOTE_EXPIRATION_TIMER);
            //log.Verbose("RuntimeTimeouts.LIVENESS_DO_GOSSIP_OPTIMIZATION: " + RuntimeTimeouts.LIVENESS_DO_GOSSIP_OPTIMIZATION);
            if (log.IsVerbose) log.Verbose("Killer.KILL_FRACTION: " + KILL_FRACTION);
            if (log.IsVerbose) log.Verbose("Killer.KILL_TYPE: " + KILL_TYPE);
        }

        protected override void Run()
        {
            // Here we calculate the time at which to make decision whether to live or to die.
            // The idea is to take enough time to let all silos to join, but not too much to make tests shorter.
            // Notice that because every silo uses slightly different global clock, it might happen that some silos
            // will choose a different decision time. The test results will be invalid then.
            int minutes = ((globalTime.Minute + TableRefreshTimer / 60 / 1000 + 1) / 2 + 1) * 2;
            int hours = globalTime.Hour;
            if (minutes >= 60)
            {
                hours++;
                minutes -= 60;
            }

            // set the decision time
            DateTime decisionTime = new DateTime(globalTime.Year, globalTime.Month, globalTime.Day, hours, minutes, 0, 0, DateTimeKind.Utc);
            if (log.IsVerbose) log.Verbose("Silo {0} sets decision time to {1} at {2}Z", oracle.MyAddress, Logger.PrintDate(decisionTime), DateTime.UtcNow);

            while (true)
            {
                DateTime now = DateTime.UtcNow;
                if (now >= decisionTime)
                {
                    // decide if we need to get killed
                    List<SiloAddress> tmpList = oracle.GetApproximateSiloStatuses(true).Keys.ToList();
                    tmpList.Sort((x, y) => x.ToString().CompareTo(y.ToString()));

                    int myIndex = tmpList.FindIndex(el => el.Equals(oracle.MyAddress));
                    double random = new SafeRandom().NextDouble();
                    if ((KILL_TYPE == 0 && myIndex + 1 > (1 - KILL_FRACTION) * tmpList.Count) ||
                        (KILL_TYPE == 1 && random <= KILL_FRACTION))        // amke sure not to kill primary
                    {
                        if (log.IsVerbose) log.Verbose("Silo {0} decided to kill itself at {1}", oracle.MyAddress, now.ToLongTimeString());
                        Environment.Exit(1);
                    }
                    else
                    {
                        if (log.IsVerbose) log.Verbose("Silo {0} decided to live at {1}", oracle.MyAddress, now.ToLongTimeString());
                        return;
                    }
                }
                // slee for a second, and check again
                Thread.Sleep(1000);
            }
        }
    }

}
