using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

using Orleans.Scheduler;

#pragma warning disable 618

namespace UnitTests
{
    //internal class TestChoice : MarshalByRefObject, ITestChoice
    //{
    //    private int frozen;

    //    private readonly object sync;

    //    private readonly List<string> workers;

    //    private readonly List<string> ready;

    //    private readonly List<string> waiting;

    //    private readonly Dictionary<string, EventWaitHandle> events;

    //    private readonly Random random;

    //    private readonly OrleansLogger logger;

    //    private int tick;

    //    private readonly List<string> reports;

    //    /// <summary>
    //    /// Initialize with list of output strings to check repeatability
    //    /// At the moment, TPL doesn't allow deterministic replay
    //    /// </summary>
    //    private readonly string[] validate = null;

    //    public TestChoice(int seed)
    //    {
    //        frozen = 0;
    //        sync = new Object();
    //        workers = new List<string>();
    //        ready = new List<string>();
    //        waiting = new List<string>();
    //        events = new Dictionary<string, EventWaitHandle>();
    //        logger = new Logger("TestChoice", Logger.LoggerType.Runtime);
    //        ((Logger)logger).SetSeverityLevel(OrleansLogger.Severity.Verbose);
    //        if (seed >= 0)
    //        {
    //            if (seed == 0)
    //            {
    //                seed = Math.Abs(new Random().Next());
    //            }
    //            logger.Info("seed={0}", seed);
    //            random = new Random(seed);
    //        }
    //        tick = 0;
    //        if (validate != null)
    //        {
    //            reports = new List<string>();
    //        }
    //    }

    //    private int Tick()
    //    {
    //        return tick++;
    //    }

    //    /// <summary>
    //    /// Freeze all existing workers
    //    /// </summary>
    //    public void Freeze()
    //    {
    //        frozen = workers.Count;
    //    }

    //    public void Unfreeze()
    //    {
    //        if (frozen > 0)
    //        {
    //            frozen = 0;
    //            if (waiting.Count == workers.Count && ready.Count > 0)
    //            {
    //                RunOneStep();
    //            }
    //        }
    //    }

    //    #region Implementation of ITestChoice

    //    public void Add(string worker)
    //    {
    //        lock (sync)
    //        {
    //            if (logger.IsVerbose3) logger.Verbose3("TestChoice add {0}", worker);
    //            workers.Add(worker);
    //            events.Add(worker, new EventWaitHandle(false, EventResetMode.AutoReset));
    //        }
    //    }

    //    public void HasItems(string worker)
    //    {
    //        lock (sync)
    //        {
    //            if (!ready.Contains(worker))
    //            {
    //                if (logger.IsVerbose3) logger.Verbose3("TestChoice ready {0}", worker);
    //                ready.Add(worker);
    //                if (waiting.Count == workers.Count && ready.Count == frozen + 1)
    //                {
    //                    RunOneStep();
    //                }
    //            }
    //        }
    //    }

    //    public void NoItems(string worker)
    //    {
    //        lock (sync)
    //        {
    //            ready.Remove(worker);
    //            if (logger.IsVerbose3) logger.Verbose3("TestChoice none {0}", worker);
    //        }
    //    }

    //    public void Wait(string worker /* todo:, CancellationToken ct, int timeout*/)
    //    {
    //        EventWaitHandle handle;
    //        if (logger.IsVerbose3) logger.Verbose3("TestChoice wait {0}: {1}", worker, Thread.CurrentThread.Name);
    //        lock (sync)
    //        {
    //            if (! waiting.Contains(worker))
    //            {
    //                if (logger.IsVerbose3) logger.Verbose3("TestChoice wait {0}", worker);
    //                waiting.Add(worker);
    //                if (waiting.Count == workers.Count && ready.Count > frozen)
    //                {
    //                    RunOneStep();
    //                }
    //            }
    //            handle = events[worker];
    //        }
    //        handle.WaitOne();
    //        if (logger.IsVerbose3) logger.Verbose3("TestChoice proceed {0}: {1}", worker, Thread.CurrentThread.Name);
    //    }

    //    private void RunOneStep()
    //    {
    //        int i = ready.Count == frozen + 1 ? frozen : (frozen + Choose(ready.Count - frozen));
    //        Report("step {0}/{1}", i, ready.Count);
    //        waiting.Remove(ready[i]);
    //        events[ready[i]].Set();
    //    }

    //    public int Choose(int count)
    //    {
    //        var result = random != null ? random.Next(count) : ChessAPI.Choose(count);
    //        Report("choose {0}/{1}", result, count);
    //        return result;
    //    }


    //    public void Report(string format, params object[] args)
    //    {
    //        var s = Canonicalize(String.Format(format, args));
    //        if (validate != null)
    //        {
    //            if (reports.Count < validate.Length && validate[reports.Count] != s)
    //                logger.Warn(0, String.Format("Did not follow same execution:\r\nWas {0}\r\nIs {1}", validate[reports.Count], s));
    //            reports.Add(s);
    //        }
    //        if (logger.IsVerbose2) logger.Verbose2("[{0}] {1}", Tick(), s);
    //    }

    //    static readonly List<string> Guids = new List<string>();

    //    private static string Canonicalize(string s)
    //    {
    //        return CanonGuid(CanonGuid(s, "Grain="), "Activation=");
    //    }

    //    private static string CanonGuid(string s, string prefix)
    //    {
    //        var i = s.IndexOf(prefix);
    //        if (i < 0)
    //            return s;
    //        var guid = s.Substring(i + prefix.Length, 32).ToLowerInvariant();
    //        var id = Guids.IndexOf(guid);
    //        if (id < 0)
    //        {
    //            id = Guids.Count;
    //            Guids.Add(guid);
    //        }
    //        return s.Substring(0, i + prefix.Length) + "{guid" + id + "}" + s.Substring(i + prefix.Length + 32);
    //    }

    //    #endregion
    //}
}

#pragma warning restore 618
