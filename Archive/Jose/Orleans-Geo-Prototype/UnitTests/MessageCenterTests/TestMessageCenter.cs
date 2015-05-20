using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;


namespace UnitTests
{

    //[Serializable]
    //public class TestMessageCenter : IMessageCenter
    //{
    //    private readonly TestMessageCommonQueue common;

    //    private readonly TestInboundMessageQueue mine;

    //    public TestMessageCenter(SiloAddress silo, TestMessageCommonQueue common)
    //    {
    //        MyAddress = silo;
    //        this.common = common;
    //        mine = common.CreateInboundQueue(silo);
    //    }

    //    public IInboundMessageQueue InboundQueue { get { return mine; } }

    //    #region Implementation of IMessageCenter

    //    public SiloAddress MyAddress { get; private set; }

    //    //public IRouter Router { get; set; }

    //    public void Start()
    //    {
    //        //if (Router != null)
    //        //    Router.Start();
    //    }

    //    public void Stop()
    //    {
    //        mine.Stop();
    //        //if (Router != null)
    //        //{
    //        //    Router.Stop();
    //        //}
    //    }

    //    public void SendMessage(Message msg)
    //    {
    //        if (msg.SendingSilo == null)
    //        {
    //            msg.SendingSilo = MyAddress;
    //        }
    //        if (msg.TargetSilo == null)
    //        {
    //            //if (!Router.Route(msg, out msg))
    //            //    return; // will send back here?

    //            var header = msg.TaskHeader;
    //            Assert.IsTrue(header == null || (header.Active != null && header.Active.Request != null));
    //        }
    //        common.SendMessage(msg);
    //    }

    //    public Message WaitMessage(Message.Categories type, CancellationToken ct)
    //    {
    //        Assert.IsTrue(type == Message.Categories.Application && default(CancellationToken).Equals(ct));
    //        while (true)
    //        {
    //            var result = mine.WaitMessage(type);
    //            if (!result.GetScalarHeader<bool>(Message.Header.ReroutingRequested))
    //                return result;
    //            result.RemoveHeader(Message.Header.ReroutingRequested);
    //            result.RemoveHeader(Message.Header.TargetSilo);
    //            SendMessage(result);
    //        }
    //    }

    //    public void RecordProxiedGrain(GrainId id, Guid client)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    #endregion

    //    #region IMessageCenter Members

    //    public Action<Message> RerouteHandler { set; private get; }

    //    /// <summary>
    //    /// Silo usage only.
    //    /// </summary>
    //    public Action<List<GrainId>> ClientDropHandler
    //    {
    //        set { throw new NotImplementedException(); }
    //    }

    //    public void RerouteMessage(Message message)
    //    {
    //        if (RerouteHandler != null)
    //            RerouteHandler(message);
    //        else
    //            SendMessage(message);
    //    }

    //    /// <summary>
    //    /// Silo use only.
    //    /// </summary>
    //    public Func<SiloAddress, bool> SiloDeadOracle
    //    {
    //        get { return null; }
    //        set { }
    //    }

    //    #endregion


    //    public int SendQueueLength
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    public int ReceiveQueueLength
    //    {
    //        get { throw new NotImplementedException(); }
    //    }

    //    public void RecordProxiedGrain(GrainId id, SiloAddress client)
    //    {
    //    }

    //    public void RecordUnproxiedGrain(GrainId id)
    //    {
    //    }

    //    public bool IsProxying
    //    {
    //        get { return false; }
    //    }

    //    public bool TryDeliverToProxy(Message msg)
    //    {
    //        return false;
    //    }

    //    #region Implementation of IDisposable

    //    public void Dispose()
    //    {
    //        Stop();
    //    }

    //    #endregion
    //}

    //public class TestInboundMessageQueue : MarshalByRefObject, IInboundMessageQueue
    //{
    //    // queues of messages from activation->activation or silo->silo
    //    private readonly List<QueueAction<Message>> queues;

    //    private readonly AutoResetEvent step;

    //    private readonly TestMessageCommonQueue common;

    //    private SiloAddress silo;

    //    public TestInboundMessageQueue(TestMessageCommonQueue common, SiloAddress silo)
    //    {
    //        queues = new List<QueueAction<Message>>();
    //        step = new AutoResetEvent(false);
    //        this.common = common;
    //        this.silo = silo;
    //    }

    //    /// <summary>
    //    /// Allow a single message to go through
    //    /// </summary>
    //    public void ReceiveOneMessage()
    //    {
    //        Assert.IsTrue(queues.Count > 0);
    //        step.Set();
    //    }

    //    public int Count
    //    {
    //        get { lock (queues) { return queues.Sum(q => q.Count); } }
    //    }

    //    public void Start()
    //    {
    //        // nothing
    //    }

    //    public void Stop()
    //    {
    //        // nothing
    //    }

    //    public void PostMessage(Message message)
    //    {
    //        bool transition = false;
    //        lock (queues)
    //        {
    //            var i = queues.FindIndex(q => Match(q.Peek(), message));
    //            if (i < 0)
    //            {
    //                i = queues.Count;
    //                queues.Add(new QueueAction<Message>());
    //                transition = i == 0;
    //            }
    //            if (common.Logger.IsVerbose3) common.Logger.Verbose3("Enqueue {0}{1}#{2} of {3} {4}", transition ? "signal " : "", i, queues[i].Count, queues.Count, message);
    //            queues[i].Enqueue(message);
    //        }
    //        if (transition)
    //        {
    //            common.AddReady(silo);
    //        }
    //    }

    //    private static bool Match(Message a, Message b)
    //    {
    //        return a.TargetAddress.Equals(b.TargetAddress) && a.SendingAddress.Equals(b.SendingAddress);
    //    }

    //    public Message WaitMessage(Message.Categories type)
    //    {
    //        while (true)
    //        {
    //            //if (stopped)
    //            //    return null;
    //            bool empty = false;
    //            Message result = null;
    //            step.WaitOne();
    //            lock (queues)
    //            {
    //                if (queues.Count > 0)
    //                {
    //                    var index = ChessAPI.Choose(queues.Count);
    //                    var message = queues[index].Dequeue();
    //                    if (queues[index].Count == 0)
    //                    {
    //                        queues.RemoveAt(index);
    //                        empty = queues.Count == 0;
    //                    }
    //                    result = message;
    //                }
    //            }
    //            if (common.Logger.IsVerbose3 && result != null) common.Logger.Verbose3("Dequeue {0} -> {1}.{2}", result.SendingAddress, result.TargetAddress, result.DebugContext);
    //            if (empty)
    //            {
    //                common.RemoveReady(silo);
    //            }
    //            if (result != null)
    //                return result;
    //        }
    //    }
    //}

    //public class TestMessageCommonQueue : MarshalByRefObject, IOutboundMessageQueue
    //{
    //    // queues of messages going into each silo
    //    private readonly Dictionary<SiloAddress, TestInboundMessageQueue> queues;

    //    // event when all queues are empty
    //    private readonly ManualResetEvent quiet;

    //    // pending messages for debugging
    //    private readonly Dictionary<ActivationAddress, HashSet<Tuple<ActivationAddress, string>>> pending;

    //    public Logger Logger { get; private set; }

    //    // queues that are non-empty
    //    private readonly List<SiloAddress> ready;

    //    // silos that are running
    //    private readonly HashSet<SiloAddress> running;

    //    public TestMessageCommonQueue()
    //    {
    //        queues = new Dictionary<SiloAddress, TestInboundMessageQueue>();
    //        quiet = new ManualResetEvent(true);
    //        pending = new Dictionary<ActivationAddress, HashSet<Tuple<ActivationAddress, string>>>();
    //        ready = new List<SiloAddress>();
    //        running = new HashSet<SiloAddress>();
    //        Logger = new Logger("TestMessageCommon");
    //    }


    //    public TestInboundMessageQueue CreateInboundQueue(SiloAddress silo)
    //    {
    //        var result = new TestInboundMessageQueue(this, silo);
    //        lock (queues)
    //        {
    //            if (Logger.IsVerbose2) Logger.Verbose2("Register {0} running {1}", silo, running.ToStrings());
    //            queues.Add(silo, result);
    //        }
    //        return result;
    //    }

    //    public void OnIdle(SiloAddress silo)
    //    {
    //        lock (queues)
    //        {
    //            if (Logger.IsVerbose2) Logger.Verbose2("OnIdle {0} running {1}", silo, running.ToStrings());
    //            if (running.Remove(silo) && running.Count == 0)
    //            {
    //                ProcessOneMessage();
    //            }
    //        }
    //    }

    //    public void ProcessOneMessage()
    //    {
    //        lock (queues)
    //        {
    //            var i = ChessAPI.Choose(ready.Count);
    //            running.Add(ready[i]);
    //            queues[ready[i]].ReceiveOneMessage();
    //        }
    //    }

    //    private void UpdatePending(Message msg)
    //    {
    //        var request = msg.Direction == Message.Directions.Request;
    //        var caller = request ? msg.SendingAddress : msg.TargetAddress;
    //        var callee = request ? msg.TargetAddress : msg.SendingAddress;
    //        var tag = String.Format("{0}->{1}#{2}:{3}", caller, callee, msg.Id, msg.DebugContext);
    //        var tuple = Tuple.Create(callee, tag);
    //        if (request)
    //        {
    //            HashSet<Tuple<ActivationAddress, string>> values;
    //            if (!pending.TryGetValue(caller, out values))
    //            {
    //                values = new HashSet<Tuple<ActivationAddress, string>>();
    //                pending.Add(caller, values);
    //            }
    //            values.Add(tuple);
    //        }
    //        else
    //        {
    //            //Assert.IsTrue(/*loops!((!pending.ContainsKey(callee)) || pending[callee].Count == 0) && */pending[caller].Contains(tuple));
    //            pending[caller].Remove(tuple);
    //        }
    //        PrintPending();
    //    }

    //    public void PrintPending()
    //    {
    //        // todo: look for roots? (but check for cycles)
    //        if (!Logger.IsVerbose3)
    //            return;
    //        var seen = new HashSet<ActivationAddress>();
    //        var result = new StringBuilder();
    //        foreach (var activation in pending.Keys)
    //        {
    //            PrintPending(0, activation, seen, result);
    //        }
    //        Logger.Verbose3(result.ToString());
    //    }

    //    private void PrintPending(int indent, ActivationAddress activation, HashSet<ActivationAddress> seen, StringBuilder result)
    //    {
    //        if (seen.Contains(activation))
    //            return;
    //        seen.Add(activation);
    //        HashSet<Tuple<ActivationAddress, string>> values;
    //        if ((!pending.TryGetValue(activation, out values)) || values.Count == 0)
    //            return;
    //        result.AppendFormat("{0}{1}:\r\n", "".PadRight(indent * 4), activation);
    //        foreach (var callee in values)
    //        {
    //            if (pending.ContainsKey(callee.Item1) && pending[callee.Item1].Count == 0)
    //            {
    //                result.AppendFormat("{0}{1}\r\n", "".PadRight(indent * 4 + 2), callee.Item2);
    //                PrintPending(indent + 1, callee.Item1, seen, result);
    //            }
    //        }
    //    }

    //    public void AddReady(SiloAddress silo)
    //    {
    //        lock (queues)
    //        {
    //            ready.Add(silo);
    //            if (ready.Count == 1)
    //            {
    //                quiet.Reset();
    //            }
    //        }
    //    }

    //    public void RemoveReady(SiloAddress silo)
    //    {
    //        lock (queues)
    //        {
    //            ready.Remove(silo);
    //            if (ready.Count == 0)
    //            {
    //                quiet.Set();
    //            }
    //        }
    //    }

    //    public void WaitUntilQuiet()
    //    {
    //        quiet.WaitOne();
    //        lock (queues)
    //        {
    //            if (ready.Count == 0)
    //            {
    //                quiet.Set();
    //            }
    //        }
    //    }

    //    #region Implementation of IDisposable

    //    public void Dispose()
    //    {
    //        // nothing
    //    }

    //    #endregion

    //    #region Implementation of IOutboundMessageQueue

    //    public void Start()
    //    {
    //        // nothing
    //    }

    //    public void Stop()
    //    {
    //        // nothing
    //    }

    //    public bool SendMessage(Message message)
    //    {
    //        lock (queues)
    //        {
    //            TestInboundMessageQueue queue;
    //            if (!queues.TryGetValue(message.TargetSilo, out queue))
    //            {
    //                queue = queues.Where(p => p.Key.Endpoint.Equals(message.TargetSilo.Endpoint)).First().Value;
    //            }
    //            queue.PostMessage(message);
    //        }
    //        UpdatePending(message);
    //        return true;
    //    }

    //    public int Count
    //    {
    //        get { return 0; }
    //    }

    //    #endregion
    //}
}
