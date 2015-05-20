using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using ManagementFramework.Agents.EventProcessingAgents.ComplexEventProcessing.StreamInsight;
using ManagementFramework.Events;
using Microsoft.ComplexEventProcessing;
using Microsoft.ComplexEventProcessing.Linq;
using Orleans.Management.Events;
using Orleans.RuntimeCore;

namespace Orleans.Management.Agents
{
    public class TracerAgent : OrleansManagementAgent
    {
        private TracerFilterEvent filter;

        private int flags;

        public TracerAgent() : base("TracerAgent")
        {
            AddSubscriptionType(typeof(TracerFilterEvent), this.ProcessTracerFilterEvent);
            AddPublishType(typeof(TracerReportEvent));
            flags = -1;
        }

        private void ProcessTracerFilterEvent(Guid eventType, AbstractEvent ae)
        {
            var request = ae as TracerFilterEvent;
            if (request == null)
                return;
            filter = request;
            int tag;
            flags = request.Tags != null && request.Tags.Count > 0
                ? request.Tags.Aggregate(0, (n, t) => n | (Enum.TryParse(t, true, out tag) ? 1 << tag : 0))
                : -1;
        }

        public void Report(Message message, Message.LifecycleTag tag)
        {
            if (filter != null && filter.MethodPrefixes != null && !filter.MethodPrefixes.Any(p => message.DebugContext.StartsWith(p)))
                return;
            if (((1 << (int)tag) & flags) == 0)
                return;
            var e = new TracerReportEvent(message, tag);
            if (filter != null && filter.Sample > 0 && (e.MessageIdentifier.GetHashCode() % filter.Sample) != 0)
                return;
            SendMessage(e);
        }
    }

    /// <summary>
    /// Control the kinds of trace reports that are sent
    /// </summary>
    [Serializable]
    public class TracerFilterEvent : OrleansManagementEvent
    {
        /// <summary>
        /// If non-null and non-empty, only report message lifecycle events with these tags
        /// </summary>
        public List<string> Tags { get; set; }

        /// <summary>
        /// If non-null and non-empty, only show methods beginning with one of these method prefixes
        /// </summary>
        public List<string> MethodPrefixes { get; set; }

        /// <summary>
        /// If non-zero, then sample (approximately) one of N messages
        /// </summary>
        public int Sample { get; set; }
    }

    /// <summary>
    /// Report of a single event in processing a message
    /// </summary>
    [Serializable]
    public class TracerReportEvent : OrleansManagementEvent
    {
        /// <summary>
        /// Unique message identifier - sending grain + activation, target grain, message ID
        /// </summary>
        public string MessageIdentifier { get; set; }

        /// <summary>
        /// Fully qualified class name + method name
        /// </summary>
        public string Method { get; set; }

        /// <summary>
        /// Tag identifying the point at which the event is recorded
        /// </summary>
        public string Tag { get; set; }

        public Message.Directions Direction { get; set; }

        public DateTime Timestamp { get; set; }

        public TracerReportEvent(Message message, Message.LifecycleTag tag)
        {
            MessageIdentifier = message.Direction != Message.Directions.Response
                ? message.SendingSilo + "#" + message.Id + "->" + message.TargetGrain
                : message.TargetSilo + "#" + message.Id + "->" + message.SendingGrain;
            Method = message.DebugContext;
            Direction = message.Direction;
            Tag = tag.ToString();
            Timestamp = DateTime.Now;
        }
    }

    public class TracerOutputEvent : OrleansManagementEvent
    {
        public string MessageIdentifier { get; set; }
        public string Method { get; set; }
        public TimeSpan ReceiveToEnqueue { get; set; }
        public TimeSpan EnqueueToInvoke { get; set; }
        public TimeSpan InvokeToRespond { get; set; }
        public TimeSpan ReceiveToRespond { get; set; }
    }

    public class TracerQuery : IQuery
    {
        #region Implementation of IQuery
        public QueryTemplate DefineQuery(Application application, string queryTemplateName, Collection<string> inputStreamNames)
        {
            var inputStream = CepStream<TracerReportEvent>.Create(inputStreamNames[0]);
            var interval = TimeSpan.FromMinutes(2);

            // calculate timespans for each message
            var delays = from e in inputStream
                         group e by e.MessageIdentifier into perMessage
                         from w in perMessage.TumblingWindow(interval, HoppingWindowOutputPolicy.ClipToWindowEnd)
                         select new
                         {
                             MessageIdentifier = w.Min(e => e.MessageIdentifier),
                             Method = w.Min(e => e.Method),
                             //Create = w.Min(e => e.Tag == Message.LifecycleTag.Create.ToString()
                             //   ? e.Timestamp : DateTime.MaxValue),
                             Receive = w.Min(e => e.Tag == Message.LifecycleTag.ReceiveIncoming.ToString() &&
                                    e.Direction != Message.Directions.Response
                                ? e.Timestamp : DateTime.MaxValue),
                             Enqueue = w.Min(e => e.Tag == Message.LifecycleTag.EnqueueWorkItem.ToString() &&
                                    e.Direction != Message.Directions.Response
                                ? e.Timestamp : DateTime.MaxValue),
                             Invoke = w.Min(e => e.Tag == Message.LifecycleTag.InvokeIncoming.ToString() &&
                                    e.Direction != Message.Directions.Response
                                ? e.Timestamp : DateTime.MaxValue),
                             Respond = w.Min(e => e.Tag == Message.LifecycleTag.CreateResponse.ToString()
                                ? e.Timestamp : DateTime.MaxValue)
                         };

            // filter complete messages in the span with total time > 100 ms
            var delays2 = from d in delays
                          where //d.Create != DateTime.MaxValue &&
                              d.Receive != DateTime.MaxValue &&
                              d.Enqueue != DateTime.MaxValue &&
                              d.Invoke != DateTime.MaxValue &&
                              d.Respond != DateTime.MaxValue &&
                              d.Respond.Subtract(d.Receive) > TimeSpan.FromMilliseconds(100)
                          select new TracerOutputEvent
                                     {
                                         MessageIdentifier = d.MessageIdentifier,
                                         Method = d.Method,
                                         ReceiveToEnqueue = d.Enqueue.Subtract(d.Receive),
                                         EnqueueToInvoke = d.Invoke.Subtract(d.Receive),
                                         InvokeToRespond = d.Respond.Subtract(d.Invoke),
                                         ReceiveToRespond = d.Respond.Subtract(d.Receive)
                                     };

            // pick the top 5 in each span
            var delays3 = (from w in delays2.TumblingWindow(interval, HoppingWindowOutputPolicy.ClipToWindowEnd)
                          from e in w
                          orderby e.ReceiveToRespond descending
                          select e).Take(5);

            return application.CreateQueryTemplate(queryTemplateName, "MessageTrace", delays3);
        }

        #endregion
    }
}
