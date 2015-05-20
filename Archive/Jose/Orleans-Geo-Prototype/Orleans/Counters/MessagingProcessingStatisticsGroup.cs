using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;


namespace Orleans.Counters
{
    internal class MessagingProcessingStatisticsGroup
    {
        private static CounterStatistic[] Dispatcher_MessagesProcessedOkPerDirection;
        private static CounterStatistic[] Dispatcher_MessagesProcessedErrorsPerDirection;
        private static CounterStatistic[] Dispatcher_MessagesProcessedReRoutePerDirection;
        private static CounterStatistic[] Dispatcher_MessagesProcessingReceivedPerDirection;
        private static CounterStatistic Dispatcher_MessagesProcessedTotal;
        private static CounterStatistic Dispatcher_MessagesReceivedTotal;
        private static CounterStatistic[] Dispatcher_ReceivedByContext;
      
        private static CounterStatistic IGC_MessagesForwarded;
        private static CounterStatistic IGC_MessagesResent;
        private static CounterStatistic IGC_MessagesReRoute;

        private static CounterStatistic IMA_Received;
        private static CounterStatistic[] IMA_EnqueuedByContext;


        internal static void Init()
        {
            Dispatcher_MessagesProcessedOkPerDirection = new CounterStatistic[Enum.GetValues(typeof(Message.Directions)).Length];
            foreach (var direction in Enum.GetValues(typeof(Message.Directions)))
            {
                Dispatcher_MessagesProcessedOkPerDirection[(int)direction] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_OK_PER_DIRECTION, Enum.GetName(typeof(Message.Directions), direction)));
            }
            Dispatcher_MessagesProcessedErrorsPerDirection = new CounterStatistic[Enum.GetValues(typeof(Message.Directions)).Length];
            foreach (var direction in Enum.GetValues(typeof(Message.Directions)))
            {
                Dispatcher_MessagesProcessedErrorsPerDirection[(int)direction] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_ERRORS_PER_DIRECTION, Enum.GetName(typeof(Message.Directions), direction)));
            }
            Dispatcher_MessagesProcessedReRoutePerDirection = new CounterStatistic[Enum.GetValues(typeof(Message.Directions)).Length];
            foreach (var direction in Enum.GetValues(typeof(Message.Directions)))
            {
                Dispatcher_MessagesProcessedReRoutePerDirection[(int)direction] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_REROUTE_PER_DIRECTION, Enum.GetName(typeof(Message.Directions), direction)));
            }

            Dispatcher_MessagesProcessingReceivedPerDirection = new CounterStatistic[Enum.GetValues(typeof(Message.Directions)).Length];
            foreach (var direction in Enum.GetValues(typeof(Message.Directions)))
            {
                Dispatcher_MessagesProcessingReceivedPerDirection[(int)direction] = CounterStatistic.FindOrCreate(new StatName(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_PER_DIRECTION, Enum.GetName(typeof(Message.Directions), direction)));
            }
            Dispatcher_MessagesProcessedTotal = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_DISPATCHER_PROCESSED_TOTAL);
            Dispatcher_MessagesReceivedTotal = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_TOTAL);

            IGC_MessagesForwarded = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IGC_FORWARDED);
            IGC_MessagesResent = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IGC_RESENT);
            IGC_MessagesReRoute = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IGC_REROUTE);

            IMA_Received = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IMA_RECEIVED);
            IMA_EnqueuedByContext = new CounterStatistic[3];
            IMA_EnqueuedByContext[0] = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IMA_ENQUEUED_TO_NULL);
            IMA_EnqueuedByContext[1] = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IMA_ENQUEUED_TO_SYSTEM_TARGET);
            IMA_EnqueuedByContext[2] = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_IMA_ENQUEUED_TO_ACTIVATION);

            Dispatcher_ReceivedByContext = new CounterStatistic[2];
            Dispatcher_ReceivedByContext[0] = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_ON_NULL);
            Dispatcher_ReceivedByContext[1] = CounterStatistic.FindOrCreate(StatNames.STAT_MESSAGING_DISPATCHER_RECEIVED_ON_ACTIVATION);
        }

        internal static void OnDispatcherMessageReceive(Message msg, ISchedulingContext context)
        {
            Dispatcher_MessagesProcessingReceivedPerDirection[(int)msg.Direction].Increment();
            Dispatcher_MessagesReceivedTotal.Increment();
            if (context == null)
            {
                Dispatcher_ReceivedByContext[0].Increment();
            }
            else if (context.ContextType == SchedulingContextType.Activation)
            {
                Dispatcher_ReceivedByContext[1].Increment();
            }
        }

        internal static void OnDispatcherMessageProcessedOK(Message msg)
        {
            Dispatcher_MessagesProcessedOkPerDirection[(int)msg.Direction].Increment();
            Dispatcher_MessagesProcessedTotal.Increment();
        }

        internal static void OnDispatcherMessageProcessedError(Message msg, string reason)
        {
            Dispatcher_MessagesProcessedErrorsPerDirection[(int)msg.Direction].Increment();
            Dispatcher_MessagesProcessedTotal.Increment();
        }

        internal static void OnDispatcherMessageReRouted(Message msg)
        {
            Dispatcher_MessagesProcessedReRoutePerDirection[(int)msg.Direction].Increment();
            Dispatcher_MessagesProcessedTotal.Increment();
        }

        internal static void OnIGCMessageForwared(Message msg)
        {
            IGC_MessagesForwarded.Increment();
        }

        internal static void OnIGCMessageResend(Message msg)
        {
            IGC_MessagesResent.Increment();
        }

        internal static void OnIGCMessageReRoute(Message msg)
        {
            IGC_MessagesReRoute.Increment();
        }

        internal static void OnIMAMessageReceived(Message msg)
        {
            IMA_Received.Increment();
        }

        internal static void OnIMAMessageEnqueued(ISchedulingContext context)
        {
            if (context == null)
            {
                IMA_EnqueuedByContext[0].Increment();
            }
            else if (context.ContextType == SchedulingContextType.SystemTarget)
            {
                IMA_EnqueuedByContext[1].Increment();
            }
            else if (context.ContextType == SchedulingContextType.Activation)
            {
                IMA_EnqueuedByContext[2].Increment();
            }
        }
    }
}

