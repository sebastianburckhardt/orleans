using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;


namespace Orleans.Counters
{
    internal class NetworkingStatisticsGroup
    {
        // SILO Sockets
        // sending
        private static CounterStatistic ClosedSiloSendingSockets;
        private static CounterStatistic OpenedSiloSendingSockets;
        // receiving
        private static CounterStatistic ClosedSiloReceivingSockets;
        private static CounterStatistic OpenedSiloReceivingSockets;

        // GW SOCKETS
        // Client to GW and GW to Client use the same Duplex socket for send and receive, so we count them once.
        private static CounterStatistic ClosedGWToClientDuplexSockets;
        private static CounterStatistic OpenedGWToClientDuplexSockets;

        // CLIENT SOCKETS
        // duplex
        private static CounterStatistic ClosedClientToGWDuplexSockets;
        private static CounterStatistic OpenedClientToGWDuplexSockets;

        private static bool Silo;

        internal static void Init(bool silo)
        {
            Silo = silo;
            if (Silo)
            {
                ClosedSiloSendingSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_SILO_SENDING_CLOSED);
                OpenedSiloSendingSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_SILO_SENDING_OPENED);
                ClosedSiloReceivingSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_SILO_RECEIVING_CLOSED);
                OpenedSiloReceivingSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_SILO_RECEIVING_OPENED);
                ClosedGWToClientDuplexSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_GWTOCLIENT_DUPLEX_CLOSED);
                OpenedGWToClientDuplexSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_GWTOCLIENT_DUPLEX_OPENED);
            }
            else
            {
                ClosedClientToGWDuplexSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_CLIENTTOGW_DUPLEX_CLOSED );
                OpenedClientToGWDuplexSockets = CounterStatistic.FindOrCreate(StatNames.STAT_NETWORKING_SOCKETS_CLIENTTOGW_DUPLEX_OPENED);
            }
        }

        internal static void OnOpenedSendingSocket()
        {
            if (Silo)
            {
                OpenedSiloSendingSockets.Increment();
            }
        }

        internal static void OnClosedSendingSocket()
        {
            if (Silo)
            {
                ClosedSiloSendingSockets.Increment();
            }
        }

        internal static void OnOpenedReceiveSocket()
        {
            if (Silo)
            {
                OpenedSiloReceivingSockets.Increment();
            }
        }

        internal static void OnClosedReceivingSocket()
        {
            if (Silo)
            {
                ClosedSiloReceivingSockets.Increment();
            }
        }

        //----

        internal static void OnOpenedGWDuplexSocket()
        {
            if (Silo)
            {
                OpenedGWToClientDuplexSockets.Increment();
            }
            else
            {
                OpenedClientToGWDuplexSockets.Increment();
            }
        }

        internal static void OnClosedGWDuplexSocket()
        {
            if (Silo)
            {
                ClosedGWToClientDuplexSockets.Increment();
            }
            else
            {
                ClosedClientToGWDuplexSockets.Increment();
            }
        }
    }
}
