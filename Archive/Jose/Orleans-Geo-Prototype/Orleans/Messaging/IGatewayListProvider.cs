using System;
using System.Collections.Generic;
using System.Net;

namespace Orleans.Messaging
{
    internal interface IGatewayListProvider
    {
        List<IPEndPoint> GetGateways();

        TimeSpan MaxStaleness { get; }

        bool IsUpdatable { get; }
    }

    internal interface IGatewayListListener
    {
        void GatewayListNotification(List<IPEndPoint> gateways);
    }

    internal interface IGatewayListObserverable
    {
        bool SubscribeToGatewayNotificationEvents(IGatewayListListener listener);

        bool UnSubscribeFromGatewayNotificationEvents(IGatewayListListener listener);
    }

    internal interface IGatewayNamingService : IGatewayListProvider, IGatewayListObserverable
    {
    }
}