using System;
using System.Collections.Generic;
using System.Threading;

namespace Orleans
{
    internal interface IMessageCenter
    {
        SiloAddress MyAddress { get; }

        void Start();

        void PrepareToStop();

        void Stop();

        void SendMessage(Message msg);

        Message WaitMessage(Message.Categories type, CancellationToken ct);

        int SendQueueLength { get; }

        int ReceiveQueueLength { get; }

        IMessagingConfiguration MessagingConfiguration { get; }
    }

    internal interface ISiloMessageCenter : IMessageCenter
    {
        Action<Message> RerouteHandler { set; }

        Action<Message> SniffIncomingMessage { set; }

        void RerouteMessage(Message message);

        Action<List<GrainId>> ClientDropHandler { set; }

        bool IsProxying { get; }

        void RecordProxiedGrain(GrainId id, Guid client);

        void RecordUnproxiedGrain(GrainId id);

        bool TryDeliverToProxy(Message msg);

        void StopAcceptingClientMessages();

        void BlockApplicationMessages();

        Func<SiloAddress, bool> SiloDeadOracle { get; set; }
    }
}
