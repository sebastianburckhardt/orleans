
using System;
using System.Threading;

namespace Orleans.Transactions
{
    public class OrleansClientTransactionService : MarshalByRefObject
    {
        private readonly TransactionManager tm;
        private readonly TransactionManagerProxy[] proxy;
        private readonly ITransactionManagerProxy[] proxyRef;

        private readonly Timer proxyPublisher;
        private volatile bool running;
        private readonly ManualResetEvent serviceTerminatedEvent;

        /// <summary>
        /// Termination event used to signal shutdown of this service.
        /// </summary>
        public WaitHandle ServiceTerminatedEvent => serviceTerminatedEvent;

        public OrleansClientTransactionService(TransactionsConfiguration config)
        {
            tm = new TransactionManager(config);
            proxy = new TransactionManagerProxy[config.TransactionManagerProxyCount];

            proxyRef = new ITransactionManagerProxy[config.TransactionManagerProxyCount];

            for (int i = 0; i < proxy.Length; i++)
            {
                proxy[i] = new TransactionManagerProxy(tm);
                proxyRef[i] = null;
            }

            proxyPublisher = new Timer(PublishProxy);
            running = false;
            serviceTerminatedEvent = new ManualResetEvent(false);
        }

        public void Start()
        {
            CancellationTokenSource cts = new CancellationTokenSource();
            tm.StartAsync().Wait(cts.Token);
            running = true;
            proxyPublisher.Change(TimeSpan.FromMilliseconds(0), TimeSpan.FromMilliseconds(-1));
        }

        public void Stop()
        {
            running = false;
            serviceTerminatedEvent.Set();
            // TODO: add a Stop method to the TM for graceful shut down?
        }

        private async void PublishProxy(object arg)
        {
            if (running)
            {
                for (int i = 0; i < proxy.Length; i++)
                {
                    if (proxyRef[i] == null)
                    {
                        proxyRef[i] = ((GrainFactory)GrainClient.GrainFactory).CreateObjectReference<ITransactionManagerProxy>(proxy[i]);
                    }
                }

                for (int i = 0; i < proxy.Length; i++)
                {
                    var directory = GrainClient.GrainFactory.GetGrain <ITMProxyDirectoryGrain>(i);

                    await directory.SetReference(proxyRef[i]);
                }

                proxyPublisher.Change(TimeSpan.FromSeconds(5), TimeSpan.FromMilliseconds(-1));
            }
        }
    }
}
