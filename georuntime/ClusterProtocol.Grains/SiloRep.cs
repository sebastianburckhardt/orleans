using Orleans;
using Orleans.Providers;
using System;
using System.Threading.Tasks;
using Common;
using ClusterProtocol.Interfaces;
using System.Collections.Concurrent;
using System.Threading;
using System.Collections.Generic;
using System.Net;

namespace ClusterProtocol
{
    public class SiloRep : IBootstrapProvider
    {

        private const int duetime = 4000;
        private const int interval = 600;

        public async Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;

            string address = null;
            IPHostEntry host;
            IPAddress localIp = null;
            host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (IPAddress ip in host.AddressList)
            {
                if (ip.AddressFamily.ToString() == "InterNetwork")
                {
                    localIp = ip;
                    Console.WriteLine("IP is {0} ", localIp);
                    address = localIp.ToString();
                }
            }


            Timer = new AsyncTimer(duetime, interval, async () =>
            {
                try
                {
                    var collected = new Dictionary<string, ActivityCounts>();

                    // get current count
                    foreach (var e in counts)
                        counts.AddOrUpdate(
                            e.Key,
                            new ActivityCounts(),
                            (string s, ActivityCounts c) =>
                            {
                                collected.Add(s, c);
                                return new ActivityCounts();
                            }
                           );

                    // update time
                    var instanceinfo = new InstanceInfo()
                    {
                        Timestamp = DateTime.UtcNow,
                        Address = address
                    };

                    // send to cluster rep
                    var clusterrep = GrainFactory.GetGrain<IClusterRep>(0);
                    await clusterrep.ReportActivity(Util.MyInstanceName, instanceinfo, collected);

                    RecordActivity("timer [silo rep]", false);
                }
                catch (Exception e)
                {
                    RecordActivity("timer [silo rep]", true);
                }

                return true;
            });

            // static field for accessing this quickly
            Instance = this;

        }


        private ConcurrentDictionary<string, ActivityCounts> counts = new ConcurrentDictionary<string, ActivityCounts>();

        public void RecordActivity(string resource, bool fail)
        {
            counts.AddOrUpdate(resource,
                new ActivityCounts() { Uses = 1, Fails = fail ? 1 : 0 },
                (s, c) => new ActivityCounts() { Uses = c.Uses + 1, Fails = c.Fails + (fail ? 1 : 0) }
            );
        }



        public string Name { get; private set; }

        private AsyncTimer Timer;

        public static SiloRep Instance
        {
            get;
            private set;
        }




    }


}