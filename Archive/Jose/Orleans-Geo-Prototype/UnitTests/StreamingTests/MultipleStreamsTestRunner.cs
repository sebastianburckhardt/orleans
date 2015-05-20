#if !DISABLE_STREAMS

using System;
using System.IO;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Providers;

using Orleans.AzureUtils;
using UnitTestGrains;
using UnitTests.StorageTests;
using Orleans.Streams;

namespace UnitTests.Streaming
{
    public class MultipleStreamsTestRunner
    {
        public static readonly string SMS_STREAM_PROVIDER_NAME = "SMSProvider";
        public static readonly string AQ_STREAM_PROVIDER_NAME = "AzureQueueProvider";
        private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(30);

        private Logger logger;
        private readonly string streamProviderName;
        private readonly int testNumber;
        private readonly bool runFullTest;

        public MultipleStreamsTestRunner(string streamProvider, int testNum = 0, bool fullTest = true)
        {
            this.streamProviderName = streamProvider;
            this.logger = Logger.GetLogger("MultipleStreamsTestRunner", Logger.LoggerType.Application);
            this.testNumber = testNum;
            this.runFullTest = fullTest;
        }

        private void Heading(string testName)
        {
            logger.Info("\n\n************************ {0}_{1}_{2} ********************************* \n\n", streamProviderName, testNumber, testName);
        }

        public async Task StreamTest_MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains(Func<SiloHandle> startSiloFunc = null, Action<SiloHandle> stopSiloFunc = null)
        {
            Heading(String.Format("MultipleStreams_ManyDifferent_ManyProducerGrainsManyConsumerGrains"));
            List<SingleStreamTestRunner> runners = new List<SingleStreamTestRunner>();
            List<Task> tasks = new List<Task>();
            for (int i = 0; i < 10; i++)
            {
                runners.Add(new SingleStreamTestRunner(this.streamProviderName, i, runFullTest));
            }
            foreach (var runner in runners)
            {
                tasks.Add(runner.StreamTest_Create_OneProducerGrainOneConsumerGrain());
            }
            await Task.WhenAll(tasks);
            tasks.Clear();

            SiloHandle silo = null;
            if (startSiloFunc != null)
            {
                silo = startSiloFunc();
            }

            foreach (var runner in runners)
            {
                tasks.Add(runner.BasicTestAsync(runFullTest));
            }
            await Task.WhenAll(tasks);
            tasks.Clear();

            if (stopSiloFunc != null)
            {
                logger.Info("\n\n\nAbout to stop silo  {0} \n\n", silo.Silo.SiloAddress);

                stopSiloFunc(silo);

                foreach (var runner in runners)
                {
                    tasks.Add(runner.BasicTestAsync(runFullTest));
                }
                await Task.WhenAll(tasks);
                tasks.Clear();
            }

            foreach (var runner in runners)
            {
                tasks.Add(runner.StopProxies());
            }
            await Task.WhenAll(tasks);
        }
    }
}

#endif
