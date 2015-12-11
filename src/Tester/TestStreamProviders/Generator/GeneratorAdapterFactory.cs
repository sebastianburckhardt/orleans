﻿/*
Project Orleans Cloud Service SDK ver. 1.0
 
Copyright (c) Microsoft Corporation
 
All rights reserved.
 
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and 
associated documentation files (the ""Software""), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS
OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.Providers.Streams.Common;
using Orleans.Runtime;
using Orleans.Streams;

namespace Tester.TestStreamProviders.Generator
{
    public enum StreamGeneratorCommand
    {
        Configure = PersistentStreamProviderCommand.AdapterFactoryCommandStartRange
    }

    /// <summary>
    /// Adapter factory for stream generator stream provider.
    /// This factory acts as the adapter and the adapter factory.  It creates receivers that use configurable generator
    ///   to generate event streams, rather than reading them from storage.
    /// </summary>
    public class GeneratorAdapterFactory : IQueueAdapterFactory, IQueueAdapter, IControllable
    {
        public const string GeneratorConfigTypeName = "StreamGeneratorConfigType";
        private IServiceProvider serviceProvider;
        private GeneratorAdapterConfig adapterConfig;
        private IStreamGeneratorConfig generatorConfig;
        private IStreamQueueMapper streamQueueMapper;
        private IStreamFailureHandler streamFailureHandler;
        private ConcurrentDictionary<QueueId, Receiver> receivers;
        private Logger logger;

        public bool IsRewindable { get { return true; } }
        public StreamProviderDirection Direction { get { return StreamProviderDirection.ReadOnly; } }

        public void Init(IProviderConfiguration providerConfig, string providerName, Logger log, IServiceProvider svcProvider)
        {
            logger = log;
            serviceProvider = svcProvider;
            receivers = new ConcurrentDictionary<QueueId, Receiver>();
            adapterConfig = new GeneratorAdapterConfig(providerName);
            adapterConfig.PopulateFromProviderConfig(providerConfig);
            if (adapterConfig.GeneratorConfigType != null)
            {
                generatorConfig = serviceProvider.GetService(adapterConfig.GeneratorConfigType) as IStreamGeneratorConfig;
                if (generatorConfig == null)
                {
                    throw new ArgumentOutOfRangeException("providerConfig", "GeneratorConfigType not valid.");
                }
                generatorConfig.PopulateFromProviderConfig(providerConfig);
            }
        }

        public Task<IQueueAdapter> CreateAdapter()
        {
            return Task.FromResult<IQueueAdapter>(this);
        }

        public IQueueAdapterCache GetQueueAdapterCache()
        {
            return new SimpleQueueAdapterCache(adapterConfig.CacheSize, logger);
        }

        public IStreamQueueMapper GetStreamQueueMapper()
        {
            return streamQueueMapper ?? (streamQueueMapper = new HashRingBasedStreamQueueMapper(adapterConfig.TotalQueueCount, adapterConfig.StreamProviderName));
        }

        public Task<IStreamFailureHandler> GetDeliveryFailureHandler(QueueId queueId)
        {
            return Task.FromResult(streamFailureHandler ?? (streamFailureHandler = new NoOpStreamDeliveryFailureHandler()));
        }

        public string Name { get { return adapterConfig.StreamProviderName; } }

        public Task QueueMessageBatchAsync<T>(Guid streamGuid, string streamNamespace, IEnumerable<T> events, StreamSequenceToken token,
            Dictionary<string, object> requestContext)
        {
            return TaskDone.Done;
        }

        public IQueueAdapterReceiver CreateReceiver(QueueId queueId)
        {
            Receiver receiver = receivers.GetOrAdd(queueId, qid => new Receiver());
            SetGeneratorOnReciever(receiver);
            return receiver;
        }

        public Task<object> ExecuteCommand(int command, object arg)
        {
            if (arg == null)
            {
                throw new ArgumentNullException("arg");
            }
            generatorConfig = arg as IStreamGeneratorConfig;
            if (generatorConfig == null)
            {
                throw new ArgumentOutOfRangeException("arg", "Arg must by of type IStreamGeneratorConfig");
            }

            // update generator on recievers
            foreach (Receiver receiver in receivers.Values)
            {
                SetGeneratorOnReciever(receiver);
            }

            return Task.FromResult<object>(true);
        }

        private class Receiver : IQueueAdapterReceiver
        {
            const int MaxDelayMs = 20;
            private readonly Random random = new Random((int)DateTime.UtcNow.Ticks % int.MaxValue);

            public IStreamGenerator QueueGenerator { get; set; }

            public Task Initialize(TimeSpan timeout)
            {
                return TaskDone.Done;
            }

            public async Task<IList<IBatchContainer>> GetQueueMessagesAsync(int maxCount)
            {
                await Task.Delay(random.Next(1,MaxDelayMs));
                List<IBatchContainer> batches;
                if (QueueGenerator == null || !QueueGenerator.TryReadEvents(DateTime.UtcNow, out batches))
                {
                    return new List<IBatchContainer>();
                }
                return batches;
            }

            public Task MessagesDeliveredAsync(IList<IBatchContainer> messages)
            {
                return TaskDone.Done;
            }

            public Task Shutdown(TimeSpan timeout)
            {
                return TaskDone.Done;
            }
        }

        private void SetGeneratorOnReciever(Receiver receiver)
        {
            // if we don't have generator configuration, don't set generator
            if (generatorConfig == null)
            {
                return;
            }

            var generator = serviceProvider.GetService(generatorConfig.StreamGeneratorType) as IStreamGenerator;
            if (generator == null)
            {
                throw new OrleansException(string.Format("StreamGenerator type not supported: {0}", generatorConfig.StreamGeneratorType));
            }
            generator.Configure(serviceProvider, generatorConfig);
            receiver.QueueGenerator = generator;
        }
    }
}
