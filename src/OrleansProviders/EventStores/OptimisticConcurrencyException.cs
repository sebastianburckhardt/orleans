﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.Providers.EventStores
{
    /// <summary>
    /// Signifies that a event store stream append operation failed because of a conflicting append.
    /// </summary>
    [Serializable]
    public class OptimisticConcurrencyException : OrleansException
    {
        public OptimisticConcurrencyException() : base("OptimisticConcurrencyException") { }
        public OptimisticConcurrencyException(string msg) : base(msg) { }
        public OptimisticConcurrencyException(string message, Exception innerException) : base(message, innerException) { }
        public OptimisticConcurrencyException(string streamName, int expectedVersion) : base()
        {
            this.StreamName = streamName;
            this.ExpectedVersion = expectedVersion;
        }

        protected OptimisticConcurrencyException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }


        public string StreamName { get; set; }
        public int ExpectedVersion { get; set; }
    }
}
