using System;
using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Streams;
using Orleans.Runtime.Streams;

namespace UnitTests
{
    [TestClass]
    public class MapReduceTests : UnitTestBase
    {
        private static readonly TimeSpan timeout = TimeSpan.FromSeconds(10);

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        // TODO: Fix this later
        // [TestMethod]
        public void MapReduce_PrimeTest()
        {
            var primes = new[] { 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97 };
     
            var list = new List<int>();
            var result = new ResultHandle();
            var observer = new StreamObserver(n => list.Add((int) n), () => result.Done = true, primes.Length);
            IDistributor source;
            ISink sink;
            var setup = Streaming.BuildMapReduce(
                "UnitTestGrains.IntegerSource", new Dictionary<string, object> { { "Next", 3 }, { "Delta", 2 }, { "Max", 99 } },
                4, 2,
                "UnitTestGrains.PrimeFilterGrain", new Dictionary<string, object>(),
                typeof(Filter).FullName, new Dictionary<string, object> { { "Target", StreamFactory.CreateObjectReference(observer) } },
                out source, out sink);
            setup.Wait();
            result.WaitForFinished(timeout);
            Assert.IsTrue(list.ToSet().SetEquals(primes));
        } 
    }
}
