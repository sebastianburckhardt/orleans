using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrainInterfaces;


namespace UnitTests
{
    [TestClass]
    public class OrderTests : UnitTestBase
    {
        public OrderTests()
//            : base(new Options {MaxRequestQueueLength = 0, UseStore = true, StartFreshOrleans = true})
        {
        }

        [ClassCleanup]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        // todo: what was this test supposed to do? it currently fails inside ProcessReceivedBuffer with OOM [TestMethod]
        //public void OrderTest()
        //{
        //    // create two references to the same grain from different clients
        //    var client1 = GrainClient.Current;
        //    StartSecondGrainClient();
        //    var client2 = GrainClient.Current;
        //    var ref2 = OrderTestGrainFactory.CreateGrain(Name: "Test");
        //    ref2.Wait();
        //    GrainClient.Current = client1;
        //    var ref1 = OrderTestGrainFactory.LookupName("Test");
        //    ref1.Wait();

        //    for (int i = 0; i < 10; i++)
        //    {
        //        // update both refs, with delays so they go to different activations
        //        GrainClient.Current = client1;
        //        var r1 = ref1.Next("ref1", i, Enumerable.Range(0, i).ToList(), 200);
        //        GrainClient.Current = client2;
        //        var r2 = ref2.Next("ref2", i, Enumerable.Range(0, i).ToList(), 200);
        //        r1.Wait();
        //        r2.Wait();
        //        // todo: better way to wait for commit
        //        Thread.Sleep(5000);
        //    }
        //}
    }
}
