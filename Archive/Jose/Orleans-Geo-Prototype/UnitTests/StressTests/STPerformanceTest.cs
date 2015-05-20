using System;
using System.Threading;
using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using UnitTestGrains;

namespace UnitTests
{
    /// <summary>
    /// Summary description for PersistenceTest
    /// </summary>
    [TestClass]
    public class STPerformanceTest : UnitTestBase
    {
        public STPerformanceTest()
        {
            Console.WriteLine("#### STPerformanceTest() is called.");
        }

        [ClassCleanup()]
        public static void MyClassCleanup()
        {
            ResetDefaultRuntimes();
        }

        [TestMethod]
        public void STPerformanceTestNonPersistent()
        {
            IErrorGrainWithAsyncMethods nonPersistentGrain = ErrorGrainWithAsyncMethodsFactory.GetGrain(GetRandomGrainId());
            const int nClients = 10;
            const int nIncsPerClient = 10;

            Task[] clients = new Task[nClients];
            for (int i = 0; i < clients.Length; i++)
            {
                clients[i] = Task.Run(() =>
                    {
                        for (int j = 0; j < nIncsPerClient; j++)
                        {
                            Task p = nonPersistentGrain.IncrementAAsync_1();
                            Console.Write(j + " ");
                            p.Wait();
                        }
                    });
            }
            for (int i = 0; i < clients.Length; i++)
            {
                clients[i].Wait();
            }

            int all = nonPersistentGrain.GetA().Result;
            Assert.AreEqual(nClients * nIncsPerClient, all);
            Console.WriteLine("\n\nTEST ENDED SUCCESSFULLY\n\n");
        }
    }
}
