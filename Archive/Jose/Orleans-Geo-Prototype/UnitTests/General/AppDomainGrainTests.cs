using System;
using System.Diagnostics;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans;
using Orleans.Host;

using UnitTestGrainInterfaces;

namespace UnitTests.General
{
    [TestClass]
    public class AppDomainGrainTests : UnitTestBase
    {
        private IActivateDeactivateWatcherGrain watcher;
        private static readonly Random random = new Random();
        private readonly PerformanceCounter appDomainCounter;
        private static Process process;

        public AppDomainGrainTests()
            : base(new Options { StartPrimary = true, StartSecondary = false }) // Only need single silo
        {
            try
            {
                //Perf counter: \.NET CLR Loading(process)\Current appdomains
                string thisProcess = Process.GetCurrentProcess().ProcessName;
                appDomainCounter = new PerformanceCounter(".NET CLR Loading", "Current appdomains", thisProcess, true);
            }
            catch (Exception exc)
            {
                Console.WriteLine(@"Cannot access perf counter: 'Current appdomains' -- Exception = " + exc);
                Assert.Inconclusive("Cannot access app domain counter");
            }
        }

        [ClassInitialize]
        public static void ClassInitialize(TestContext testContext)
        {
            ResetDefaultRuntimes();
        }
        [ClassCleanup]
        public static void Cleanup()
        {
            if (process != null)
            {
                try
                {
                    AppDomainHost.KillHostProcess(process);
                    process = null;
                }
                catch (Exception) { }
            }

            ResetDefaultRuntimes();
        }

        [TestInitialize]
        public void TestInitialize()
        {
            watcher = ActivateDeactivateWatcherGrainFactory.GetGrain(0);
            watcher.Clear().Wait();
        }
        [TestCleanup]
        public void TestCleanup()
        {
            Cleanup();
        }

        [TestMethod]
        public void AppDomainGrain_Watcher_GetGrain()
        {
            IActivateDeactivateWatcherGrain grain = ActivateDeactivateWatcherGrainFactory.GetGrain(1);
        }

        [TestMethod]
        public void AppDomainGrain_Deactivate()
        {
            long appDomainsInitialCount = GetAppDomainsCount();
            Console.WriteLine("Initial app domains = " + appDomainsInitialCount);

            int id = random.Next();
            IAppDomainTestGrain grain = AppDomainTestGrainFactory.GetGrain(id);

            // Activate
            ActivationId activation = grain.DoSomething().Result;

            long appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After Activate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount + 1, appDomainsCount, "Number of app domains after Activate");

            ActivationId[] activateCalls = watcher.ActivateCalls.Result;
            Assert.AreEqual(1, activateCalls.Length, "Number of Activate calls");
            Assert.AreEqual(activation, activateCalls[0], "Activate call from expected activation");

            // Deactivate
            grain.DoDeactivate().Wait();
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After Deactivate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Deactivate");

            ActivationId[] deactivateCalls = watcher.DeactivateCalls.Result;
            Assert.AreEqual(1, deactivateCalls.Length, "Number of Deactivate calls");
            Assert.AreEqual(activation, deactivateCalls[0], "Deactivate call from expected activation");

            // Reactivate
            ActivationId activation2 = grain.DoSomething().Result;

            Assert.AreNotEqual(activation, activation2, "New activation created after re-activate");

            activateCalls = watcher.ActivateCalls.Result;
            Assert.AreEqual(2, activateCalls.Length, "Number of Activate calls - After reactivation");
            Assert.AreEqual(activation, activateCalls[0], "Activate call #1 from expected activation");
            Assert.AreEqual(activation2, activateCalls[1], "Activate call #2 from expected activation");

            deactivateCalls = watcher.DeactivateCalls.Result;
            Assert.AreEqual(1, deactivateCalls.Length, "Number of Deactivate calls - After reactivation");
            Assert.AreEqual(activation, deactivateCalls[0], "Deactivate call from activation #1 only");

            appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After reactivate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount + 1, appDomainsCount, "Number of app domains after Reactivate");
        }

        [TestMethod]
        public void AppDomainGrain_Deactivate_Loop()
        {
            long appDomainsInitialCount = GetAppDomainsCount();
            Console.WriteLine("Initial app domains = " + appDomainsInitialCount);

            const int Iterations = 20;
            IAppDomainTestGrain[] grains = new IAppDomainTestGrain[Iterations];
            ActivationId[] activations = new ActivationId[Iterations];

            int baseId = random.Next();
            for (int i = 0; i < Iterations; i++)
            {
                int id = baseId + i;
                grains[i] = AppDomainTestGrainFactory.GetGrain(id);
            }

            // Activate
            for (int i = 0; i < Iterations; i++)
            {
                activations[i] = grains[i].DoSomething().Result;
            }

            long appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After Activate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount + Iterations, appDomainsCount, "Number of app domains after Activate");

            ActivationId[] activateCalls = watcher.ActivateCalls.Result;
            Assert.AreEqual(Iterations, activateCalls.Length, "Number of Activate calls");

            // Deactivate
            for (int i = 0; i < Iterations; i++)
            {
                grains[i].DoDeactivate().Wait();
            }
            Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

            appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After Deactivate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Deactivate");

            ActivationId[] deactivateCalls = watcher.DeactivateCalls.Result;
            Assert.AreEqual(Iterations, deactivateCalls.Length, "Number of Deactivate calls");

            // Reactivate
            for (int i = 0; i < Iterations; i++)
            {
                ActivationId activation2 = grains[i].DoSomething().Result;

                Assert.AreNotEqual(activations[i], activation2, "New activation created after re-activate");
            }

            activateCalls = watcher.ActivateCalls.Result;
            Assert.AreEqual(2 * Iterations, activateCalls.Length, "Number of Activate calls - After reactivation");

            deactivateCalls = watcher.DeactivateCalls.Result;
            Assert.AreEqual(Iterations, deactivateCalls.Length, "Number of Deactivate calls - After reactivation");

            appDomainsCount = GetAppDomainsCount();
            Console.WriteLine("After reactivate, app domains = " + appDomainsCount);
            Assert.AreEqual(appDomainsInitialCount + Iterations, appDomainsCount, "Number of app domains afer Reactivate");
        }

        [TestMethod]
        public void AppDomainHost_Grain()
        {
            IAppDomainHostTestGrain grain = null;
            try
            {
                int id = random.Next();
                grain = AppDomainHostTestGrainFactory.GetGrain(id);

                // Activate
                ActivationId activation = grain.DoSomething().Result;

                ActivationId[] activateCalls = watcher.ActivateCalls.Result;
                Assert.AreEqual(1, activateCalls.Length, "Number of Activate calls");
                Assert.AreEqual(activation, activateCalls[0], "Activate call from expected activation");

                // Deactivate
                grain.DoDeactivate().Wait();
                Thread.Sleep(TimeSpan.FromSeconds(2)); // Allow some time for deactivate to happen

                ActivationId[] deactivateCalls = watcher.DeactivateCalls.Result;
                Assert.AreEqual(1, deactivateCalls.Length, "Number of Deactivate calls");
                Assert.AreEqual(activation, deactivateCalls[0], "Deactivate call from expected activation");

                // Reactivate
                ActivationId activation2 = grain.DoSomething().Result;

                Assert.AreNotEqual(activation, activation2, "New activation created after re-activate");

                activateCalls = watcher.ActivateCalls.Result;
                Assert.AreEqual(2, activateCalls.Length, "Number of Activate calls - After reactivation");
                Assert.AreEqual(activation, activateCalls[0], "Activate call #1 from expected activation");
                Assert.AreEqual(activation2, activateCalls[1], "Activate call #2 from expected activation");

                deactivateCalls = watcher.DeactivateCalls.Result;
                Assert.AreEqual(1, deactivateCalls.Length, "Number of Deactivate calls - After reactivation");
                Assert.AreEqual(activation, deactivateCalls[0], "Deactivate call from activation #1 only");
            }
            finally
            {
                if (grain != null)
                {
                    try
                    {
                        // Deactivate
                        grain.DoDeactivate().Wait();
                    }
                    catch (Exception) { }
                }
            }
        }

        [TestMethod]
        public void AppDomainHost_Spawn()
        {
            try
            {
                long appDomainsInitialCount = GetAppDomainsCount();
                Console.WriteLine("Initial app domains = " + appDomainsInitialCount);

                // Init app domain hosting
                AppDomainHost.InitClient();

                // Spawn process to host remotable type in app domain
                int port = AppDomainHost.BaseServerPort;
                Type hostType = typeof(ResultHandle);

                ResultHandle remote = (ResultHandle)AppDomainHost.GetRemoteObject(hostType, port, null, out process);
                Assert.IsFalse(process.HasExited, "AppDomainHost did not start correctly");

                long appDomainsCount = GetAppDomainsCount();
                Console.WriteLine("After Activate, app domains = " + appDomainsCount);
                Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Activate");

                // Do something with hosted type
                remote.Reset();

                // Deactivate
                AppDomainHost.KillHostProcess(process);
                process = null;

                appDomainsCount = GetAppDomainsCount();
                Console.WriteLine("After Deactivate, app domains = " + appDomainsCount);
                Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Deactivate");
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error = " + exc);
                throw;
            }
        }

        [TestMethod]
        public void AppDomainHost_Spawn_WithArgs()
        {
            try
            {
                long appDomainsInitialCount = GetAppDomainsCount();
                Console.WriteLine("Initial app domains = " + appDomainsInitialCount);

                // Init app domain hosting
                AppDomainHost.InitClient();

                // Spawn process to host remotable type in app domain
                int port = AppDomainHost.BaseServerPort;
                Type hostType = typeof(RemotableTypeWithArgs);
                object[] args = new object[] { 40, 2 };

                RemotableTypeWithArgs remote = (RemotableTypeWithArgs)AppDomainHost.GetRemoteObject(hostType, port, args, out process);
                Assert.IsFalse(process.HasExited, "AppDomainHost did not start correctly");

                long appDomainsCount = GetAppDomainsCount();
                Console.WriteLine("After Activate, app domains = " + appDomainsCount);
                Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Activate");

                // Do something with hosted type
                int result = remote.Add();

                Assert.AreEqual(42, result, "Result from {0}", hostType.FullName);

                // Deactivate
                AppDomainHost.KillHostProcess(process);
                process = null;

                appDomainsCount = GetAppDomainsCount();
                Console.WriteLine("After Deactivate, app domains = " + appDomainsCount);
                Assert.AreEqual(appDomainsInitialCount, appDomainsCount, "Number of app domains after Deactivate");
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error = " + exc);
                throw;
            }
        }

        private long GetAppDomainsCount()
        {
            if (appDomainCounter == null)
            {
                Assert.Inconclusive("Cannot access app domain counter");
            }
            return appDomainCounter.RawValue;
        }
    }

    public class RemotableTypeWithArgs : MarshalByRefObject
    {
        private int A { get; set; }
        private int B { get; set; }

        public RemotableTypeWithArgs(int a, int b)
        {
            this.A = a;
            this.B = b;
        }

        public int Add()
        {
            return A + B;
        }
    }
}
