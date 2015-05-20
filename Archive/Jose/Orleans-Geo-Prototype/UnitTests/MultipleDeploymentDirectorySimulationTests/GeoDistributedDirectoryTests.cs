using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

using Microsoft.VisualStudio.TestTools.UnitTesting;
using UnitTestGrains;
using UnitTestGrainInterfaces;
using UnitTests.DirectoryTests;

namespace UnitTests.MultipleDeploymentDirectorySimulationTests
{
    [TestClass]
    public class GeoDistributedDirectoryTests : UnitTestBase
    {
        private int _numberOfDeployments;
        private int _numberOfSilosPerDeployment;

        //public GeoDistributedDirectoryTests(int n, int m)
        //{
            //_numberOfDeployments = n;
          //  _numberOfSilosPerDeployment = m;
        //}

        [TestInitialize]
        public void LoadNetworkTopologyCase1() // 2 deployments, 2 silos per deployment
        {
            // load 4 silos, 2 per deployment
            _numberOfDeployments = 2;
            _numberOfSilosPerDeployment = 2;
            //ISiloGrain is1 = SiloGrainFactory.GetGrain(0);
            //ISiloGrain is2 = SiloGrainFactory.GetGrain(1);

            //is1.SendMessage(is2, "hello");

            var dgrains = new ISiloGrain[_numberOfDeployments, _numberOfSilosPerDeployment];

            for (int i = 0; i < 2; i++)
            {
                for (int j = 0; j < 2; j++)
                {
                    dgrains[i, j] = SiloGrainFactory.GetGrain(Guid.NewGuid());
                }
                
            }

            // create deployment boundaries 00 and 01 are neighbors in one deployment, 10 and 11 are in another deployment
            dgrains[0, 0].AddDeploymentNeighbor(dgrains[0, 0]);
            dgrains[0, 0].AddDeploymentNeighbor(dgrains[0, 1]);
            dgrains[0, 1].AddDeploymentNeighbor(dgrains[0, 0]);
            dgrains[0, 1].AddDeploymentNeighbor(dgrains[0, 1]);

            dgrains[1, 0].AddDeploymentNeighbor(dgrains[1, 0]);
            dgrains[1, 0].AddDeploymentNeighbor(dgrains[1, 1]);
            dgrains[1, 1].AddDeploymentNeighbor(dgrains[1, 0]);
            dgrains[1, 1].AddDeploymentNeighbor(dgrains[1, 1]);


        }

        //[TestMethod]
        public void TestMessagePassing()
        {
            Guid guid1 = Guid.NewGuid();
            var isg1 = SiloGrainFactory.GetGrain(guid1); // reference to ISiloGrain for protocol
            var itsg1 = TestSiloGrainFactory.GetGrain(guid1); // reference to ITestSiloGrain for testing interface
            Guid guid2 = Guid.NewGuid();
            var isg2 = SiloGrainFactory.GetGrain(guid2);
            var itsg2 = TestSiloGrainFactory.GetGrain(guid2);
            itsg1.SendMessage(isg2, "hello");
            var m = itsg2.ReturnLastReceivedMessage().Result;
            //if ( m != null)
            //  Assert.AreEqual("hello", m);
            //Assert.AreEqual("get lost", m);
        }

        //[TestMethod]
        public void TestDeploymentBoundary()
        {
            ISiloGrain d1s1 = SiloGrainFactory.GetGrain(Guid.NewGuid()); // deployment 1, silo 1
            ISiloGrain d1s2 = SiloGrainFactory.GetGrain(Guid.NewGuid()); // deployment 1, silo 2
            ISiloGrain d2s1 = SiloGrainFactory.GetGrain(Guid.NewGuid()); // deployment 2, silo 1

            // simulate deployments

            d1s1.AddDeploymentNeighbor(d1s2);
            d1s1.AddDeploymentNeighbor(d1s1);

            d1s2.AddDeploymentNeighbor(d1s1);
            d1s2.AddDeploymentNeighbor(d1s2);

            d2s1.AddDeploymentNeighbor(d2s1);

            //Assert.IsTrue(d1s1.CheckDeploymentMembership(d2s1).Result); // this shoud fail
            Assert.IsTrue(d1s2.CheckDeploymentMembership(d1s1).Result); // this shoud succeed
            Assert.IsTrue(d1s2.CheckDeploymentMembership(d1s2).Result); // this shoud succeed

            //Assert.IsTrue(d1s1.CheckDeploymentMembership(d1s1).Result);

            //d1s1.SendMessage(d1s2, "this message should go through");

            //d1s1.SendMessage(d2s1, "this message should not go through");

            // the test will check whether d1s1 can only send messages to d1s2, and not to d2s1
        }

        //[TestMethod]
        public void GeodistributedGrainRegistrationTestCase1()
        {
            // for this test case, we will create one grain, and three test deployments (d0, d1, d2). One deployment d0 will send a register request, 
            // the other two deployments are "not interested". 
            // So d0 will broadcast "reg(g,d0)", and d1 and d2 will reply "not interested". 
            var grains = CreateTestGrains(1); //  technically we should not init a grain, since we are testing grain registration
            var deployments = CreateTestDeployments(3);
            //var result = deployments.ElementAt(0).SendBroadcastMessage(grains[0], "register"); // this unit test is now calling the SendBroadcastMessage method, so this is a message passed from
                                                                                                         // the unit test to the deployment grain 
            var result = deployments[0].Register(grains[0]);
            if (result.IsCanceled)
            {
                const string error = "task canceled";
                Assert.AreEqual(error, "task canceled");
            }
            else
            {
                //string s = result.Result;
                Assert.AreEqual(result.Result, "registration failed");
            }
                
        }

        //[TestMethod]
        public void GeodistributedGrainRegistrationTestCase2()
        {
            // for this test case, we want to force a race condition where two deployments simultaneously try to register the same grain
            var grains = CreateTestGrains(1);
            var deployments = CreateTestDeployments(3);
            
            foreach (var t in deployments)
            {
                t.Register(grains[0]);
            }
        }



        private static List<IDeploymentGrain> CreateTestDeployments(int n)
        {
            var deployments = new List<IDeploymentGrain>();
            var testDeployments = new List<ITestDeploymentGrain>();
            var listOfDeploymentGuid = new List<Guid>();
            for (int i = 0; i < n; i++)
            {
                var g = Guid.NewGuid();
                listOfDeploymentGuid.Add(g);
                var dg = DeploymentGrainFactory.GetGrain(g);
                var tdg = TestDeploymentGrainFactory.GetGrain(g);
                tdg.LoadDeployment(i, 3); // for each deployment, init 3 silos
                deployments.Add(dg);
                testDeployments.Add(tdg);
                
            }
            for (int i = 0; i < n; i++ )
            {
                var test = TestDeploymentGrainFactory.GetGrain(listOfDeploymentGuid[i]);
                test.SetListOfDeployments(deployments);
            }
            return deployments;
        }


        private static List<int> CreateTestGrains(int n)
        {
            var grains = new List<int>();
            for (int i = 0; i < n; i++)
            {
                grains.Add(i);
            }
            return grains;
        }

    }
}
