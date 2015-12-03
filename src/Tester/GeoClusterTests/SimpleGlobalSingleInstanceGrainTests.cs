/*
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

using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.TestingHost;
using TestGrainInterfaces;
using UnitTests.Tester;

namespace Tester.GeoClusterTests
{
    [TestClass]
    [DeploymentItem("TestGrainInterfaces.dll")]
    [DeploymentItem("TestGrains.dll")]
    [DeploymentItem("OrleansAzureUtils.dll")]
    public class SimpleGlobalSingleInstanceGrainTests : UnitTestSiloHost
    {
        private const string SimpleGrainNamePrefix = "UnitTests.Grains.SimpleG";

        public SimpleGlobalSingleInstanceGrainTests()
            : base(new TestingSiloOptions { StartPrimary = true, StartSecondary = true })
        {
            StartAdditionalSilos(3);
        }

        public ISimpleGlobalSingleInstanceGrain GetGlobalSingleInstanceGrain()
        {
            return GrainFactory.GetGrain<ISimpleGlobalSingleInstanceGrain>(GetRandomGrainId(), SimpleGrainNamePrefix);
        }

        private static int GetRandomGrainId()
        {
            return random.Next();
        }


        [TestCleanup]
        public void TestCleanup()
        {
            StopAllSilos();
        }

        [TestMethod, TestCategory("BVT"), TestCategory("Functional"), TestCategory("GeoCluster"), TestCategory("Azure")]
        public async Task GlobalSingleInstanceGrainTest()
        {
            int i = 0;
            while (i++ < 100)
            {
                ISimpleGlobalSingleInstanceGrain grain = GetGlobalSingleInstanceGrain();
                int r1 = random.Next(0, 100);
                int r2 = random.Next(0, 100);
                await grain.SetA(r1);
                await grain.SetB(r2);
                int result = await grain.GetAxB();
                Assert.AreEqual(r1 * r2, result);
            }
        }
    }
}
