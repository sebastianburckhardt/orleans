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

using System;
using System.Linq;
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Orleans.Runtime;
using Orleans.Runtime.MultiClusterNetwork;
using Orleans.MultiCluster;
using Orleans.Runtime.MembershipService;
using UpdateFaultCombo = Orleans.Runtime.MembershipService.MembershipOracleData.UpdateFaultCombo;
using System.Collections.Generic;

namespace UnitTests.GeoClusterTests
{
    /// <summary>
    /// Test selection algorithm for multi-cluster gateways
    /// </summary>
    [TestClass]
    public class GatewaySelectionTests
    {
     
        [TestMethod, TestCategory("GeoCluster"), TestCategory("Functional")]
        public void TestMultiClusterGatewaySelection()
        {
            var candidates = new SiloAddress[] {
                SiloAddress.New(new IPEndPoint(0,0),0),
                SiloAddress.New(new IPEndPoint(0,0),1),
                SiloAddress.New(new IPEndPoint(0,1),0),
                SiloAddress.New(new IPEndPoint(0,1),1),
                SiloAddress.New(new IPEndPoint(0,234),1),
                SiloAddress.New(new IPEndPoint(1,0),0),
                SiloAddress.New(new IPEndPoint(1,0),1),
                SiloAddress.New(new IPEndPoint(1,1),1),
                SiloAddress.New(new IPEndPoint(1,234),1),
                SiloAddress.New(new IPEndPoint(2,234),1),
                SiloAddress.New(new IPEndPoint(3,234),1),
                SiloAddress.New(new IPEndPoint(4,234),1),
            };
            
            Func<SiloAddress,UpdateFaultCombo> group = (a) => new UpdateFaultCombo(a.Endpoint.Port, a.Generation);

            // randomize order
            var r = new Random();
            var randomized = new SortedList<int,SiloAddress> ();
            foreach(var c in candidates)
                randomized.Add(r.Next(), c);

            var x = MembershipOracleData.DeterministicBalancedChoice(randomized.Values, 10, group);

            for (int i = 0; i < 10; i++)
                Assert.AreEqual(candidates[i], x[i]);
        }

    }
}
