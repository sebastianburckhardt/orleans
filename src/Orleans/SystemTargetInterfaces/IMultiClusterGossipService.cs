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

using Orleans.MultiCluster;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    internal interface IMultiClusterGossipService : ISystemTarget
    {
        /// <summary>
        /// One-way small-scale gossip: send partial data to recipient
        /// </summary>
        /// <param name="data">The gossip data</param>
        /// <param name="forwardlocally">Whether to forward the changes to local silos</param>
        /// <returns></returns>
        Task Push(IMultiClusterGossipData GossipData, bool forwardlocally);

        /// <summary>
        /// Two-way bulk gossip: send all known data to recipient, and receive all unknown data
        /// </summary>
        /// <param name="data">The pushed gossip data</param>
        /// <returns>The returned gossip data</returns>
        Task<IMultiClusterGossipData> PushAndPull(IMultiClusterGossipData data);

        /// <summary>
        /// Find silos whose configuration does not match the expected configuration.
        /// </summary>
        /// <param name="expected">the configuration to compare with</param>
        /// <param name="forwardlocally">whether to recursively include all silos in the same cluster</param>
        /// <returns></returns>
        Task<Dictionary<SiloAddress,MultiClusterConfiguration>> FindUnstableSilos(MultiClusterConfiguration expected, bool forwardlocally);
    }


    // placeholder interface for gossip data. Actual implementation is in Orleans.Runtime.
    internal interface IMultiClusterGossipData { }
}
