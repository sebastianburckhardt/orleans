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
using System.Threading.Tasks;
using HelloGeoInterfaces;
using Orleans.Runtime;
using Microsoft.Azure;
using Orleans.MultiCluster;

namespace HelloGeoGrains
{

    /// <summary>
    /// A regular implementation of the IHelloEnvironment grain interface.
    /// </summary>
    public class RegularGrain : Orleans.Grain, IHelloEnvironment
    {
        Task<string> IHelloEnvironment.RequestDetails()
        {
            // check the cloud configuration to find out what deployment we are in
            var clusterid = CloudConfigurationManager.GetSetting("ClusterId");

            return Task.FromResult(String.Format("ClusterId \"{0}\"\n{1} - {2} - {3} Processors", clusterid, Environment.MachineName, Environment.OSVersion, Environment.ProcessorCount));
        }
    }


    /// <summary>
    /// A global-single-instance implementation of the IHelloEnvironment grain interface.
    /// </summary>
    [GlobalSingleInstance]
    public class SingleInstanceGrainGrain : Orleans.Grain, IHelloEnvironment
    {
        Task<string> IHelloEnvironment.RequestDetails()
        {
            // check the cloud configuration to find out what deployment we are in
            var clusterid = CloudConfigurationManager.GetSetting("ClusterId");

            return Task.FromResult(String.Format("ClusterId \"{0}\"\n{1} - {2} - {3} Processors", clusterid, Environment.MachineName, Environment.OSVersion, Environment.ProcessorCount));
        }
    }

}
