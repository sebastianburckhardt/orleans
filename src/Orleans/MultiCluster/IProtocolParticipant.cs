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
using Orleans.Concurrency;


namespace Orleans.MultiCluster
{
    /// <summary>
    /// Grain interface for for grains that participate in multi-cluster-protocols.
    /// </summary>
    public interface IProtocolParticipant  : IGrain  
    {
        /// <summary>
        /// Called when a message is received from another replica.
        /// This MUST interleave with other calls to avoid deadlocks.
        /// </summary>
        /// <param name="payload"></param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task<IProtocolMessage> OnProtocolMessageReceived(IProtocolMessage payload);

        /// <summary>
        /// Called when a configuration change notification is received.
        /// </summary>
        /// <param name="prev"></param>
        /// <param name="current"></param>
        /// <returns></returns>
        [AlwaysInterleave]
        Task OnMultiClusterConfigurationChange(MultiClusterConfiguration next);


        /// <summary>
        /// Called immediately before the user-level OnActivateAsync, on same scheduler
        /// </summary>
        /// <returns></returns>
        Task ActivateProtocolParticipant();

        /// <summary>
        /// Called immediately after the user-level OnDeactivateAsync, on same scheduler
        /// </summary>
        /// <returns></returns>
        Task DeactivateProtocolParticipant();
    }

    /// <summary>
    /// interface to mark classes that represent protocol messages
    /// </summary>
    public interface IProtocolMessage
    {
    }
}
