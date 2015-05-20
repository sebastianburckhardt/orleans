using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;

namespace Orleans
{
    // someday, this will be the only provider for the ring, i.e., directory service will use this

    internal interface IConsistentRingProvider
    {
        //void Start();
        //void Stop();

        /// <summary>
        /// Get the responsbility range of the current silo
        /// </summary>
        /// <returns></returns>
        IRingRange GetMyRange();

        // the following two are similar to the ISiloStatusOracle interface ... this replaces the 'OnRangeChanged' because OnRangeChanged only supports one subscription

        /// <summary>
        /// Subscribe to receive range change notifications
        /// </summary>
        /// <param name="observer">An observer interface to receive range change notifications.</param>
        /// <returns>bool value indicating that subscription succeeded or not.</returns>
        bool SubscribeToRangeChangeEvents(IRingRangeListener observer);

        /// <summary>
        /// Unsubscribe from receiving range change notifications
        /// </summary>
        /// <param name="observer">An observer interface to receive range change notifications.</param>
        /// <returns>bool value indicating that unsubscription succeeded or not</returns>
        bool UnSubscribeFromRangeChangeEvents(IRingRangeListener observer);

        /// <summary>
        /// Get the silo responsible for <paramref name="key"/> according to consistent hashing
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        SiloAddress GetPrimary(int key);

        /// <summary>
        /// Get <paramref name="n"/> successors of the current silo
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        List<SiloAddress> GetMySucessors(int n = 1);

        /// <summary>
        /// /// Get <paramref name="n"/> predecessors of the current silo
        /// </summary>
        /// <param name="n"></param>
        /// <returns></returns>
        List<SiloAddress> GetMyPredecessors(int n = 1);
    }

    // similar to ISiloStatusListener
    internal interface IRingRangeListener
    {
        void RangeChangeNotification(IRingRange old, IRingRange now, bool increased);
    }
}
