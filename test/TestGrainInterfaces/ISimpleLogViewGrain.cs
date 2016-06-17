using System;
using System.Threading.Tasks;
using Orleans;
using Orleans.LogViews;
using System.Collections.Generic;

namespace UnitTests.GrainInterfaces
{
    public interface ISimpleLogViewGrain: IGrainWithIntegerKey
    {
        #region Queries

        // read A

        Task<int> GetAGlobal();

        Task<int> GetALocal();

        // read both

        Task<AB> GetBothGlobal();

        Task<AB> GetBothLocal();

        // reservations

        Task<int[]> GetReservationsGlobal();

        // version

        Task<int> GetConfirmedVersion();

        // exception
        Task<Exception> GetLastException();

        #endregion


        #region Updates

        // set or increment A

        Task SetAGlobal(int a);

        Task<Tuple<int, bool>> SetAConditional(int a);

        Task SetALocal(int a);

        Task IncrementALocal();

        Task IncrementAGlobal();

        // set B

        Task SetBGlobal(int b);

        Task SetBLocal(int b);

        // reservations

        Task AddReservationLocal(int x);

        Task RemoveReservationLocal(int x);

        #endregion


        Task<KeyValuePair<int, object>> Read();
        Task<bool> Update(IReadOnlyList<object> updates, int expectedversion);

        #region Other

        // other operations

        Task SynchronizeGlobalState();
        Task Deactivate();

        #endregion
    }

    public struct AB
    {
        public int A;
        public int B;
    }
}
