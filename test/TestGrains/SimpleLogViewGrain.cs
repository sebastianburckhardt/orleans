using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.LogViews;
using UnitTests.GrainInterfaces;

namespace UnitTests.Grains
{


    [Serializable]
    public class MyGrainState
    {
        public int A;
        public int B;
        public Dictionary<String, int> Reservations;

        public MyGrainState()
        {
            Reservations = new Dictionary<string, int>();
        }

        public override string ToString()
        {
            return string.Format("A={0} B={1} R={{{2}}}", A, B, string.Join(", ", Reservations.Select(kvp => string.Format("{0}:{1}", kvp.Key, kvp.Value))));
        }

        // dynamic dispatch to the cases listed below
        public void Apply(dynamic o) { Apply(o); }

        // all the update operations are listed here
        public void Apply(UpdateA x) { A = x.Val; }
        public void Apply(UpdateB x) { B = x.Val; }
        public void Apply(IncrementA x) { A++; }

        public void Apply(AddReservation x) { Reservations[x.Val.ToString()] = x.Val; }
        public void Apply(RemoveReservation x) { Reservations.Remove(x.Val.ToString()); }
    }
 

    [Serializable]
    public class UpdateA { public int Val; }
    [Serializable]
    public class UpdateB  { public int Val; }
    [Serializable]
    public class IncrementA  { public int Val; }
    [Serializable]
    public class AddReservation { public int Val; }
    [Serializable]
    public class RemoveReservation { public int Val; }



    /// <summary>
    /// A simple grain with two fields A, B that can be updated or incremented
    /// We subclass this to create variations for all storage providers
    /// </summary>
    public abstract class SimpleLogViewGrain : LogViewGrain<MyGrainState,object>, ISimpleLogViewGrain
    {
        protected override void UpdateView(MyGrainState state, object delta)
        {
            state.Apply(delta);
        }

        public async Task SetAGlobal(int x)
        {
            Submit(new UpdateA() { Val = x });
            await ConfirmSubmittedEntriesAsync();
        }

        public async Task<Tuple<int, bool>> SetAConditional(int x)
        {
            int version = this.ConfirmedVersion;
            bool success = await TryAppend(new UpdateA() { Val = x });
            return new Tuple<int, bool>(version, success);
        }

        public Task SetALocal(int x)
        {
            Submit(new UpdateA() { Val = x });
            return TaskDone.Done;
        }
        public async Task SetBGlobal(int x)
        {
            Submit(new UpdateB() { Val = x });
            await ConfirmSubmittedEntriesAsync();
        }

        public Task SetBLocal(int x)
        {
            Submit(new UpdateB() { Val = x });
            return TaskDone.Done;
        }

        public async Task IncrementAGlobal()
        {
            Submit(new IncrementA());
            await ConfirmSubmittedEntriesAsync();
        }

        public Task IncrementALocal()
        {
            Submit(new IncrementA());
            return TaskDone.Done;

        }

        public async Task<int> GetAGlobal()
        {
            await SynchronizeNowAsync();
            return ConfirmedView.A;
        }

        public Task<int> GetALocal()
        {
            return Task.FromResult(TentativeView.A);
        }

        public async Task<AB> GetBothGlobal()
        {
            await SynchronizeNowAsync();
            return new AB() { A = ConfirmedView.A, B = ConfirmedView.B };
        }

        public Task<AB> GetBothLocal()
        {
            var state = TentativeView;
            return Task.FromResult(new AB() { A = state.A, B = state.B });
        }

        public Task AddReservationLocal(int val)
        {
            Submit(new AddReservation() { Val = val });
            return TaskDone.Done;

        }
        public Task RemoveReservationLocal(int val)
        {
            Submit(new RemoveReservation() { Val = val });
            return TaskDone.Done;

        }
        public async Task<int[]> GetReservationsGlobal()
        {
            await SynchronizeNowAsync();
            return ConfirmedView.Reservations.Values.ToArray();
        }

        public Task SynchronizeGlobalState()
        {
            return SynchronizeNowAsync();
        }

        public Task<int> GetConfirmedVersion()
        {
            return Task.FromResult(this.ConfirmedVersion);
        }

        public Task<Exception> GetLastException()
        {
            return Task.FromResult(LastException);
        }

        public async Task<KeyValuePair<int, object>> Read()
        {
            await SynchronizeNowAsync();
            return new KeyValuePair<int, object>(ConfirmedVersion, ConfirmedView);
        }
        public async Task<bool> Update(IReadOnlyList<object> updates, int expectedversion)
        {
            if (expectedversion > ConfirmedVersion)
                await SynchronizeNowAsync();
            if (expectedversion != ConfirmedVersion)
                return false;
            return await TryAppendRange(updates);
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return TaskDone.Done;
        }

    }
}