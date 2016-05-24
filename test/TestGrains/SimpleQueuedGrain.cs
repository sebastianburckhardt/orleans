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
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;
using Orleans.Providers;
using Orleans.QueuedGrains;
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
    public abstract class SimpleQueuedGrain : QueuedGrainWithDynamicApply<MyGrainState>, ISimpleQueuedGrain
    {
        public async Task SetAGlobal(int x)
        {
            EnqueueUpdate(new UpdateA() { Val = x });
            await ConfirmUpdates();
        }

        public async Task<Tuple<int, bool>> SetAConditional(int x)
        {
            int version = this.ConfirmedVersion;
            bool success = await TryConditionalUpdateAsync(new UpdateA() { Val = x });
            return new Tuple<int, bool>(version, success);
        }

        public Task SetALocal(int x)
        {
            EnqueueUpdate(new UpdateA() { Val = x });
            return TaskDone.Done;
        }
        public async Task SetBGlobal(int x)
        {
            EnqueueUpdate(new UpdateB() { Val = x });
            await ConfirmUpdates();
        }

        public Task SetBLocal(int x)
        {
            EnqueueUpdate(new UpdateB() { Val = x });
            return TaskDone.Done;
        }

        public async Task IncrementAGlobal()
        {
            EnqueueUpdate(new IncrementA());
            await ConfirmUpdates();
        }

        public Task IncrementALocal()
        {
            EnqueueUpdate(new IncrementA());
            return TaskDone.Done;

        }

        public async Task<int> GetAGlobal()
        {
            await SynchronizeNowAsync();
            return ConfirmedState.A;
        }

        public Task<int> GetALocal()
        {
            return Task.FromResult(TentativeState.A);
        }

        public async Task<AB> GetBothGlobal()
        {
            await SynchronizeNowAsync();
            return new AB() { A = ConfirmedState.A, B = ConfirmedState.B };
        }

        public Task<AB> GetBothLocal()
        {
            var state = TentativeState;
            return Task.FromResult(new AB() { A = state.A, B = state.B });
        }

        public Task AddReservationLocal(int val)
        {
            EnqueueUpdate(new AddReservation() { Val = val });
            return TaskDone.Done;

        }
        public Task RemoveReservationLocal(int val)
        {
            EnqueueUpdate(new RemoveReservation() { Val = val });
            return TaskDone.Done;

        }
        public async Task<int[]> GetReservationsGlobal()
        {
            await SynchronizeNowAsync();
            return ConfirmedState.Reservations.Values.ToArray();
        }

        public Task SynchronizeGlobalState()
        {
            return SynchronizeNowAsync();
        }

        public Task<int> GetConfirmedVersion()
        {
            return Task.FromResult(this.ConfirmedVersion);
        }

        public async Task<KeyValuePair<int, object>> Read()
        {
            await SynchronizeNowAsync();
            return new KeyValuePair<int, object>(ConfirmedVersion, ConfirmedState);
        }
        public async Task<bool> Update(IReadOnlyList<object> updates, int expectedversion)
        {
            if (expectedversion > ConfirmedVersion)
                await SynchronizeNowAsync();
            if (expectedversion != ConfirmedVersion)
                return false;
            return await TryConditionalUpdateSequenceAsync(updates);
        }

        public Task Deactivate()
        {
            DeactivateOnIdle();
            return TaskDone.Done;
        }

    }
}