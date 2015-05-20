using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces;


namespace UnitTestGrains
{
    internal class CollectionTestGrain : GrainBase, ICollectionTestGrain
    {
        private DateTime activated;

        private ICollectionTestGrain other;

        public override Task ActivateAsync()
        {
            GetLogger("CollectionTestGrain").Info("Activate {0} hash code {1:x} on {2}", Identity, Identity.GetHashCode(), RuntimeIdentity);
            activated = DateTime.UtcNow;
            return TaskDone.Done;
        }

        public override Task DeactivateAsync()
        {
            GetLogger("CollectionTestGrain").Info("Deactivate " + Identity + " on " + RuntimeIdentity);
            return TaskDone.Done;
        }

        #region Implementation of ICollectionTestGrain

        public Task<TimeSpan> GetAge()
        {
            GetLogger("CollectionTestGrain").Info("GetAge " + Identity + " on " + RuntimeIdentity);
            return Task.FromResult(DateTime.UtcNow.Subtract(activated));
        }

        public Task DeactivateSelf()
        {
            GetLogger("CollectionTestGrain").Info("DeactivateSelf " + Identity + " on " + RuntimeIdentity);
            DeactivateOnIdle();
            return TaskDone.Done;
        }

        public Task SetOther(ICollectionTestGrain other)
        {
            this.other = other;
            return TaskDone.Done;
        }

        public Task<TimeSpan> GetOtherAge()
        {
            return other.GetAge();
        }

        public Task<string> GetRuntimeInstanceId()
        {
            return Task.FromResult(this.RuntimeIdentity);
        }

        public Task<GrainId> GetGrainId()
        {
            return Task.FromResult(Identity);
        }

        #endregion
    }
}
