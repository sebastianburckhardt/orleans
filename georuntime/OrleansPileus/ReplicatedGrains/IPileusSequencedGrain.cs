using System;
namespace OrleansPileus.ReplicatedGrains
{
    interface IPileusSequencedGrain<StateObject>
     where StateObject : class, new()
    {
        void setSynchronous(bool pSynchronous);
        System.Threading.Tasks.Task UpdateLocallyAsync(IAppliesTo<StateObject> update, bool save);
    }
}
