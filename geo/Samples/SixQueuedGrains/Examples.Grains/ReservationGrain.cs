
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.Replication;



namespace Examples.Grains
{

    /// <summary>
    /// The state of the reservation grain
    /// </summary>
    [Serializable]
    public class ReservationState : GrainState
    {
        public Dictionary<int, string> Reservations { get; set; }

        public ReservationState()
        {
            Reservations = new Dictionary<int,string>();
        }
    }

    /// <summary>
    /// The class that defines the update operation when a reservation is requested
    /// </summary>
    [Serializable]
    public class ReservationRequest : IUpdateOperation<ReservationState>
    {
      
        public int Seat { get; set; }
        public string UserId { get; set; }

        public void Update(ReservationState state)
        {
            // insert a reservation, but only if the seat is still free
            // this is a "first writer wins" conflict resolution
            if (!state.Reservations.ContainsKey(Seat))
                state.Reservations.Add(Seat, UserId);
        }
    }

     

    /// <summary>
    /// The grain implementation
    /// </summary>
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class ReservationGrain : QueuedGrain<ReservationState>, IReservationGrain
    {
        public async Task<bool> Reserve(int seatnumber, string userid)
        {
            // first, enqueue the request
            EnqueueUpdate(new ReservationRequest() { Seat = seatnumber, UserId = userid });

            // then, wait for the request to propagate
            await CurrentQueueHasDrained();

            // check if the reservation went through
            var success = (ConfirmedState.Reservations.ContainsKey(seatnumber)
                                 && ConfirmedState.Reservations[seatnumber] == userid);
            return success;
        }
    }


}

