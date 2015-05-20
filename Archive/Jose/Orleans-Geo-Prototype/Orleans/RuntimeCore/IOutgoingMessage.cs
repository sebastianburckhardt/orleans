using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace Orleans
{
    // Used for Client -> GW and Silo <-> Silo messasing
    internal interface IOutgoingMessage
    {
        bool IsSameDestination(IOutgoingMessage other);
    }

    // Used for GW -> Client messaging
    internal class OutgoingClientMessage : Tuple<Guid, Message>, IOutgoingMessage
    {
        public OutgoingClientMessage(Guid clientGuid, Message message) : base(clientGuid, message) { }

        public bool IsSameDestination(IOutgoingMessage other)
        {
            OutgoingClientMessage otherTuple = (OutgoingClientMessage)other;
            if (otherTuple == null) return false;
            return this.Item1.Equals(otherTuple.Item1);
            //return this.Item2.IsSameDestination(otherTuple.Item2);
        }
    }
}
