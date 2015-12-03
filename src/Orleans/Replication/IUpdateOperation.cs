using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Replication
{
    /// <summary>
    /// Interface for objects that represent an update of a state object.
    /// Updates must be unconditional and behave reasonably on all possible states. 
    /// </summary>
    /// <typeparam name="StateObject"></typeparam>
    public interface IUpdateOperation<StateObject> 
    {
       /// <summary>
       /// Updates the state according to the operation represented by this object.
       /// By convention, all exceptions thrown are ignored.
       /// </summary>
       /// <param name="state"></param>
        void Update(StateObject state);
    }
}
