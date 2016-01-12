using Orleans.Runtime;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    public interface ILogViewHost<TLogView,TLogEntry>  
    {
        // transition function for this view
        void TransitionView(TLogView view, TLogEntry entry);

        // identity of this host (for logging purposes only)
        string IdentityString { get; }

    }


  
   

 
}
