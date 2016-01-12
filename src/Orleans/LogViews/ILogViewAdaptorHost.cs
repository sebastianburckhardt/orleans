using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Runtime;

namespace Orleans.LogViews
{
    /// <summary>
    ///  This interface encapsulates functionality of grains that want to host a log view adaptor.
    /// </summary>
    public interface ILogViewAdaptorHost 
    {
        // called right after grain construction to install the log view adaptor 
       void InstallAdaptor(ILogViewProvider provider, 
            object state, string graintypename, IProtocolServices services);

    }
}
