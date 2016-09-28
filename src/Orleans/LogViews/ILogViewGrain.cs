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
    ///  It is the equivalent of <see cref="IStatefulGrain"/> for grains using log-view persistence.
    /// </summary>
    public interface ILogViewGrain 
    {
        // called right after grain construction to install the log view adaptor 
        void InstallAdaptor(ILogViewProvider provider, object state, string graintypename, IProtocolServices services);
    }


    /// <summary>
    /// Base class for all grains using a log view provider.
    /// It is the equivalent of <see cref="Grain{T}"/> for grains using log-view persistence.
    /// (SiloAssemblyLoader uses it to extract type)
    /// </summary>
    /// <typeparam name="TView">The type of the view</typeparam>
    public class LogViewGrainBase<TView> : Grain
    {
    }
}
