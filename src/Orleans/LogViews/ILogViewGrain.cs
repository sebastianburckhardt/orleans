using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Concurrency;
using Orleans.Core;
using Orleans.Runtime;

namespace Orleans.LogViews
{
    /// <summary>
    ///  This interface encapsulates functionality of grains that want to host a log view adaptor.
    ///  It is the equivalent of <see cref="IStatefulGrain"/> for grains using log-view persistence.
    /// </summary>
    public interface ILogViewGrain 
    {
        /// <summary>
        /// called right after grain construction to install the log view adaptor 
        /// </summary>
        /// <param name="provider"> The log view provider to install </param>
        /// <param name="state"> The initial state of the view </param>
        /// <param name="graintypename"> The type name of the grain </param>
        /// <param name="services"> Protocol services </param>
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
        public LogViewGrainBase()
        { }

        /// <summary>
        /// Grain implementers do NOT have to expose this constructor but can choose to do so.
        /// This constructor is particularly useful for unit testing where test code can create a Grain and replace
        /// the IGrainIdentity and IGrainRuntime with test doubles (mocks/stubs).
        /// </summary>
        /// <param name="identity"></param>
        /// <param name="runtime"></param>
        public LogViewGrainBase(IGrainIdentity identity, IGrainRuntime runtime) : base(identity, runtime)
        { }
    }

}
