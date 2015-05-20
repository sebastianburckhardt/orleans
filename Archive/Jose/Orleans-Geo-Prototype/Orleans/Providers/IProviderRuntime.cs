using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Orleans.Providers
{
    /// <summary>
    /// Interface to allow callbacks from providers into their assigned provider-manager.
    /// This allows access to runtime functionality, such as logging.
    /// </summary>
    /// <remarks>
    /// Passed to the provider during IOrleansProvider.Init call to that provider instance.
    /// </remarks>
    /// <seealso cref="IOrleansProvider"/>
    public interface IProviderRuntime
    {
        /// <summary>
        /// Provides a logger to be used by the provider. 
        /// </summary>
        /// <param name="loggerName">Name of the logger being requested.</param>
        /// <param name="logType">Type of the logger being requested.</param>
        /// <returns>Object reference to the requested logger.</returns>
        /// <seealso cref="Logger.LoggerType"/>
        OrleansLogger GetLogger(string loggerName, Logger.LoggerType logType);
    }

    /// <summary>
    /// Provider-facing interface for manager of storage providers
    /// </summary>
    public interface IStorageProviderRuntime : IProviderRuntime
    {
        // for now empty, later can add storage specific runtime capabilities.
    }
}