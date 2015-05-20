using System;
using System.Collections.Generic;
using System.Threading.Tasks;


namespace Orleans.Providers
{
    #pragma warning disable 1574
    /// <summary>
    /// Base interface for all type-specific provider interfaces in Orleans
    /// </summary>
    /// <seealso cref="Orleans.Providers.IBootstrapProvider"/>
    /// <seealso cref="Orleans.Storage.IStorageProvider"/>
    public interface IOrleansProvider
    {
        /// <summary>The name of this provider instance, as given to it in the config.</summary>
        string Name { get; }

        /// <summary>
        /// Initialization function called by Orleans Provider Manager when a new provider class instance  is created
        /// </summary>
        /// <param name="name">Name assigned for this provider</param>
        /// <param name="providerRuntime">Callback for accessing system functions in the Provider Runtime</param>
        /// <param name="config">Configuration metadata to be used for this provider instance</param>
        /// <returns>Completion promise Task for the inttialization work for this provider</returns>
        Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config);

        // For now, I've decided to keep Close in the per-provider interface and not as part of the common IOrleansProvider interface.
        // There is currently no central place where Close can / would be called. 
        // It might eventually be provided by xProviderManager classes in certain cases, 
        //  for example: if they detect silo shutdown in progress.

        //Task Close();
    }
    #pragma warning restore 1574

    /// <summary>
    /// Internal provider management interface for instantiating dependent providers in a hierarchical tree of dependencies
    /// </summary>
    internal interface IProviderManager
    {
        /// <summary>
        /// Call into Provider Manager for instantiating dependent providers in a hierarchical tree of dependencies
        /// </summary>
        /// <param name="name">Name of the provider to be found</param>
        /// <returns>Provider instance with the given name</returns>
        IOrleansProvider GetProvider(string name);
    }

    /// <summary>
    /// Configuration information that a provider receives
    /// </summary>
    public interface IProviderConfiguration
    {
        /// <summary>
        /// Configuration properties for this provider instance, as name-value pairs.
        /// </summary>
        Dictionary<string, string> Properties { get; }

        /// <summary>
        /// Nested providers in case of a hierarchical tree of dependencies
        /// </summary>
        List<IOrleansProvider> Children { get; }
    }
}