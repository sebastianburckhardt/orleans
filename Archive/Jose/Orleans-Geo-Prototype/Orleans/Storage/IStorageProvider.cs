using System;
using System.Runtime.Serialization;
using System.Threading.Tasks;

using Orleans.Providers;

namespace Orleans.Storage
{
    /// <summary>
    /// Interface to be implemented for a storage provider able to read and write Orleans grain state data.
    /// </summary>
    public interface IStorageProvider : IOrleansProvider
    {
        /// <summary>Logger used by this storage provider instance.</summary>
        /// <returns>Reference to the Logger object used by this provider.</returns>
        /// <seealso cref="OrleansLogger"/>
        OrleansLogger Log { get; }
        
        /// <summary>Close function for this storage provider instance.</summary>
        /// <returns>Completion promise for the Close operation on this provider.</returns>
        Task Close();
        
        /// <summary>Read data function for this storage provider instance.</summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <param name="grainState">State data object to be populated for this grain.</param>
        /// <returns>Completion promise for the Read operation on the specified grain.</returns>
        Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState);

        /// <summary>Write data function for this storage provider instance.</summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <param name="grainState">State data object to be written for this grain.</param>
        /// <returns>Completion promise for the Write operation on the specified grain.</returns>
        Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState);
        
        /// <summary>Delete / Clear data function for this storage provider instance.</summary>
        /// <param name="grainType">Type of this grain [fully qualified class name]</param>
        /// <param name="grainReference">Grain reference object for this grain.</param>
        /// <param name="grainState">Copy of last-known state data object for this grain.</param>
        /// <returns>Completion promise for the Delete operation on the specified grain.</returns>
        Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState);
    }

    /// <summary>
    /// Exception thrown whenever a grain call is attempted with a bad / missing storage provider configuration settings for that grain.
    /// </summary>
    [Serializable]
    public class BadProviderConfigException : OrleansException
    {
        public BadProviderConfigException()
        {}
        public BadProviderConfigException(string msg) 
            : base(msg)
        { }
        public BadProviderConfigException(string msg, Exception exc)
            : base(msg, exc)
        { }
        protected BadProviderConfigException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        { }
    }

    /// <summary>
    /// Exception thrown when a storage provider detects an Etag inconsistency when attemptiong to perform a WriteStateAsync operation.
    /// </summary>
    [Serializable]
    public class InconsistentStateException : OrleansException
    {
        /// <summary>The Etag value currently held in persistent storage.</summary>
        public string StoredEtag { get; private set; }

        /// <summary>The Etag value currently help in memory, and attempting to be updated.</summary>
        public string CurrentEtag { get; private set; }

        public InconsistentStateException()
        {}
        public InconsistentStateException(string msg) 
            : base(msg)
        { }
        public InconsistentStateException(string msg, Exception exc)
            : base(msg, exc)
        { }
        protected InconsistentStateException(SerializationInfo info, StreamingContext context)
            : base(info, context)
        {}

        public InconsistentStateException(
          string errorMsg,
          string storedEtag,
          string currentEtag,
          Exception storageException
        ) : base(errorMsg, storageException)
        {
            this.StoredEtag = storedEtag;
            this.CurrentEtag = currentEtag;
        }

        public InconsistentStateException(string storedEtag, string currentEtag, Exception storageException)
            : this(storageException.Message, storedEtag, currentEtag, storageException)
        { }
    }
}
