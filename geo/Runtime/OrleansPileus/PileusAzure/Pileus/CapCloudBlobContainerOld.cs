using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using System.Threading;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Similar to CloudBlobContainer except deals with CapCloudBlobs
    /// </summary>
    public class CapCloudBlobContainerOld
    {
        private ConsistencySLAEngine slaEngine;

        private Stopwatch watch;

        public string Name { get; internal set; }

        public ReplicaConfiguration Configuration { get { return configuration; } }

        private ReplicaConfiguration configuration;

        private CloudBlobContainer mainPrimaryContainer;

        /// <summary>
        /// Initializes a new instance of the <see cref="CapCloudStorageAccount"/> class using the specified
        /// storage accounts.
        /// </summary>
        /// <param name="engine">The SLA engine.</param>
        public CapCloudBlobContainerOld(
            string name,
            ConsistencySLAEngine engine, string clientName)
        {
            this.slaEngine = engine;
            this.Name = name;
            
            configuration= ClientRegistry.GetConfiguration(this.Name, false);

            //configuration can be null.
            //This means that no other thread has created one before (it exists neither in memory nor in the cloud (in configurationClient))
            if (configuration != null)
            {
                mainPrimaryContainer = ClientRegistry.GetMainPrimaryContainer(name);
            }
            watch = new Stopwatch();


            //enables a periodic uploading of the configuration.

            // uploadTask = new UploadConfigurationTask(clientName);
            //Task.Factory.StartNew(() => uploadTask.StartUploadConfigurationTask(this.Name));
        }

        private DateTimeOffset Timestamp(CloudBlobContainer container)
        {
            // get timestamp from metadata if present since primary and secondary will have different LastModified times for same version of container
            DateTimeOffset result;
            if (container.Metadata.ContainsKey("LastModifiedOnPrimary")) {
                result = DateTimeOffset.Parse(container.Metadata["LastModifiedOnPrimary"]);
            }
            else {
                result = container.Properties.LastModified ?? DateTimeOffset.MinValue;
            }
            return result;
        }

        /// <summary>
        /// Gets a reference to a blob in this container.
        /// </summary>
        /// <param name="blobName">The name of the blob.</param>
        /// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        /// <param name="options">An object that specifies any additional options for the request.</param>
        /// <returns>A reference to the blob.</returns>
        public ICloudBlob GetBlobReferenceFromServer(string blobName, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return new CapCloudBlob(blobName, configuration, slaEngine);
        }

        //#region CloudBlobContainer public methods

        ///// <summary>
        ///// Creates the container.
        ///// If the container already exists (i.e., a configuration is created for it), everything is removed, and initiliazed again.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="operationContext">An OperationContext object that represents the context for the current operation. This object
        ///// is used to track requests to the storage service, and to provide additional runtime information about the operation. </param>
        public void Create(BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            // watch.Start();
            mainPrimaryContainer.Create(requestOptions, operationContext);
            //mainPrimaryServerState.AddRtt(watch.ElapsedMilliseconds);
            //slaEngine.SessionState.RecordObjectWritten("C:" + mainPrimaryContainer.Name, Timestamp(mainPrimaryContainer), mainPrimaryServerState);
        }

        ///// <summary>
        ///// Begins an asynchronous operation to create a container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginCreate(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginCreate(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to create a container.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginCreate(BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginCreate(options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to create a container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        //public void EndCreate(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndCreate(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten("C:"+primaryContainer.Name, Timestamp(primaryContainer), primaryServer);
        //}

        /// <summary>
        /// Creates the container if it does not already exist.
        /// 
        /// </summary>
        /// <param name="options">An object that specifies any additional options for the request.</param>
        /// <returns><c>true</c> if the container did not already exist and was created; otherwise <c>false</c>.</returns>
        public bool CreateIfNotExists(string primaryServerName, string secondaryServerName, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        { 
            if (configuration != null)
            {
                //the container already exists.
                return false;
            }

            List<string> primaries = new List<string>();
            primaries.Add(primaryServerName);
            List<string> secondaries = new List<string>();
            secondaries.Add(secondaryServerName);
            this.configuration = new ReplicaConfiguration(this.Name, primaries, secondaries);
            mainPrimaryContainer = ClientRegistry.GetMainPrimaryContainer(Name);

            ServerState mainPrimaryServerState = this.slaEngine.Monitor.GetServerState(primaryServerName);

            watch.Start();
            bool created = mainPrimaryContainer.CreateIfNotExists(requestOptions, operationContext);
            mainPrimaryServerState.AddRtt(watch.ElapsedMilliseconds);
            if (created)
            {
                slaEngine.Session.RecordObjectWritten("C:" + primaryServerName, Timestamp(mainPrimaryContainer), mainPrimaryServerState);
            }

            return created;
        }

        /// <summary>
        /// Creates the container if it does not already exist.
        /// 
        /// TODO: fix this.  In case of having several primaries, we need to create a lease, and then create the container.
        /// </summary>
        /// <param name="options">An object that specifies any additional options for the request.</param>
        /// <returns><c>true</c> if the container did not already exist and was created; otherwise <c>false</c>.</returns>
        public bool CreateIfNotExists(List<CloudBlobContainer> primaryContainer, List<CloudBlobContainer> secondaryContainer, BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        {
            //if (configuration != null)
            //{
            //    //the container already exists.
            //    return false;
            //}

            //this.configuration = new CapCloudBlobContainerConfiguration(this.Name, primaryContainer, secondaryContainer);
            //mainPrimaryContainer = configuration.MainPrimaryContainer.Container;
            //mainPrimaryServerState = this.slaEngine.SessionState.GetServerState(configuration.MainPrimaryContainer.Name);

            //watch.Start();
            //bool created = mainPrimaryContainer.CreateIfNotExists(requestOptions, operationContext);
            //mainPrimaryServerState.AddRtt(watch.ElapsedMilliseconds);
            //if (created)
            //{
            //    slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, Timestamp(primaryContainer), mainPrimaryServerState);
            //}
            //return created;
            return false;
        }

        ///// <summary>
        ///// Begins an asynchronous request to create the container if it does not already exist.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginCreateIfNotExists(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginCreateIfNotExists(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to create the container if it does not already exist.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginCreateIfNotExists(BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginCreateIfNotExists(options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Returns the result of an asynchronous request to create the container if it does not already exist.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns><c>true</c> if the container did not already exist and was created; otherwise, <c>false</c>.</returns>
        //public bool EndCreateIfNotExists(IAsyncResult asyncResult)
        //{
        //    bool created = primaryContainer.EndCreateIfNotExists(asyncResult);
        //    if (created)
        //    {
        //        slaEngine.SessionState.RecordObjectWritten("C:"+primaryContainer.Name, Timestamp(primaryContainer), primaryServer);
        //    }
        //    return created;
        //}

        ///// <summary>
        ///// Deletes the container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        public void Delete(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            // WARNING: Don't we delete at the back???
            // watch.Start();
            mainPrimaryContainer.Delete(accessCondition, options, operationContext);
            // primaryServer.AddRtt(watch.ElapsedMilliseconds);
            // deleted object no longer has a LastWritten property; 
            // record current time as its timestamp to force future reads to fail at primary.
            // slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, DateTimeOffset.Now, primaryServer);
        }

        ///// <summary>
        ///// Begins an asynchronous operation to delete a container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginDelete(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginDelete(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to delete a container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginDelete(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginDelete(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to delete a container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        //public void EndDelete(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndDelete(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, DateTimeOffset.Now, primaryServer);
        //}

        /// <summary>
        /// Deletes the container if it already exists.
        /// </summary>
        /// <param name="options">An object that specifies any additional options for the request.</param>
        /// <returns><c>true</c> if the container did not already exist and was created; otherwise <c>false</c>.</returns>
        public bool DeleteIfExists(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            if (configuration == null)
                return false;

            if (configuration.IsInFastMode())
            {
                return _DeleteIfExists(accessCondition, options, operationContext);
            }
            else
            {
                using (CloudBlobLease lease = new CloudBlobLease(configuration.Name, LeaseTakingPolicy.TryUntilSuccessful))
                {
                    if (lease.HasLease)
                    {
                        configuration.SyncWithCloud(ClientRegistry.GetConfigurationAccount());
                        return _DeleteIfExists(accessCondition, options, operationContext);
                    }
                    else
                    {
                        throw new OperationCanceledException("Cannot delete the container.");
                    }
                }
            }
        }

        private bool _DeleteIfExists(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            bool done = false;
            while (!done)
            {
                Dictionary<CloudBlobContainer, IAsyncResult> results = new Dictionary<CloudBlobContainer, IAsyncResult>();
                foreach (string serverName in configuration.PrimaryServers.Union(configuration.SecondaryServers))
                {
                    watch.Start();
                    CloudBlobContainer container = ClientRegistry.GetCloudBlobContainer(serverName, this.Name);
                    results[container] = container.BeginDeleteIfExists(accessCondition, options, operationContext, null, null);
                    ServerState ss = slaEngine.Monitor.GetServerState(serverName);
                    //ss.AddRtt(watch.ElapsedMilliseconds);
                    //slaEngine.SessionState.RecordObjectWritten(this.Name, Timestamp(container), ss);
                }

                foreach (CloudBlobContainer container in results.Keys)
                {
                    container.EndDeleteIfExists(results[container]);
                }

                ClientRegistry.RemoveConfiguration(configuration);
                configuration = null;

                done = true;
            }
            return true;
        }

        ///// <summary>
        ///// Begins an asynchronous request to delete the container if it already exists.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginDeleteIfExists(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginDeleteIfExists(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to delete the container if it already exists.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginDeleteIfExists(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginDeleteIfExists(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Returns the result of an asynchronous request to delete the container if it already exists.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns><c>true</c> if the container did not already exist and was created; otherwise, <c>false</c>.</returns>
        //public bool EndDeleteIfExists(IAsyncResult asyncResult)
        //{
        //    bool deleted = primaryContainer.EndDeleteIfExists(asyncResult);
        //    slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, DateTimeOffset.Now, primaryServer);
        //    return deleted;
        //}



        ///// <summary>
        ///// Begins an asynchronous operation to get a reference to a blob in this container.
        ///// </summary>
        ///// <param name="blobName">The name of the blob.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginGetBlobReferenceFromServer(string blobName, AsyncCallback callback, object state)
        //{
        //    // Note: It is difficult to get both blob references asynchronously.
        //    // So, we get the primary handle now, and later get the secondary one in a synchronous call.
        //    return primaryContainer.BeginGetBlobReferenceFromServer(blobName, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to get a reference to a blob in this container.
        ///// </summary>
        ///// <param name="blobName">The name of the blob.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginGetBlobReferenceFromServer(string blobName, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginGetBlobReferenceFromServer(blobName, accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to get a reference to a blob in this container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns>A reference to the blob.</returns>
        //public CapCloudBlob EndGetBlobReferenceFromServer(IAsyncResult asyncResult)
        //{
        //    ICloudBlob primaryBlob = primaryContainer.EndGetBlobReferenceFromServer(asyncResult);
        //    // TODO: remember the AccessConditions, etc. that were used when fetching the primary blob and pass them here.
        //    ICloudBlob secondaryBlob = secondaryContainer.GetBlobReferenceFromServer(primaryBlob.Name, null, null, null);
        //    return new CapCloudBlob(primaryBlob, secondaryBlob, slaEngine);
        //}

        ///// <summary>
        ///// Returns an enumerable collection of the blobs in the container that are retrieved lazily.
        ///// </summary>
        ///// <param name="useFlatBlobListing">Whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        ///// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <returns>An enumerable collection of objects that implement <see cref="IListBlobItem"/> and are retrieved lazily.</returns>
        public IEnumerable<IListBlobItem> ListBlobs(string prefix = null, bool useFlatBlobListing = false, BlobListingDetails blobListingDetails = BlobListingDetails.None, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            //IEnumerable<IListBlobItem> result;
            //ServerState ss = slaEngine.FindServerToRead("C:" + primaryContainer.Name);
            //CloudBlobContainer container = (ss.IsPrimary) ? primaryContainer : secondaryContainer;

            //watch.Start();
            //result = container.ListBlobs(prefix, useFlatBlobListing, blobListingDetails, options, operationContext);
            //ss.AddRtt(watch.ElapsedMilliseconds);
            //slaEngine.SessionState.RecordObjectRead("C:" + container.Name, Timestamp(container), ss);
            
            // WARNING: check if we need to update session state
            return mainPrimaryContainer.ListBlobs(prefix, useFlatBlobListing, blobListingDetails, options, operationContext);
        }

        ///// <summary>
        ///// Returns a result segment containing a collection of blob items 
        ///// in the container.
        ///// </summary>
        ///// <returns>A result segment containing objects that implement <see cref="IListBlobItem"/>.</returns>
        //public BlobResultSegment ListBlobsSegmented(BlobContinuationToken currentToken)
        //{
        //    BlobResultSegment result;
        //    ServerState ss = slaEngine.FindServerToRead("C:" + primaryContainer.Name);
        //    CloudBlobContainer container = (ss.IsPrimary) ? primaryContainer : secondaryContainer;
            
        //    watch.Start();
        //    result = container.ListBlobsSegmented(currentToken);
        //    ss.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead("C:" + container.Name, Timestamp(container), ss);
        //    return result;
        //}

        ///// <summary>
        ///// Returns a result segment containing a collection of blob items 
        ///// in the container.
        ///// </summary>
        ///// <param name="useFlatBlobListing">Whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        ///// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        ///// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        ///// per-operation limit of 5000. If this value is zero, the maximum possible number of results will be returned, up to 5000.</param>         
        ///// <param name="continuationToken">A continuation token returned by a previous listing operation.</param> 
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <returns>A result segment containing objects that implement <see cref="IListBlobItem"/>.</returns>
        //public BlobResultSegment ListBlobsSegmented(string prefix, bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext)
        //{
        //    BlobResultSegment result;
        //    ServerState ss = slaEngine.FindServerToRead("C:" + primaryContainer.Name);
        //    CloudBlobContainer container = (ss.IsPrimary) ? primaryContainer : secondaryContainer;

        //    watch.Start();
        //    result = container.ListBlobsSegmented(prefix, useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext);
        //    ss.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead("C:" + container.Name, Timestamp(container), ss);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to return a result segment containing a collection of blob items 
        ///// in the container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        public ICancellableAsyncResult BeginListBlobsSegmented(BlobContinuationToken currentToken, AsyncCallback callback, object state)
        {
          // Note: We could read asynchronously from the secondary container (if allowed by the SLA), but we don't bother.
            return mainPrimaryContainer.BeginListBlobsSegmented(currentToken, callback, state);
              
        }

        ///// <summary>
        ///// Begins an asynchronous operation to return a result segment containing a collection of blob items 
        ///// in the container.
        ///// </summary>
        ///// <param name="useFlatBlobListing">Whether to list blobs in a flat listing, or whether to list blobs hierarchically, by virtual directory.</param>
        ///// <param name="blobListingDetails">A <see cref="BlobListingDetails"/> enumeration describing which items to include in the listing.</param>
        ///// <param name="maxResults">A non-negative integer value that indicates the maximum number of results to be returned at a time, up to the 
        ///// per-operation limit of 5000. If this value is zero, the maximum possible number of results will be returned, up to 5000.</param>         
        ///// <param name="continuationToken">A continuation token returned by a previous listing operation.</param> 
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        public ICancellableAsyncResult BeginListBlobsSegmented(string prefix, bool useFlatBlobListing, BlobListingDetails blobListingDetails, int? maxResults, BlobContinuationToken currentToken, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return mainPrimaryContainer.BeginListBlobsSegmented(prefix, useFlatBlobListing, blobListingDetails, maxResults, currentToken, options, operationContext, callback, state);
        }

        ///// <summary>
        ///// Ends an asynchronous operation to return a result segment containing a collection of blob items 
        ///// in the container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns>A result segment containing objects that implement <see cref="IListBlobItem"/>.</returns>
        public BlobResultSegment EndListBlobsSegmented(IAsyncResult asyncResult)
        {
           return mainPrimaryContainer.EndListBlobsSegmented(asyncResult);
        }

        ///// <summary>
        ///// Sets permissions for the container.
        ///// </summary>
        ///// <param name="permissions">The permissions to apply to the container.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        //public void SetPermissions(BlobContainerPermissions permissions, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    primaryContainer.SetPermissions(permissions, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, Timestamp(primaryContainer), primaryServer);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to set permissions for the container.
        ///// </summary>
        ///// <param name="permissions">The permissions to apply to the container.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginSetPermissions(BlobContainerPermissions permissions, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginSetPermissions(permissions, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to set permissions for the container.
        ///// </summary>
        ///// <param name="permissions">The permissions to apply to the container.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginSetPermissions(BlobContainerPermissions permissions, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginSetPermissions(permissions, accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Returns the result of an asynchronous request to set permissions for the container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        //public void EndSetPermissions(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndSetPermissions(asyncResult);
        //}

        ///// <summary>
        ///// Gets the permissions settings for the container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <returns>The container's permissions.</returns>
        //public BlobContainerPermissions GetPermissions(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    BlobContainerPermissions result;
        //    ServerState ss = slaEngine.FindServerToRead("C:" + primaryContainer.Name);
        //    CloudBlobContainer container = (ss.IsPrimary) ? primaryContainer : secondaryContainer;

        //    watch.Start();
        //    result = container.GetPermissions(accessCondition, options, operationContext);
        //    ss.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead("C:" + container.Name, Timestamp(container), ss);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous request to get the permissions settings for the container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginGetPermissions(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginGetPermissions(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to get the permissions settings for the container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginGetPermissions(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginGetPermissions(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Returns the asynchronous result of the request to get the permissions settings for the container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns>The container's permissions.</returns>
        //public BlobContainerPermissions EndGetPermissions(IAsyncResult asyncResult)
        //{
        //    return primaryContainer.EndGetPermissions(asyncResult);
        //}

        ///// <summary>
        ///// Checks existence of the container.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <returns><c>true</c> if the container exists.</returns>
        //public bool Exists(BlobRequestOptions requestOptions = null, OperationContext operationContext = null)
        //{
        //    // It is probably unwise to ask the secondary if a container exists, so we don't.
        //    watch.Start();
        //    bool result = primaryContainer.Exists(requestOptions, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead("C:" + primaryContainer.Name, Timestamp(primaryContainer), primaryServer);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous request to check existence of the container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginExists(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginExists(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous request to check existence of the container.
        ///// </summary>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginExists(BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginExists(options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Returns the asynchronous result of the request to check existence of the container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        ///// <returns><c>true</c> if the container exists.</returns>
        //public bool EndExists(IAsyncResult asyncResult)
        //{
        //    return primaryContainer.EndExists(asyncResult);
        //}

        ///// <summary>
        ///// Retrieves the container's attributes.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        //public void FetchAttributes(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    ServerState ss = slaEngine.FindServerToRead("C:" + primaryContainer.Name);
        //    CloudBlobContainer container = (ss.IsPrimary) ? primaryContainer : secondaryContainer;

        //    watch.Start();
        //    container.FetchAttributes(accessCondition, options, operationContext);
        //    ss.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectRead("C:" + container.Name, Timestamp(container), ss);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to retrieve the container's attributes.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginFetchAttributes(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginFetchAttributes(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to retrieve the container's attributes.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginFetchAttributes(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginFetchAttributes(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to retrieve the container's attributes.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        //public void EndFetchAttributes(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndFetchAttributes(asyncResult);
        //}

        ///// <summary>
        ///// Sets the container's user-defined metadata.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        //public void SetMetadata(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    primaryContainer.SetMetadata(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    slaEngine.SessionState.RecordObjectWritten("C:" + primaryContainer.Name, Timestamp(primaryContainer), primaryServer);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to set user-defined metadata on the container.
        ///// </summary>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginSetMetadata(AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginSetMetadata(callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to set user-defined metadata on the container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">An object that specifies any additional options for the request.</param>
        ///// <param name="callback">The callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginSetMetadata(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginSetMetadata(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous request operation to set user-defined metadata on the container.
        ///// </summary>
        ///// <param name="asyncResult">An <see cref="IAsyncResult"/> that references the pending asynchronous operation.</param>
        //public void EndSetMetadata(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndSetMetadata(asyncResult);
        //}

        ///// <summary>
        ///// Acquires a lease on this container.
        ///// </summary>
        ///// <param name="leaseTime">A <see cref="TimeSpan"/> representing the span of time for which to acquire the lease,
        ///// which will be rounded down to seconds. If null, an infinite lease will be acquired. If not null, this must be
        ///// greater than zero.</param>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease, or null if no lease ID is proposed.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <returns>The ID of the acquired lease.</returns>
        //public string AcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    // Note: All leases are managed by the primary.
        //    watch.Start();
        //    string result = primaryContainer.AcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to acquire a lease on this container.
        ///// </summary>
        ///// <param name="leaseTime">A <see cref="TimeSpan"/> representing the span of time for which to acquire the lease,
        ///// which will be rounded down to seconds. If null, an infinite lease will be acquired. If not null, this must be
        ///// greater than zero.</param>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease, or null if no lease ID is proposed.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginAcquireLease(leaseTime, proposedLeaseId, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to acquire a lease on this container.
        ///// </summary>
        ///// <param name="leaseTime">A <see cref="TimeSpan"/> representing the span of time for which to acquire the lease,
        ///// which will be rounded down to seconds. If null, an infinite lease will be acquired. If not null, this must be
        ///// greater than zero.</param>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease, or null if no lease ID is proposed.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginAcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to acquire a lease on this container.
        ///// </summary>
        ///// <param name="asyncResult">An IAsyncResult that references the pending asynchronous operation.</param>
        ///// <returns>The ID of the acquired lease.</returns>
        //public string EndAcquireLease(IAsyncResult asyncResult)
        //{
        //    return primaryContainer.EndAcquireLease(asyncResult);
        //}

        ///// <summary>
        ///// Renews a lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        //public void RenewLease(AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    primaryContainer.RenewLease(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to renew a lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginRenewLease(AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginRenewLease(accessCondition, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to renew a lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginRenewLease(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginRenewLease(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to renew a lease on this container.
        ///// </summary>
        ///// <param name="asyncResult">An IAsyncResult that references the pending asynchronous operation.</param>
        //public void EndRenewLease(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndRenewLease(asyncResult);
        //}

        ///// <summary>
        ///// Changes the lease ID on this container.
        ///// </summary>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease. This cannot be null.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <returns>The new lease ID.</returns>
        //public string ChangeLease(string proposedLeaseId, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    string result = primaryContainer.ChangeLease(proposedLeaseId, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to change the lease on this container.
        ///// </summary>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease. This cannot be null.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginChangeLease(proposedLeaseId, accessCondition, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to change the lease on this container.
        ///// </summary>
        ///// <param name="proposedLeaseId">A string representing the proposed lease ID for the new lease. This cannot be null.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginChangeLease(proposedLeaseId, accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to change the lease on this container.
        ///// </summary>
        ///// <param name="asyncResult">An IAsyncResult that references the pending asynchronous operation.</param>
        ///// <returns>The new lease ID.</returns>
        //public string EndChangeLease(IAsyncResult asyncResult)
        //{
        //    return primaryContainer.EndChangeLease(asyncResult);
        //}

        ///// <summary>
        ///// Releases the lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        //public void ReleaseLease(AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    primaryContainer.ReleaseLease(accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to release the lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginReleaseLease(AccessCondition accessCondition, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginReleaseLease(accessCondition, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to release the lease on this container.
        ///// </summary>
        ///// <param name="accessCondition">An object that represents the access conditions for the container, including a required lease ID.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginReleaseLease(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginReleaseLease(accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to release the lease on this container.
        ///// </summary>
        ///// <param name="asyncResult">An IAsyncResult that references the pending asynchronous operation.</param>
        //public void EndReleaseLease(IAsyncResult asyncResult)
        //{
        //    primaryContainer.EndReleaseLease(asyncResult);
        //}

        ///// <summary>
        ///// Breaks the current lease on this container.
        ///// </summary>
        ///// <param name="breakPeriod">A <see cref="TimeSpan"/> representing the amount of time to allow the lease to remain,
        ///// which will be rounded down to seconds. If null, the break period is the remainder of the current lease,
        ///// or zero for infinite leases.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <returns>A <see cref="TimeSpan"/> representing the amount of time before the lease ends, to the second.</returns>
        //public TimeSpan BreakLease(TimeSpan? breakPeriod = null, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        //{
        //    watch.Start();
        //    TimeSpan result = primaryContainer.BreakLease(breakPeriod, accessCondition, options, operationContext);
        //    primaryServer.AddRtt(watch.ElapsedMilliseconds);
        //    return result;
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to break the current lease on this container.
        ///// </summary>
        ///// <param name="breakPeriod">A <see cref="TimeSpan"/> representing the amount of time to allow the lease to remain,
        ///// which will be rounded down to seconds. If null, the break period is the remainder of the current lease,
        ///// or zero for infinite leases.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginBreakLease(breakPeriod, callback, state);
        //}

        ///// <summary>
        ///// Begins an asynchronous operation to break the current lease on this container.
        ///// </summary>
        ///// <param name="breakPeriod">A <see cref="TimeSpan"/> representing the amount of time to allow the lease to remain,
        ///// which will be rounded down to seconds. If null, the break period is the remainder of the current lease,
        ///// or zero for infinite leases.</param>
        ///// <param name="accessCondition">An object that represents the access conditions for the container. If null, no condition is used.</param>
        ///// <param name="options">The options for this operation. If null, default options will be used.</param>
        ///// <param name="callback">An optional callback delegate that will receive notification when the asynchronous operation completes.</param>
        ///// <param name="state">A user-defined object that will be passed to the callback delegate.</param>
        ///// <returns>An <see cref="IAsyncResult"/> that references the asynchronous operation.</returns>
        //public ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        //{
        //    return primaryContainer.BeginBreakLease(breakPeriod, accessCondition, options, operationContext, callback, state);
        //}

        ///// <summary>
        ///// Ends an asynchronous operation to break the current lease on this container.
        ///// </summary>
        ///// <param name="asyncResult">An IAsyncResult that references the pending asynchronous operation.</param>
        ///// <returns>A <see cref="TimeSpan"/> representing the amount of time before the lease ends, to the second.</returns>
        //public TimeSpan EndBreakLease(IAsyncResult asyncResult)
        //{
        //    return primaryContainer.EndBreakLease(asyncResult);
        //}

        //#endregion

        //#region CloudBlobContainerBase public methods
        //// Note that none of the calls below invole network calls, and they all are performed on the primary container.
        //// Some of these calls return CloudPageBlobs or CloudBlockBlobs rather than CapCloudBlobs.
        //// Calls on these returned blobs will therefore not use consistency-based SLAs.

        ///// <summary>
        ///// Returns a shared access signature for the container.
        ///// </summary>
        ///// <param name="policy">The access policy for the shared access signature.</param>
        ///// <returns>A shared access signature.</returns>
        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy)
        {
            return mainPrimaryContainer.GetSharedAccessSignature(policy);
        }

        ///// <summary>
        ///// Returns a shared access signature for the container.
        ///// </summary>
        ///// <param name="policy">The access policy for the shared access signature.</param>
        ///// <param name="groupPolicyIdentifier">A container-level access policy.</param>
        ///// <returns>A shared access signature.</returns>
        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy, string groupPolicyIdentifier)
        {
            return mainPrimaryContainer.GetSharedAccessSignature(policy, groupPolicyIdentifier);
        }

        ///// <summary>
        ///// Gets a reference to a page blob in this container.
        ///// </summary>
        ///// <param name="blobName">The name of the blob.</param>
        ///// <returns>A reference to a page blob.</returns>
        public CloudPageBlob GetPageBlobReference(string blobName)
        {
            return mainPrimaryContainer.GetPageBlobReference(blobName);
        }

        ///// <summary>
        ///// Returns a reference to a page blob in this virtual directory.
        ///// </summary>
        ///// <param name="itemName">The name of the page blob.</param>
        ///// <param name="snapshotTime">The snapshot timestamp, if the blob is a snapshot.</param>
        ///// <returns>A reference to a page blob.</returns>
        public CloudPageBlob GetPageBlobReference(string blobName, DateTimeOffset? snapshotTime)
        {
            return mainPrimaryContainer.GetPageBlobReference(blobName, snapshotTime);
        }

        ///// <summary>
        ///// Gets a reference to a block blob in this container.
        ///// </summary>
        ///// <param name="blobName">The name of the blob.</param>
        ///// <returns>A reference to a block blob.</returns>
        public CloudBlockBlob GetBlockBlobReference(string blobName)
        {
            return mainPrimaryContainer.GetBlockBlobReference(blobName);
        }

        ///// <summary>
        ///// Gets a reference to a block blob in this container.
        ///// </summary>
        ///// <param name="blobName">The name of the blob.</param>
        ///// <param name="snapshotTime">The snapshot timestamp, if the blob is a snapshot.</param>
        ///// <returns>A reference to a block blob.</returns>
        public CloudBlockBlob GetBlockBlobReference(string blobName, DateTimeOffset? snapshotTime)
        {
            return mainPrimaryContainer.GetBlockBlobReference(blobName, snapshotTime);
        }

        ///// <summary>
        ///// Gets a reference to a virtual blob directory beneath this container.
        ///// </summary>
        ///// <param name="relativeAddress">The name of the virtual blob directory.</param>
        ///// <returns>A reference to a virtual blob directory.</returns>
        public CloudBlobDirectory GetDirectoryReference(string relativeAddress)
        {
            return mainPrimaryContainer.GetDirectoryReference(relativeAddress);
        }

        //#endregion
    }
}
