using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;

namespace Orleans.AzureUtils
{
    /// <summary>
    /// How to use the Queue Storage Service: http://www.windowsazure.com/en-us/develop/net/how-to-guides/queue-service/
    /// Windows Azure Storage Abstractions and their Scalability Targets: http://blogs.msdn.com/b/windowsazurestorage/archive/2010/05/10/windows-azure-storage-abstractions-and-their-scalability-targets.aspx
    /// Naming Queues and Metadata: http://msdn.microsoft.com/en-us/library/windowsazure/dd179349.aspx
    /// Windows Azure Queues and Windows Azure Service Bus Queues - Compared and Contrasted: http://msdn.microsoft.com/en-us/library/hh767287(VS.103).aspx
    /// Status and Error Codes: http://msdn.microsoft.com/en-us/library/dd179382.aspx
    ///
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/tags/scalability/
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/12/30/windows-azure-storage-architecture-overview.aspx
    /// http://blogs.msdn.com/b/windowsazurestorage/archive/2010/11/06/how-to-get-most-out-of-windows-azure-tables.aspx
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    internal static class AzureQueueDefaultPolicies
    {
        public static int MaxQueueOperationRetries;
        public static TimeSpan PauseBetweenQueueOperationRetries;
        public static TimeSpan QueueOperation_TIMEOUT;
        public static RetryPolicy QueueOperationRetryPolicy;

        static AzureQueueDefaultPolicies()
        {
            MaxQueueOperationRetries = 5;
            PauseBetweenQueueOperationRetries = TimeSpan.FromMilliseconds(100);
            QueueOperationRetryPolicy = RetryPolicies.Retry(MaxQueueOperationRetries, PauseBetweenQueueOperationRetries); // 5 x 100ms
            QueueOperation_TIMEOUT = PauseBetweenQueueOperationRetries.Multiply(MaxQueueOperationRetries).Multiply(6);    // 3 sec
        }
    }

    public class AzureQueueDataManager
    {
        public string QueueName { get; private set; }

        protected string ConnectionString { get; set; }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        private readonly Logger logger;
        private readonly CloudQueueClient queueOperationsClient;
        private CloudQueue queue;
        //private readonly PendingTask _initQueueTask;

        private class PendingTask
        {
            private readonly Func<Task> _work;
            private readonly object _initLock = new object();
            private bool _initialized;
            private Task _initializationTask;

            private PendingTask(Func<Task> work)
            {
                this._work = work;
                this._initLock = new object();
            }

            public static PendingTask Create(Func<Task> work)
            {
                PendingTask pendingTask = new PendingTask(work);
                pendingTask.Start();
                return pendingTask;
            }

            private void Start()
            {
                if (this._initialized) return;
                lock (this._initLock)
                {
                    if (this._initialized) return;
                    this._initializationTask = _work();
                    this._initialized = true;
                }
            }

            public bool IsComplete()
            {
                if (!this._initialized)
                {
                    Start();
                }
                switch (this._initializationTask.Status)
                {
                    case TaskStatus.RanToCompletion:
                        return true;
                    case TaskStatus.Faulted:
                        Exception ex = this._initializationTask.Exception;
                        this._initialized = false;
                        if (ex != null)
                        {
                            throw ex;
                        }
                        return false;
                    default:
                        return false;
                }
            }

            public async Task Complete()
            {
                if (!IsComplete())
                {
                    await _initializationTask;
                }
            }
        }

        public AzureQueueDataManager(string queueName, string deploymentId, string storageConnectionString)
        {
            AzureStorageUtils.ValidateQueueName(queueName);

            this.logger = Logger.GetLogger(this.GetType().Name, Logger.LoggerType.Runtime);
            this.QueueName = deploymentId + "-" + queueName;
            AzureStorageUtils.ValidateQueueName(QueueName);
            this.ConnectionString = storageConnectionString;

            this.queueOperationsClient = AzureStorageUtils.GetCloudQueueClient(
                ConnectionString,
                AzureQueueDefaultPolicies.QueueOperationRetryPolicy,
                AzureQueueDefaultPolicies.QueueOperation_TIMEOUT,
                logger);
            //this._initQueueTask = PendingTask.Create(InitQueueAsync);
        }

        public Task InitQueue_Async()
        {
            return InitQueueAsync(); // this._initQueueTask.Complete();
        }

        private async Task InitQueueAsync()
        {
            DateTime startTime = DateTime.UtcNow;

            try
            {
                // Retrieve a reference to a queue.
                // GKTODO: not sure if this is a blocking call or not. Did not find an alternative async API. Should probably use BeginListQueuesSegmented.
                queue = queueOperationsClient.GetQueueReference(QueueName);

                // Create the queue if it doesn't already exist.
                bool didCreate;
                
                didCreate = await Task<bool>.Factory.FromAsync(
                     queue.BeginCreateIfNotExist, queue.EndCreateIfNotExist, null);

                logger.Info(ErrorCode.AzureQueue_01, "{0} Azure storage queue {1}", (didCreate ? "Created" : "Attached to"), QueueName);
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "CreateIfNotExist", ErrorCode.AzureQueue_02);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "InitQueue_Async");
            }
        }

        public async Task DeleteQueue()
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Deleting queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                CloudQueue queueRef = queue;
                if (queueRef == null)
                {
                    // that way we don't have first to create the queue to be able later to delete it.
                    queueRef = queueOperationsClient.GetQueueReference(QueueName);
                }
                Task<bool> exists = Task<bool>.Factory.FromAsync(queueRef.BeginExists, queueRef.EndExists, null);
                if (await exists)
                {
                    Task promise = Task.Factory.FromAsync(queueRef.BeginDelete, queueRef.EndDelete, null);
                    await promise;
                    logger.Info(ErrorCode.AzureQueue_03, String.Format("Deleted Azure Queue {0}", QueueName));
                }
                //logger.Info(ErrorCode.AzureQueue_15, String.Format("Could not delete Azure Queue {0} since it does not exist.", QueueName));
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "DeleteQueue", ErrorCode.AzureQueue_04);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "DeleteQueue");
            }
        }

        public async Task ClearQueue()
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Clearing a queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task promise = Task.Factory.FromAsync(queue.BeginClear, queue.EndClear, null);
                await promise;
                logger.Info(ErrorCode.AzureQueue_05, String.Format("Cleared Azure Queue {0}", QueueName));
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "ClearQueue", ErrorCode.AzureQueue_06);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "ClearQueue");
            }
        }

        public async Task AddQueueMessage(CloudQueueMessage message)
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Adding message {0} to queue: {1}", message, QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task promise = Task.Factory.FromAsync(
                         queue.BeginAddMessage, queue.EndAddMessage, message, null);
                await promise;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "AddQueueMessage", ErrorCode.AzureQueue_07);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "AddQueueMessage");
            }
        }

        public async Task<CloudQueueMessage> PeekQueueMessage()
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Peeking a message from queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task<CloudQueueMessage> promise = Task<CloudQueueMessage>.Factory.FromAsync(
                         queue.BeginPeekMessage, queue.EndPeekMessage, null);
                CloudQueueMessage message = await promise;
                return message;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "PeekQueueMessage", ErrorCode.AzureQueue_08);
                return null; // Dummy statement to keep compiler happy
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "PeekQueueMessage");
            }
        }

        public async Task<CloudQueueMessage> GetQueueMessage()
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Getting a message from queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                // http://msdn.microsoft.com/en-us/library/ee758456.aspx
                // If no messages are visible in the queue, GetMessage returns null.
                Task<CloudQueueMessage> promise = Task<CloudQueueMessage>.Factory.FromAsync(
                         queue.BeginGetMessage, queue.EndGetMessage, null);
                CloudQueueMessage message = await promise;
                return message;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "GetQueueMessage", ErrorCode.AzureQueue_09);
                return null; // Dummy statement to keep compiler happy
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "GetQueueMessage");
            }
        }

        public async Task<IEnumerable<CloudQueueMessage>> GetQueueMessages(int count = -1)
        {
            DateTime startTime = DateTime.UtcNow;
            if (count == -1)
            {
                count = CloudQueueMessage.MaxNumberOfMessagesToPeek;
            }
            if (logger.IsVerbose2) logger.Verbose2("Getting up to {0} messages from queue: {1}", count, QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task<IEnumerable<CloudQueueMessage>> promise = Task<IEnumerable<CloudQueueMessage>>.Factory.FromAsync(
                         queue.BeginGetMessages, queue.EndGetMessages, count, null);
                IEnumerable<CloudQueueMessage> messages = await promise;
                return messages;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "GetQueueMessages", ErrorCode.AzureQueue_10);
                return null; // Dummy statement to keep compiler happy
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "GetQueueMessages");
            }
        }

        public async Task DeleteQueueMessage(CloudQueueMessage message)
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("Deleting a message from queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task promise = Task.Factory.FromAsync(
                         queue.BeginDeleteMessage, queue.EndDeleteMessage, message.Id, message.PopReceipt, null);
                await promise;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "DeleteMessage", ErrorCode.AzureQueue_11);
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "DeleteQueueMessage");
            }
        }

        internal async Task GetAndDeleteQueueMessage()
        {
            CloudQueueMessage message = await GetQueueMessage();
            await DeleteQueueMessage(message);
        }

        public async Task<int> GetApproximateMessageCount()
        {
            DateTime startTime = DateTime.UtcNow;
            if (logger.IsVerbose2) logger.Verbose2("GetApproximateMessageCount a message from queue: {0}", QueueName);
            try
            {
                //await this._initQueueTask.Complete();
                Task promise = Task.Factory.FromAsync(
                         queue.BeginFetchAttributes, queue.EndFetchAttributes, null);
                await promise;
                return queue.ApproximateMessageCount.HasValue ? queue.ApproximateMessageCount.Value : 0;
            }
            catch (Exception exc)
            {
                ReportErrorAndRethrow(exc, "FetchAttributes", ErrorCode.AzureQueue_12);
                return 0; // Dummy statement to keep compiler happy
            }
            finally
            {
                CheckAlertSlowAccess(startTime, "GetApproximateMessageCount");
            }
        }

        private void CheckAlertSlowAccess(DateTime startOperation, string operation)
        {
            TimeSpan timeSpan = DateTime.UtcNow - startOperation;
            if (timeSpan > AzureQueueDefaultPolicies.QueueOperation_TIMEOUT)
            {
                logger.Warn(ErrorCode.AzureQueue_13, "Slow access to Azure queue {0} for {1}, which took {2}.", QueueName, operation, timeSpan);
            }
        }

        private void ReportErrorAndRethrow(Exception exc, string operation, ErrorCode errorCode)
        {
            string errMsg = String.Format("Error doing {0} for Azure storage queue {1} \n Exception = \n {2}", operation, QueueName, exc);
            logger.Error(errorCode, errMsg, exc);
            throw new AggregateException(errMsg, exc);
        }
    }
}

