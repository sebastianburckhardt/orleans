using System;
using System.Collections.Generic;
using System.Threading.Tasks;


using Orleans.Storage;

namespace Orleans
{
    /// <summary>
    /// Bridge to provide runtime services to Orleans clients, both inside and outside silos.
    /// </summary>
    /// <remarks>
    /// Only one GrainClient is permitted per AppDomain.
    /// </remarks>
    public static class GrainClient
    {
        /// <summary>
        /// Global pre-call interceptor function
        /// Synchronous callback made just before a message is about to be constructed and sent by a client to a grain.
        /// This call will be made from the same thread that constructs the message to be sent, so any thread-local settings 
        /// such as <c>Orleans.RequestContext</c> will be picked up.
        /// </summary>
        /// <remarks>This callback method should return promptly and do a minimum of work, to avoid blocking calling thread or impacting throughput.</remarks>
        /// <param name="request">Details of the method to be invoked, including InterfaceId and MethodId</param>
        /// <param name="grain">The GrainReference this request is being sent through.</param>
        public static Action<InvokeMethodRequest, IGrain> ClientInvokeCallback { get; set; }

        /// <summary>
        /// A reference to the GrainClient instance in the current app domain, 
        /// of the appropriate type depending on whether caller is running inside or outside silo.
        /// </summary>
        public static IGrainClient Current { get; set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        internal static IGrainClientInternal InternalCurrent { get { return Current as IGrainClientInternal; } }

        internal static Message CreateMessage(InvokeMethodRequest request, InvokeMethodOptions options, PlacementStrategy placement = null)
        {
            var message =
                new Message(
                    Message.Categories.Application,
                (options & InvokeMethodOptions.OneWay) != 0 ? Message.Directions.OneWay : Message.Directions.Request)
            {
                Id = CorrelationId.GetNext(),
                InterfaceId = request.InterfaceId,
                MethodId = request.MethodId,
                IsReadOnly = (options & InvokeMethodOptions.ReadOnly) != 0,
                IsUnordered = (options & InvokeMethodOptions.Unordered) != 0,
                //TaskType = (int)(options & InvokeMethodOptions.TaskTypeMask),
                //DelayForConsistency = (options & InvokeMethodOptions.DelayForConsistency) != 0,
                BodyObject = request,
                PlacementStrategy = placement,
            };
            if ((options & InvokeMethodOptions.AlwaysInterleave) != 0)
            {
                message.IsAlwaysInterleave = true;
            }
            RequestContext.ExportToMessage(message);
            //message.SetHeader(Message.Header.Timestamps, String.Format("Created={0}", Logger.PrintDate(DateTime.UtcNow)));
            return message;
        }
    }

    /// <summary>
    /// The IGrainClient interface defines the API exposed to non-Orleans code for interacting with Orleans.
    /// </summary>
    public interface IGrainClient
    {
        /// <summary>
        /// Provides client application code with access to an Orleans logger.
        /// </summary>
        OrleansLogger AppLogger { get; }

        /// <summary>
        /// A unique identifier for the current client.
        /// There is no semantic content to this string, but it may be useful for logging.
        /// </summary>
        string Identity { get; }

        /// <summary>
        /// Return the currently running grain if in a grain, or null if running in a client
        /// </summary>
        IAddressable CurrentGrain { get; }

        /// <summary>
        /// Get the current response timeout setting for this client.
        /// </summary>
        /// <returns>Response timeout value</returns>
        TimeSpan GetResponseTimeout();

        /// <summary>
        /// Sets the current response timeout setting for this client.
        /// </summary>
        /// <param name="timeout">New response timeout value</param>
        void SetResponseTimeout(TimeSpan timeout);
    }

    /// <summary>
    /// The IGrainClientInternal interface defines the internal client API exposed to Orleans runtime code.
    /// For internal use only.
    /// </summary>
    internal interface IGrainClientInternal
    {
        void SendRequest(GrainReference target, InvokeMethodRequest request, TaskCompletionSource<object> context, Action<Message, TaskCompletionSource<object>> callback, string debugContext = null, InvokeMethodOptions options = InvokeMethodOptions.None, string genericType = null, PlacementStrategy placement = null);

        void ReceiveResponse(Message message);

        /// <summary>
        /// Return the currently storage provider configured for this grain, or null if no storage provider configured for this grain.
        /// </summary>
        /// <exception cref="InvalidOperationException">If called from outside grain class</exception>
        IStorageProvider CurrentStorageProvider { get; }

        AsyncValue<IOrleansReminder> RegisterOrUpdateReminder(string reminderName, TimeSpan dueTime, TimeSpan period);

        AsyncCompletion UnregisterReminder(IOrleansReminder reminder);

        AsyncValue<IOrleansReminder> GetReminder(string reminderName);

        AsyncValue<List<IOrleansReminder>> GetReminders();

        Task ExecAsync(Func<Task> action, ISchedulingContext context);

        void Reset();

        Task<GrainReference> CreateObjectReference(IGrainObserver obj, IGrainMethodInvoker invoker);

        Task<GrainReference> CreateObjectReference(IAddressable obj, IGrainMethodInvoker invoker);

        Task DeleteObjectReference(IGrainObserver obj);

        Task DeleteObjectReference(IAddressable obj);

        ActivationAddress CurrentActivation { get; }

        SiloAddress CurrentSilo { get; }

        void DeactivateOnIdle(ActivationId id);

#if !DISABLE_STREAMS
        Streams.IStreamProviderManager CurrentStreamProviderManager { get; }
#endif
        IGrainTypeResolver GrainTypeResolver { get; }

        string CaptureRuntimeEnvironment();

        IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null);

        SiloStatus GetSiloStatus(SiloAddress siloAddress);
    }
}
