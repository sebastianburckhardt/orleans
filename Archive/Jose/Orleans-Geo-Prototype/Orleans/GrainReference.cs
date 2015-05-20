using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using Orleans.Serialization;

namespace Orleans
{
    // todo: figure out how to reduce visibility and still make this callable from generated code
    /// <summary>
    /// This is the base class for all typed grain references.
    /// It is the grain equivalent to <c>object</c>.
    /// </summary>
    [Serializable]
    public class GrainReference : IAddressable, IEquatable<GrainReference>, ISerializable
    {
        [NonSerialized]
        private readonly string _genericType;
        private PlacementStrategy _placement;

        [NonSerialized]
        private readonly Dictionary<int, DateTime> _lastCallTime = new Dictionary<int, DateTime>();
        [NonSerialized]
        private readonly Dictionary<int, object> _lastCallResult = new Dictionary<int, object>();

        [NonSerialized]
        private static readonly Logger logger = Logger.GetLogger("GrainReference", Logger.LoggerType.Runtime);
        [NonSerialized]
        private static bool USE_DEBUG_CONTEXT = true;
        [NonSerialized]
        private static bool USE_DEBUG_CONTEXT_PARAMS = false;

        /// <summary>
        /// For internal use only.
        /// </summary>
        internal GrainId GrainId { get; private set; }

        /// <summary>
        /// For internal use only.
        /// Called from Orleans generated code.
        /// </summary>
        protected internal SiloAddress SystemTargetSilo { get; private set; }

        /// <summary>
        /// Whether the runtime environment for system targets has been initialized yet.
        /// For internal use only.
        /// Called from Orleans generated code.
        /// </summary>
        protected internal bool IsInitializedSystemTarget { get { return SystemTargetSilo != null; } }

        #region Constructors

        /// <summary>
        /// Constructs a reference to the grain with the specified Id.
        /// </summary>
        /// <param name="grainId">The Id of the grain to refer to.</param>
        private GrainReference(GrainId grainId, string genericType = null, PlacementStrategy placement = null, SiloAddress systemTargetSilo = null)
        {
            GrainId = grainId;
            _genericType = genericType;
            SystemTargetSilo = systemTargetSilo;
            CopyPlacementField(placement);
        }

        /// <summary>
        /// Constructs a copy of a grain reference.
        /// </summary>
        /// <param name="other">The reference to copy.</param>
        protected GrainReference(GrainReference other)
        {
            GrainId = other.GrainId;
            _genericType = other._genericType;
            SystemTargetSilo = other.SystemTargetSilo;
            CopyPlacementField(other._placement);
        }

        #endregion

        #region Instance creator factory functions

        /// <summary>
        /// Constructs a reference to the grain with the specified ID.
        /// For internal use only.
        /// </summary>
        /// <param name="grainId">The ID of the grain to refer to.</param>
        internal static GrainReference FromGrainId(GrainId grainId, string genericType = null, PlacementStrategy placement = null, SiloAddress systemTargetSilo = null)
        {
            return new GrainReference(grainId, genericType, placement, systemTargetSilo);
        }

        #endregion

        /// <summary> Returns a string representation of this reference. </summary>
        public override string ToString()
        {
            if (GrainId.IsSystemTarget)
                return String.Format("SystemTarget:{0}/{1}", GrainId, SystemTargetSilo);
            else
                return "GrainReference:" + GrainId;
        }

        /// <summary>
        /// Tests this reference for equality to another object.
        /// Two grain references are equal if they both refer to the same grain.
        /// </summary>
        /// <param name="obj">The object to test for equality against this reference.</param>
        /// <returns><c>true</c> if the object is equal to this reference.</returns>
        public override bool Equals(object obj)
        {
            return Equals(obj as GrainReference);
        }
        public bool Equals(GrainReference other)
        {
            if (other == null)
                return false;

            if (_genericType != other._genericType)
                return false;

            if (!Equals(_placement, other._placement))
                return false;

            if (!Equals(SystemTargetSilo, other.SystemTargetSilo))
                return false;

            return GrainId.Equals(other.GrainId);
        }

        /// <summary> Calculates a hash code for a grain reference. </summary>
        public override int GetHashCode()
        {
            return SystemTargetSilo == null ? GrainId.GetHashCode() : GrainId.GetHashCode() ^ SystemTargetSilo.GetHashCode();
        }

        /// <summary>
        /// Compares two references for equality.
        /// Two grain references are equal if they both refer to the same grain.
        /// </summary>
        /// <param name="reference1">First grain reference to compare.</param>
        /// <param name="reference2">Second grain reference to compare.</param>
        /// <returns><c>true</c> if both grain references refer to the same grain (by grain identifier).</returns>
        public static bool operator ==(GrainReference reference1, GrainReference reference2)
        {
            if (((object)reference1) == null)
                return ((object)reference2) == null;

            return reference1.Equals(reference2);
        }

        /// <summary>
        /// Compares two references for inequality.
        /// Two grain references are equal if they both refer to the same grain.
        /// </summary>
        /// <param name="reference1">First grain reference to compare.</param>
        /// <param name="reference2">Second grain reference to compare.</param>
        /// <returns><c>false</c> if both grain references are resolved to the same grain (by grain identifier).</returns>
        public static bool operator !=(GrainReference reference1, GrainReference reference2)
        {
            if (((object)reference1) == null)
                return ((object)reference2) != null;

            return !reference1.Equals(reference2);
        }

        #region Protected members

        /// <summary>
        /// Implemented by generated subclasses to return a constant
        /// Implemented in Orleans generated code.
        /// </summary>
        protected virtual int InterfaceId
        {
            // todo: omit? methodId can be function of interface+method
            get
            {
                throw new InvalidOperationException("Should be overridden by subclass");
            }
        }

        /// <summary>
        /// This method is for internal run-time use only.
        /// Implemented in Orleans generated code.
        /// </summary>
        public virtual bool IsCompatible(int interfaceId)
        {
            throw new InvalidOperationException("Should be overridden by subclass");
        }

        /// <summary>
        /// Return the name of the interface for this GrainReference. 
        /// Implemented in Orleans generated code.
        /// </summary>
        protected virtual string InterfaceName
        {
            get
            {
                throw new InvalidOperationException("Should be overridden by subclass");
            }
        }

        /// <summary>
        /// Return the method name associated with the specified interfaceId and methodId values.
        /// </summary>
        /// <param name="interfaceId">Interface Id</param>
        /// <param name="methodId">Method Id</param>
        /// <returns>Method name string.</returns>
        protected virtual string GetMethodName(int interfaceId, int methodId)
        {
            throw new InvalidOperationException("Should be overridden by subclass");
        }

        /// <summary>
        /// This method is for internal run-time use only.
        /// Called from Orleans generated code.
        /// </summary>
        protected void InvokeOneWayMethod(int methodId, object[] arguments, InvokeMethodOptions options = InvokeMethodOptions.None, SiloAddress silo = null)
        {
            Task<object> resultTask = InvokeMethodAsync<object>(methodId, arguments, TimeSpan.Zero, options | InvokeMethodOptions.OneWay);
            if (!resultTask.IsCompleted && resultTask.Result != null)
            {
                throw new OrleansException("Unexpected return value: one way InvokeMethod is expected to return null.");
            }
        }

        /// <summary>
        /// This method is for internal run-time use only.
        /// Called from Orleans generated code.
        /// </summary>
        protected async Task<T> InvokeMethodAsync<T>(int methodId, object[] arguments, TimeSpan cacheDuration, InvokeMethodOptions options = InvokeMethodOptions.None, SiloAddress silo = null)
        {
            CheckForGrainArguments(arguments);

            if (cacheDuration != TimeSpan.Zero)
            {
                lock (this)
                {
                    // Use any cached value from previous call if still with the Cacheable duration window
                    if (_lastCallTime.ContainsKey(methodId) &&
                        (DateTime.UtcNow - _lastCallTime[methodId]) < cacheDuration)
                    {
                        return (T) _lastCallResult[methodId];
                    }
                }
            }
            var argsDeepCopy = (object[])SerializationManager.DeepCopy(arguments);
            var request = new InvokeMethodRequest(this.InterfaceId, methodId, argsDeepCopy);

            Task<object> resultTask = InvokeMethod_Impl(request, null, options);

            if (resultTask == null)
            {
                return default(T);
            }

            resultTask = AsyncValue.FromTask(resultTask).AsTask();
            object result;
            //if (GrainClient.Current == null || GrainClient.Current is OutsideGrainClient)
            //{
            //    // We are not running under Orleans scheduler(s) - most likely on the client
            //    // We want to use ConfigureAwait(false) here to create an execution context 
            //    // boundary for all the internal Orleans runtime functions "below" this point.
            //    // Otherwise any execution constraints [such as single thread ASP / GUI code] 
            //    // would affect the Orleans runtime, and potentially deadlock 
            //    // the internal workings of the runtime.
            //    result = await resultTask.ConfigureAwait(false);
            //}
            //else
            //{
                // We are running on server under Orleans scheduler control
                // so won't need to detach from caller's execution context
                result = await resultTask;
            //}

            if (cacheDuration != TimeSpan.Zero)
            {
                lock (this)
                {
                    _lastCallResult[methodId] = result;
                    _lastCallTime[methodId] = DateTime.UtcNow; // Note: Set lastCallTime last, as that is the indicator for whether there is a cached result available for that method
                }
            }

            // When we return to the caller which is awaiting this Invoke call, 
            // they will resume execution in whichever execution context 
            // they were originally running in.
            return (T) result;
        }

        #endregion

        #region Private members

        private Task<object> InvokeMethod_Impl(InvokeMethodRequest request, string debugContext, InvokeMethodOptions options)
        {
            if (debugContext == null && USE_DEBUG_CONTEXT)
            {
                debugContext = GetDebugContext(this.InterfaceName, GetMethodName(this.InterfaceId, request.MethodId), request.Arguments);
            }

            // Call any registered client pre-call interceptor function.
            CallClientInvokeCallback(request);

            bool isOneWayCall = ((options & InvokeMethodOptions.OneWay) != 0);

            TaskCompletionSource<object> resolver = isOneWayCall ? null : new TaskCompletionSource<object>();
            // todo: avoid serializing immutable arguments for local calls
            GrainClient.InternalCurrent.SendRequest(this, request, resolver, ResponseCallback, debugContext, options, _genericType, _placement);
            return isOneWayCall ? null : resolver.Task;
        }

        private void CallClientInvokeCallback(InvokeMethodRequest request)
        {
            // Make callback to any registered client callback function, allowing opportunity for an application to set any additional RequestContext info, etc.
            // TODO: Should we set some kind of callback-in-progress flag to detect and prevent any inappropriate callback loops on this GrainReference?
            try
            {
                Action<InvokeMethodRequest, IGrain> callback = GrainClient.ClientInvokeCallback; // Take copy to avoid potential race conditions
                if (callback != null)
                {
                    // Call ClientInvokeCallback only for grain calls, not for system targets.
                    if (this is IGrain)
                    {
                        callback(request, (IGrain)this);
                    }
                }
            }
            catch (Exception exc)
            {
                logger.Warn(ErrorCode.ProxyClient_ClientInvokeCallback_Error,
                    "Error while invoking ClientInvokeCallback function " + GrainClient.ClientInvokeCallback,
                    exc);
                throw;
            }
        }

        private void DisallowPlacementForSystemTargets(PlacementStrategy placement)
        {
            // this is a sanity check to ensure that we're not specifying a placement silo for system grains. 
            if (placement != null && GrainId.IsSystemTarget)
                throw new InvalidOperationException("You cannot specify a placement strategy for system grains.");
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        private static void ResponseCallback(Message message, TaskCompletionSource<object> context)
        {
            TaskCompletionSource<object> resolver = (TaskCompletionSource<object>)context;
            OrleansResponse response = null;
            // todo: review - seems like there should be more well-defined logic here for for Error and Rejection
            if (message.Result != Message.ResponseTypes.Rejection)
            {
                try
                {
                    response = (OrleansResponse)message.BodyObject;
                }
                catch (Exception exc)
                {
                    //  catch the Deserialize exception and break the promise with it.
                    response = OrleansResponse.ExceptionResponse(exc);
                }
            }
            else
            {
                Exception rejection;
                switch (message.RejectionType)
                {
                    case Message.RejectionTypes.GatewayTooBusy:
                        rejection = new OrleansGatewayTooBusyException();
                        break;
                    case Message.RejectionTypes.DuplicateRequest:
                        return; // Ignore duplicates
                    //break;
                    default:
                        if (String.IsNullOrEmpty(message.RejectionInfo))
                        {
                            message.RejectionInfo = "Unable to send request - no rejection info available";
                        }
                        rejection = new OrleansException(message.RejectionInfo);
                        break;
                }
                response = OrleansResponse.ExceptionResponse(rejection);
            }

            if (!response.ExceptionFlag)
            {
                resolver.TrySetResult(response.Data);
            }
            else
            {
                resolver.TrySetException(response.Exception);
            }
        }

        private void CopyPlacementField(PlacementStrategy placement)
        {
            DisallowPlacementForSystemTargets(placement);
            if (null == placement)
                this._placement = null;
            else
            {
                // we deep-copy the placement strategy so that we don't end up with an aliased object.
                this._placement = (PlacementStrategy)SerializationManager.DeepCopy(placement);
            }
        }

        #endregion

        /// <summary>
        /// Internal implementation of Cast operation for grain references
        /// Called from Orleans generated code.
        /// </summary>
        /// <param name="targetReferenceType">Type that this grain reference should be cast to</param>
        /// <param name="grainRefCreatorFunc">Delegate function to create grain references of the target type</param>
        /// <param name="grainRef">Grain reference to cast from</param>
        /// <param name="interfaceId">Interface id value for the target cast type</param>
        /// <returns>GrainReference that is usable as the target type</returns>
        /// <exception cref="System.InvalidCastException">if the grain cannot be cast to the target type</exception>
        protected internal static IAddressable CastInternal(
            Type targetReferenceType,
            Func<GrainReference, IAddressable> grainRefCreatorFunc,
            IAddressable grainRef,
            int interfaceId)
        {
            if (grainRef == null) throw new ArgumentNullException("grainRef");

            Type sourceType = grainRef.GetType();

            if (!typeof(IAddressable).IsAssignableFrom(targetReferenceType))
            {
                throw new InvalidCastException(String.Format("Target type must be derived from Orleans.IAddressable - cannot handle {0}", targetReferenceType));
            }
            else if (typeof(GrainBase).IsAssignableFrom(sourceType))
            {
                GrainBase grainClassRef = (GrainBase)grainRef;
                GrainReference g = FromGrainId(grainClassRef.Identity);
                grainRef = g;
            }
            else if (!typeof(GrainReference).IsAssignableFrom(sourceType))
            {
                throw new InvalidCastException(String.Format("Grain reference object must an Orleans.GrainReference - cannot handle {0}", sourceType));
            }

            if (targetReferenceType.IsAssignableFrom(sourceType))
            {
                // Already compatible - no conversion or wrapping necessary
                return grainRef;
            }

            // We have an untyped grain reference that may resolve eventually successfully -- need to enclose in an apprroately typed wrapper class
            GrainReference grainReference = (GrainReference)grainRef;
            GrainReference grainWrapper = (GrainReference)grainRefCreatorFunc(grainReference);
            return grainWrapper;
        }

        /// <summary>
        /// This method is for internal run-time use only.
        /// Called from Orleans generated code.
        /// </summary>
        public static Task<GrainReference> CreateObjectReference(IAddressable o, IGrainMethodInvoker invoker)
        {
            return GrainClient.InternalCurrent.CreateObjectReference(o, invoker);
        }

        /// <summary>
        /// This method is for internal run-time use only.
        /// Called from Orleans generated code.
        /// </summary>
        public static void DeleteObjectReference(IAddressable observer)
        {
            GrainClient.InternalCurrent.DeleteObjectReference(observer);
        }

        /// <summary> Serializer function for grain reference. </summary>
        /// <seealso cref="SerializationManager"/>
        [SerializerMethod]
        protected internal static void SerializeGrainReference(object obj, BinaryTokenStreamWriter stream, Type expected)
        {
            GrainReference input = (GrainReference)obj;
            stream.Write(input.GrainId.Key.ToByteArray());
            stream.Write((byte) (input.SystemTargetSilo != null ? 1 : 0));
            if (input.SystemTargetSilo != null)
            {
                stream.Write(input.SystemTargetSilo);
            }
        }

        /// <summary> Deserializer function for grain reference. </summary>
        /// <seealso cref="SerializationManager"/>
        [DeserializerMethod]
        protected internal static object DeserializeGrainReference(Type t, BinaryTokenStreamReader stream)
        {
            GrainId id = stream.ReadGrainId();
            SiloAddress silo = null;
            byte siloAddressPresent = stream.ReadByte();
            if (siloAddressPresent != 0)
            {
                silo = stream.ReadSiloAddress();
            }
            GrainReference grainRef = FromGrainId(id, null, null, silo);
            return grainRef;
        }

        /// <summary> Copier function for grain reference. </summary>
        /// <seealso cref="SerializationManager"/>
        [CopierMethod]
        protected internal static object CopyGrainReference(object original)
        {
            GrainReference grain = (GrainReference)original;
            return grain;
        }

        private static String GetDebugContext(string interfaceName, string methodName, object[] arguments)
        {
            // String concatenation is approx 35% faster than string.Format here
            //debugContext = String.Format("{0}:{1}()", this.InterfaceName, GetMethodName(this.InterfaceId, methodId));
            StringBuilder debugContext = new StringBuilder();
            debugContext.Append(interfaceName);
            debugContext.Append(":");
            debugContext.Append(methodName);
            if (USE_DEBUG_CONTEXT_PARAMS && arguments != null && arguments.Length > 0)
            {
                debugContext.Append("(");
                debugContext.Append(Utils.IEnumerableToString(arguments));
                debugContext.Append(")");
            }
            else
            {
                debugContext.Append("()");
            }
            return debugContext.ToString();
        }

        private static void CheckForGrainArguments(object[] arguments)
        {
            foreach (object argument in arguments)
                if (argument is GrainBase)
                    throw new ArgumentException(String.Format("Cannot pass a grain object {0} as an argument to a method. Pass this.AsReference() instead.", argument.GetType().FullName));
        }

        private static readonly Dictionary<GrainId, Dictionary<SiloAddress, ISystemTarget>> TypedReferenceCache =
            new Dictionary<GrainId, Dictionary<SiloAddress, ISystemTarget>>();

        internal static T GetSystemTarget<T>(GrainId grainId, SiloAddress destination, Func<IAddressable, T> cast)
            where T : ISystemTarget
        {
            Dictionary<SiloAddress, ISystemTarget> cache;

            lock (TypedReferenceCache)
            {
                if (TypedReferenceCache.ContainsKey(grainId))
                    cache = TypedReferenceCache[grainId];
                else
                {
                    cache = new Dictionary<SiloAddress, ISystemTarget>();
                    TypedReferenceCache[grainId] = cache;
                }
            }
            lock (cache)
            {
                if (cache.ContainsKey(destination))
                    return (T)cache[destination];

                var reference = cast(FromGrainId(grainId, null, null, destination));
                cache[destination] = reference;
                return reference;
            }
        }

        /// <summary> Get a uniform hash code for this grain reference. </summary>
        public int GetUniformHashCode()
        {
            return GrainId.GetUniformHashCode();
        }

        /// <summary> Get the key value for this grain, as a string. </summary>
        public string ToKeyString()
        {
            return GrainId.ToParsableString();
        }


        #region ISerializable Members

        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            // Use the AddValue method to specify serialized values.
            info.AddValue("GrainId", GrainId.ToParsableString(), typeof(string));
            if (GrainId.IsSystemTarget)
            {
                info.AddValue("SystemTargetSilo", SystemTargetSilo.ToParsableString(), typeof(string));
            }
        }

        // The special constructor is used to deserialize values. 
        protected GrainReference(SerializationInfo info, StreamingContext context)
        {
            // Reset the property value using the GetValue method.
            string grainIdStr = info.GetString("GrainId");
            GrainId = GrainId.FromParsableString(grainIdStr);
            if (GrainId.IsSystemTarget)
            {
                string siloAddressStr = info.GetString("SystemTargetSilo");
                SystemTargetSilo = SiloAddress.FromParsableString(siloAddressStr);
            }
        }

        #endregion
    }
}
