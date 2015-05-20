using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.Counters;
using Orleans.Serialization;

namespace Orleans
{
    [Serializable]
    internal class Message : IOutgoingMessage
    {
        public static class Header
        {
            public const string AlwaysInterleave = "#AI";
            public const string CacheInvalidationHeader = "#CIH";
            public const string Category = "#MT";
            public const string CorrelationId = "#ID";
            public const string DebugContext = "#CTX";
            public const string Direction = "#ST";
            public const string Expiration = "#EX";
            public const string ForwardCount = "#FC";
            public const string InterfaceId = "#IID";
            public const string MethodId = "#MID";
            public const string NewGrainType = "#NT";
            public const string GenericGrainType = "#GGT";
            public const string Result = "#R";
            public const string RejectionInfo = "#RJI";
            public const string RejectionType = "#RJT";
            public const string ReadOnly = "#RO";
            public const string ReroutingRequested = "#RR";
            public const string ResendCount = "#RS";
            public const string SendingActivation = "#SA";
            public const string SendingGrain = "#SG";
            public const string SendingSilo = "#SS";

            public const string TargetActivation = "#TA";
            public const string TargetGrain = "#TG";
            public const string TargetSilo = "#TS";
            public const string Timestamps = "Times";
            public const string IsUnordered = "#UO";

            public const char ApplicationHeaderFlag = '!';
            public const string PingApplicationHeader = "Ping";

            public const string PlacementStrategy = "#PS";

            // Not currently used
            public const string ActivationGroup = "#AG";
            public const string RequestId = "#RI";
            public const string TaskType = "#TT";
            public const string DelayforConsistency = "#DC";
            public const string PriorMessageId = "#PMI";
            public const string PriorMessageTimes = "#PMT";
            public const string RetryTask = "#RT";
            public const string TaskHeader = "#TH";
        }

        public static class Metadata
        {
            public const string MaxRetries = "MaxRetries";
            //public const string RejectionReason = "Exception";
            public const string ExcludeTargetActivations = "#XA";
            public const string TARGET_HISTORY = "TargetHistory";
            public const string ActivationData = "ActivationData";
        }


        public static int LargeMessageSizeThreshold { get; set; }
        public const int LengthHeaderSize = 8;
        public const int LengthMetaHeader = 4;

        private readonly Dictionary<string, object> headers;
        [NonSerialized]
        private Dictionary<string, object> metadata;

        /// <summary>
        /// NOTE: The contents of bodyBytes should never be modified
        /// </summary>
        private List<ArraySegment<byte>> bodyBytes;

        private List<ArraySegment<byte>> headerBytes;

        private object bodyObject;

        // Cache values of TargetAddess and SendingAddress as they are used very frequently
        private ActivationAddress targetAddress;
        private ActivationAddress sendingAddress;

        internal static bool WriteMessagingTraces { get; set; }

        //public static readonly IEqualityComparer<Message> Comparer = new MessageComparer();

        private static readonly Logger logger;

        static Message()
        {
            lifecycleStatistics = new Dictionary<string, TransitionStats[,]>();
            logger = Logger.GetLogger("Message", Logger.LoggerType.Runtime);
            //Logger.RegisterPeriodicAction("Message", GetTransitionData);
        }

        public enum Categories
        {
            Ping,
            System,
            //Routing,
            //Task,
            Application,
        }

        public enum Directions
        {
            Request,
            Response,
            OneWay
        }

        public enum ResponseTypes
        {
            Success,
            Error,
            Rejection
        }

        public enum RejectionTypes
        {
            Transient,
            FutureTransient, // should be Transient (that is, retriable), but was not tested yet.
            DuplicateRequest,
            Unrecoverable,
            GatewayTooBusy,
        }

        public Categories Category
        {
            get { return GetScalarHeader<Categories>(Header.Category); }
            set { SetHeader(Header.Category, value); }
        }

        public Directions Direction
        {
            get { return GetScalarHeader<Directions>(Header.Direction); }
            set { SetHeader(Header.Direction, value); }
        }

        public bool IsReadOnly
        {
            get { return GetScalarHeader<bool>(Header.ReadOnly); }
            set { SetHeader(Header.ReadOnly, value); }
        }

        public bool IsAlwaysInterleave
        {
            get { return GetScalarHeader<bool>(Header.AlwaysInterleave); }
            set { SetHeader(Header.AlwaysInterleave, value); }
        }

        public bool IsUnordered
        {
            get { return GetScalarHeader<bool>(Header.IsUnordered); }
            set
            {
                if (value || ContainsHeader(Header.IsUnordered))
                    SetHeader(Header.IsUnordered, value);
            }
        }

        public CorrelationId Id
        {
            get { return GetSimpleHeader<CorrelationId>(Header.CorrelationId); }
            set { SetHeader(Header.CorrelationId, value); }
        }

        public int ResendCount
        {
            get { return GetScalarHeader<int>(Header.ResendCount); }
            set { SetHeader(Header.ResendCount, value); }
        }

        public int ForwardCount
        {
            get { return GetScalarHeader<int>(Header.ForwardCount); }
            set { SetHeader(Header.ForwardCount, value); }
        }

        public SiloAddress TargetSilo
        {
            get { return (SiloAddress)GetHeader(Header.TargetSilo); }
            set
            {
                SetHeader(Header.TargetSilo, value);
                targetAddress = null;
            }
        }

        public GrainId TargetGrain
        {
            get { return GetSimpleHeader<GrainId>(Header.TargetGrain); }
            set
            {
                SetHeader(Header.TargetGrain, value);
                targetAddress = null;
            }
        }

        public ActivationId TargetActivation
        {
            get { return GetSimpleHeader<ActivationId>(Header.TargetActivation); }
            set
            {
                SetHeader(Header.TargetActivation, value);
                targetAddress = null;
            }
        }

        public ActivationAddress TargetAddress
        {
            get
            {
                if (targetAddress == null)
                    targetAddress = ActivationAddress.GetAddress(TargetSilo, TargetGrain, TargetActivation);
                return targetAddress;
            }
            set
            {
                TargetGrain = value.Grain;
                TargetActivation = value.Activation;
                TargetSilo = value.Silo;
                targetAddress = value;
            }
        }

        public SiloAddress SendingSilo
        {
            get { return (SiloAddress)GetHeader(Header.SendingSilo); }
            set
            {
                SetHeader(Header.SendingSilo, value);
                sendingAddress = null;
            }
        }

        public GrainId SendingGrain
        {
            get { return GetSimpleHeader<GrainId>(Header.SendingGrain); }
            set
            {
                SetHeader(Header.SendingGrain, value);
                sendingAddress = null;
            }
        }

        public ActivationId SendingActivation
        {
            get { return GetSimpleHeader<ActivationId>(Header.SendingActivation); }
            set
            {
                SetHeader(Header.SendingActivation, value);
                sendingAddress = null;
            }
        }

        public ActivationAddress SendingAddress
        {
            get
            {
                if (sendingAddress == null)
                    sendingAddress = ActivationAddress.GetAddress(SendingSilo, SendingGrain, SendingActivation);
                return sendingAddress;
            }
            set
            {
                SendingGrain = value.Grain;
                SendingActivation = value.Activation;
                SendingSilo = value.Silo;
                sendingAddress = value;
            }
        }

        public ResponseTypes Result
        {
            get { return GetScalarHeader<ResponseTypes>(Header.Result); }
            set { SetHeader(Header.Result, value); }
        }

        public DateTime Expiration
        {
            get { return GetScalarHeader<DateTime>(Header.Expiration); }
            set { SetHeader(Header.Expiration, value); }
        }

        public bool IsExpired
        {
            get
            {
                if (ContainsHeader(Header.Expiration))
                {
                    return DateTime.UtcNow > Expiration;
                }
                return false;
            }
        }

        public bool IsExpirableMessage(IMessagingConfiguration config)
        {
            if (config.DropExpiredMessages)
            {
                GrainId id = TargetGrain;
                if (id != null)
                {
                    // don't set expiration for one way, system target and system grain messages.
                    if (Direction != Message.Directions.OneWay && !id.IsSystemTarget && !Constants.IsSystemGrain(id))
                    {
                        return true;
                    }
                }
            }
            return false;
        }


        public string DebugContext
        {
            get { return GetStringHeader(Header.DebugContext); }
            set { SetHeader(Header.DebugContext, value); }
        }

        public IEnumerable<ActivationAddress> CacheInvalidationHeader
        {
            get
            {
                object obj = GetHeader(Header.CacheInvalidationHeader);
                if (obj == null) return null;
                return ((IEnumerable)obj).Cast<ActivationAddress>();
            }
        }

        public PlacementStrategy PlacementStrategy
        {
            get { return (PlacementStrategy)GetHeader(Header.PlacementStrategy); }
            set
            {
                if (null == value)
                    RemoveHeader(Header.PlacementStrategy);
                else if (!Object.Equals(PlacementStrategy, value))
                    SetHeader(Header.PlacementStrategy, value);
            }
        }

        internal void AddToCacheInvalidationHeader(ActivationAddress address)
        {
            List<ActivationAddress> list = new List<ActivationAddress>();
            if (ContainsHeader(Message.Header.CacheInvalidationHeader))
            {
                IEnumerable<ActivationAddress> prevList = ((IEnumerable)GetHeader(Header.CacheInvalidationHeader)).Cast<ActivationAddress>();
                list.AddRange(prevList);
            }
            list.Add(address);
            SetHeader(Header.CacheInvalidationHeader, list);
        }

        // Resends are used by the sender, usualy due to en error to send or due to a transient rejection.
        public bool MayResend(IMessagingConfiguration config)
        {
            return ResendCount < config.MaxResendCount;
        }

        // Forwardings are used by the receiver, usualy when it cannot process the message and forwars it to another silo to perform the processing
        // (got here due to outdated cache, silo is shutting down/overloaded, ...).
        public bool MayForward(GlobalConfiguration config)
        {
            return ForwardCount < config.MaxForwardCount;
        }

        public int MethodId
        {
            get { return GetScalarHeader<int>(Header.MethodId); }
            set { SetHeader(Header.MethodId, value); }
        }

        public int InterfaceId
        {
            get { return GetScalarHeader<int>(Header.InterfaceId); }
            set { SetHeader(Header.InterfaceId, value); }
        }

        /// <summary>
        /// Set by sender's placement logic when NewPlacementRequested is true
        /// so that receiver knows desired grain type
        /// </summary>
        public string NewGrainType
        {
            get { return GetStringHeader(Header.NewGrainType); }
            set { SetHeader(Header.NewGrainType, value); }
        }

        /// <summary>
        /// Set by caller's grain reference 
        /// </summary>
        public string GenericGrainType
        {
            get { return GetStringHeader(Header.GenericGrainType); }
            set { SetHeader(Header.GenericGrainType, value); }
        }

        public RejectionTypes RejectionType
        {
            get { return GetScalarHeader<RejectionTypes>(Header.RejectionType); }
            set { SetHeader(Header.RejectionType, value); }
        }

        public string RejectionInfo
        {
            get { return GetStringHeader(Header.RejectionInfo); }
            set { SetHeader(Header.RejectionInfo, value); }
        }


        //public bool DelayForConsistency
        //{
        //    get { return GetScalarHeader<bool>(Header.DelayforConsistency); }
        //    set { SetHeader(Header.DelayforConsistency, value); }
        //}

        public object BodyObject
        {
            get
            {
                if (bodyObject != null)
                {
                    return bodyObject;
                }
                if (bodyBytes == null)
                {
                    return null;
                }
                try
                {
                    var stream = new BinaryTokenStreamReader(bodyBytes);
                    bodyObject = SerializationManager.Deserialize(stream);
                }
                catch (Exception ex)
                {
                    logger.Error(ErrorCode.Messaging_UnableToDeserializeBody, "Exception deserializing message body", ex);
                    throw;
                }
                finally
                {
                    BufferPool.GlobalPool.Release(bodyBytes);
                    bodyBytes = null;
                }
                return bodyObject;
            }
            set
            {
                bodyObject = value;
                if (bodyBytes != null)
                {
                    BufferPool.GlobalPool.Release(bodyBytes);
                    bodyBytes = null;
                }
            }
        }

        public Message()
        {
            headers = new Dictionary<string, object>();
            metadata = new Dictionary<string, object>();
            bodyObject = null;
            bodyBytes = null;
            headerBytes = null;
        }

        public Message(Categories type, Directions subtype)
            : this()
        {
            Category = type;
            Direction = subtype;
        }

        internal Message(byte[] header, byte[] body)
            : this(new List<ArraySegment<byte>> { new ArraySegment<byte>(header) },
                new List<ArraySegment<byte>> { new ArraySegment<byte>(body) })
        {
        }

        public Message(List<ArraySegment<byte>> header, List<ArraySegment<byte>> body)
        {
            metadata = new Dictionary<string, object>();

            var input = new BinaryTokenStreamReader(header);
            headers = SerializationManager.DeserializeMessageHeaders(input);
            BufferPool.GlobalPool.Release(header);

            bodyBytes = body;
            bodyObject = null;
            headerBytes = null;
        }

        public Message CreateResponseMessage()
        {
            Message response = new Message(this.Category, Directions.Response);

            response.Id = this.Id;
            response.IsReadOnly = this.IsReadOnly;
            response.IsAlwaysInterleave = this.IsAlwaysInterleave;
            response.TargetSilo = this.SendingSilo;
            if (this.ContainsHeader(Header.SendingGrain))
            {
                response.SetHeader(Header.TargetGrain, this.GetHeader(Header.SendingGrain));
                if (this.ContainsHeader(Header.SendingActivation))
                {
                    response.SetHeader(Header.TargetActivation, this.GetHeader(Header.SendingActivation));
                }
            }

            response.SendingSilo = this.TargetSilo;
            if (this.ContainsHeader(Header.TargetGrain))
            {
                response.SetHeader(Header.SendingGrain, this.GetHeader(Header.TargetGrain));
                if (this.ContainsHeader(Header.TargetActivation))
                {
                    response.SetHeader(Header.SendingActivation, this.GetHeader(Header.TargetActivation));
                }
                else if (this.TargetGrain.IsSystemTarget)
                {
                    response.SetHeader(Header.SendingActivation, ActivationId.GetSystemActivation(TargetGrain, TargetSilo));
                }
            }

            if (this.ContainsHeader(Header.Timestamps))
            {
                response.SetHeader(Header.Timestamps, this.GetHeader(Header.Timestamps));
            }
            if (this.ContainsHeader(Header.DebugContext))
            {
                response.SetHeader(Header.DebugContext, this.GetHeader(Header.DebugContext));
            }
            if (this.ContainsHeader(Header.CacheInvalidationHeader))
            {
                response.SetHeader(Header.CacheInvalidationHeader, this.GetHeader(Header.CacheInvalidationHeader));
            }
            if (this.ContainsHeader(Header.Expiration))
            {
                response.SetHeader(Header.Expiration, this.GetHeader(Header.Expiration));
            }
            response.AddTimestamp(LifecycleTag.CreateResponse);

            RequestContext.ExportToMessage(response);

            return response;
        }

        public Message CreateRejectionResponse(RejectionTypes type, string info)
        {
            Message response = CreateResponseMessage();
            //TODO: why do we need to send the original msg back?
            // ageller -- I think the original notion was that this meant that the sender's message center could resend the message directly when
            // it received the rejection, without going up the stack -- but it can't anyway because it doesn't have all the headers, so there's
            // actually no reason to do this.
            //response.bodyBytes = bodyBytes;
            response.Result = ResponseTypes.Rejection;
            response.RejectionType = type;
            //response.RemoveHeader(Header.SendingSilo); // Rejections always come from the silo that sends them -- CHANGED to always come from the *intended* target silo
            response.RejectionInfo = info;
            if (logger.IsVerbose) logger.Verbose("Creating {0} rejection with info '{1}' for {2} at:\r\n{3}", type, info, this, new System.Diagnostics.StackTrace(true));
            return response;
        }

        public bool ContainsHeader(string tag)
        {
            return headers.ContainsKey(tag);
        }

        public void RemoveHeader(string tag)
        {
            lock (headers)
            {
                headers.Remove(tag);
                if (tag == Header.TargetActivation || tag == Header.TargetGrain | tag == Header.TargetSilo)
                    targetAddress = null;
            }
        }

        public void SetHeader(string tag, object value)
        {
            lock (headers)
            {
                headers[tag] = value;
            }
        }

        public object GetHeader(string tag)
        {
            object val;
            bool flag;
            lock (headers)
            {
                flag = headers.TryGetValue(tag, out val);
            }
            if (flag)
            {
                return val;
            }
            else
            {
                return null;
            }
        }

        public string GetStringHeader(string tag)
        {
            object val;
            if (headers.TryGetValue(tag, out val))
            {
                string s = val as string;
                if (s != null)
                {
                    return s;
                }
            }
            return "";
        }

        public T GetScalarHeader<T>(string tag)// where T : struct
        {
            object val;
            if (headers.TryGetValue(tag, out val))
            {
                return (T)val;
            }
            return default(T);
        }

        public T GetSimpleHeader<T>(string tag)
        //where T : new()
        {
            object val;
            if (headers.TryGetValue(tag, out val))
            {
                if (val != null)
                {
                    if (val is T)
                    {
                        return (T)val;
                    }
                }
            }
            return default(T);
        }

        internal void SetApplicationHeaders(Dictionary<string, object> data)
        {
            lock (headers)
            {
                foreach (var item in data)
                {
                    string key = Header.ApplicationHeaderFlag + item.Key;
                    headers[key] = SerializationManager.DeepCopy(item.Value);
                }
            }
        }

        internal void GetApplicationHeaders(Dictionary<string, object> dict)
        {
            TryGetApplicationHeaders(ref dict);
        }

        private void TryGetApplicationHeaders(ref Dictionary<string, object> dict)
        {
            lock (headers)
            {
                foreach (var pair in headers)
                {
                    if (pair.Key[0] == Header.ApplicationHeaderFlag)
                    {
                        if (dict == null)
                        {
                            dict = new Dictionary<string, object>();
                        }
                        dict[pair.Key.Substring(1)] = pair.Value;
                    }
                }
            }
        }

        public object GetApplicationHeader(string headerName)
        {
            lock (headers)
            {
                object obj;
                if (headers.TryGetValue(Header.ApplicationHeaderFlag + headerName, out obj))
                {
                    return obj;
                }
                return null;
            }
        }

        public bool ContainsMetadata(string tag)
        {
            return metadata != null && metadata.ContainsKey(tag);
        }

        public void SetMetadata(string tag, object data)
        {
            metadata = metadata ?? new Dictionary<string, object>();
            metadata[tag] = data;
        }

        public void RemoveMetadata(string tag)
        {
            if (metadata != null)
            {
                metadata.Remove(tag);
            }
        }

        public object GetMetadata(string tag)
        {
            object data;
            if (metadata != null && metadata.TryGetValue(tag, out data))
            {
                return data;
            }
            return null;
        }

        /// <summary>
        /// Tell whether two messages are duplicates of one another
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public bool IsDuplicate(Message other)
        {
            return Equals(SendingSilo, other.SendingSilo) && Equals(Id, other.Id);
        }

        #region Message timestamping

        private class TransitionStats
        {
            ulong count;
            TimeSpan totalTime;
            TimeSpan maxTime;

            public TransitionStats()
            {
                count = 0;
                totalTime = TimeSpan.Zero;
                maxTime = TimeSpan.Zero;
            }

            public void RecordTransition(TimeSpan time)
            {
                lock (this)
                {
                    count++;
                    totalTime += time;
                    if (time > maxTime)
                    {
                        maxTime = time;
                    }
                }
            }

            public override string ToString()
            {
                StringBuilder sb = new StringBuilder();

                if (count > 0)
                {
                    sb.AppendFormat("{0}\t{1}\t{2}", count, totalTime.Divide(count), maxTime);
                }

                return sb.ToString();
            }

            //public bool IsEmpty { get { return count == 0; } }
        }

        private static readonly Dictionary<string, TransitionStats[,]> lifecycleStatistics;

        public void AddTimestamp(LifecycleTag tag)
        {
            if (logger.IsVerbose2)
            {
                if (LogVerbose(tag))
                    logger.Verbose("Message {0} {1}", tag, this);
                else if (logger.IsVerbose2)
                    logger.Verbose2("Message {0} {1}", tag, this);
            }

            if (WriteMessagingTraces)
            {
                DateTime now = DateTime.UtcNow;
                List<object> timestamp = new List<object>();
                timestamp.Add(tag);
                timestamp.Add(now);
                object val;
                List<object> list = null;
                if (headers.TryGetValue(Header.Timestamps, out val))
                {
                    list = val as List<object>;
                }
                if (list == null)
                {
                    list = new List<object>();
                    lock (headers)
                    {
                        headers[Header.Timestamps] = list;
                    }
                }
                else if (list.Count > 0)
                {
                    var last = list[list.Count - 1] as List<object>;
                    if (last != null)
                    {
                        var context = DebugContext;
                        if (String.IsNullOrEmpty(context))
                        {
                            context = "Unspecified";
                        }
                        TransitionStats[,] entry;
                        bool found;
                        lock (lifecycleStatistics)
                        {
                            found = lifecycleStatistics.TryGetValue(context, out entry);
                        }
                        if (!found)
                        {
                            var newEntry = new TransitionStats[32, 32];
                            for (int i = 0; i < 32; i++) for (int j = 0; j < 32; j++) newEntry[i, j] = new TransitionStats();
                            lock (lifecycleStatistics)
                            {
                                if (!lifecycleStatistics.TryGetValue(context, out entry))
                                {
                                    entry = newEntry;
                                    lifecycleStatistics.Add(context, entry);
                                }
                            }
                        }
                        int from = (int)(LifecycleTag)(last[0]);
                        int to = (int)tag;
                        entry[from, to].RecordTransition(now.Subtract((DateTime)last[1]));
                    }
                }
                list.Add(timestamp);
            }
            if (OnTrace != null)
                OnTrace(this, tag);
        }

        private static bool LogVerbose(LifecycleTag tag)
        {
            return tag == LifecycleTag.EnqueueOutgoing ||
                   tag == LifecycleTag.CreateNewPlacement ||
                   tag == LifecycleTag.EnqueueIncoming ||
                   tag == LifecycleTag.InvokeIncoming;
        }

        public List<Tuple<string, DateTime>> GetTimestamps()
        {
            List<Tuple<string, DateTime>> result = new List<Tuple<string, DateTime>>();

            object val;
            List<object> list = null;
            if (headers.TryGetValue(Header.Timestamps, out val))
            {
                list = val as List<object>;
            }
            if (list != null)
            {
                foreach (object item in list)
                {
                    List<object> stamp = item as List<object>;
                    if ((stamp != null) && (stamp.Count == 2) && (stamp[0] is string) && (stamp[1] is DateTime))
                    {
                        result.Add(new Tuple<string, DateTime>(stamp[0] as string, (DateTime)stamp[1]));
                    }
                }
            }

            return result;
        }

        public string GetTimestampString(bool singleLine = true, bool includeTimes = true, int indent = 0)
        {
            StringBuilder sb = new StringBuilder();

            object val;
            List<object> list = null;
            if (headers.TryGetValue(Header.Timestamps, out val))
            {
                list = val as List<object>;
            }
            if (list != null)
            {
                bool firstItem = true;
                string indentString = new string(' ', indent);
                foreach (object item in list)
                {
                    List<object> stamp = item as List<object>;
                    if ((stamp != null) && (stamp.Count == 2) && (stamp[0] is string) && (stamp[1] is DateTime))
                    {
                        if (!firstItem && singleLine)
                        {
                            sb.Append(", ");
                        }
                        else if (!singleLine && (indent > 0))
                        {
                            sb.Append(indentString);
                        }
                        sb.Append(stamp[0]);
                        if (includeTimes)
                        {
                            sb.Append(" ==> ");
                            DateTime when = (DateTime)stamp[1];
                            sb.Append(when.ToString("HH:mm:ss.ffffff"));
                        }
                        if (!singleLine)
                        {
                            sb.AppendLine();
                        }
                        firstItem = false;
                    }
                }
            }

            return sb.ToString();
        }

        #endregion

        #region Serialization

        internal List<ArraySegment<byte>> Serialize()
        {
            int dummy1;
            int dummy2;
            return Serialize_Impl(false, out dummy1, out dummy2);
        }

        public List<ArraySegment<byte>> Serialize(out int headerLength)
        {
            int dummy;
            return Serialize_Impl(false, out headerLength, out dummy);
        }

        public List<ArraySegment<byte>> SerializeForBatching(out int headerLength, out int bodyLength)
        {
            return Serialize_Impl(true, out headerLength, out bodyLength);
        }

        // List<ArraySegment<byte> (instead of List<byte[]>) is required for the Socket API
        private List<ArraySegment<byte>> Serialize_Impl(bool batching, out int headerLengthOut, out int bodyLengthOut)
        {
            var headerStream = new BinaryTokenStreamWriter();
            lock (headers) // Guard against any attempts to modify message headers while we are serializing them
            {
                SerializationManager.SerializeMessageHeaders(headers, headerStream);
            }

            if (bodyBytes == null)
            {
                var bodyStream = new BinaryTokenStreamWriter();
                SerializationManager.Serialize(bodyObject, bodyStream);
                // We don't bother to turn this into a byte array and save it in bodyBytes because Serialize only gets called on a message
                // being sent off-box. In this case, the likelihood of needed to re-serialize is very low, and the cost of capturing the
                // serialized bytes from the steam -- where they're a list of ArraySegment objects -- into an array of bytes is actually
                // pretty high (an array allocation plus a bunch of copying).
                bodyBytes = bodyStream.ToBytes();
            }

            if (headerBytes != null)
            {
                BufferPool.GlobalPool.Release(headerBytes);
            }
            headerBytes = headerStream.ToBytes();
            int headerLength = headerBytes.Sum(ab => ab.Count);
            int bodyLength = bodyBytes.Sum(ab => ab.Count);

            //MessagingStatisticsGroup.HeaderBytesSent.IncrementBy(headerLength);

            var bytes = new List<ArraySegment<byte>>();
            if (!batching)
            {
                bytes.Add(new ArraySegment<byte>(BitConverter.GetBytes(headerLength)));
                bytes.Add(new ArraySegment<byte>(BitConverter.GetBytes(bodyLength)));
            }
            bytes.AddRange(headerBytes);
            bytes.AddRange(bodyBytes);

            if (headerLength + bodyLength > LargeMessageSizeThreshold)
            {
                logger.Info(ErrorCode.Messaging_LargeMsg_Outgoing, "Preparing to send large message Size={0} HeaderLength={1} BodyLength={2} #ArraySegments={3}. Msg={4}",
                    headerLength + bodyLength + LengthHeaderSize, headerLength, bodyLength, bytes.Count, this.ToString());
                if (logger.IsVerbose3) logger.Verbose3("Sending large message {0}", this.ToLongString());
            }

            headerLengthOut = headerLength;
            bodyLengthOut = bodyLength;
            return bytes;
        }


        public void ReleaseBodyAndHeaderBuffers()
        {
            ReleaseHeadersOnly();
            ReleaseBodyOnly();
        }

        public void ReleaseHeadersOnly()
        {
            if (headerBytes != null)
            {
                BufferPool.GlobalPool.Release(headerBytes);
                headerBytes = null;
            }
        }

        public void ReleaseBodyOnly()
        {
            if (bodyBytes != null)
            {
                BufferPool.GlobalPool.Release(bodyBytes);
                bodyBytes = null;
            }
        }

        #endregion

        // For testing and logging/tracing
        public string ToLongString()
        {
            var sb = new StringBuilder();

            string debugContex = DebugContext;
            if (!string.IsNullOrEmpty(debugContex))
            {
                // if DebugContex is present, print it first.
                sb.Append(debugContex).Append(".");
            }

            lock (headers)
            {
                foreach (var pair in headers)
                {
                    if (pair.Key != Header.DebugContext)
                    {
                        sb.AppendFormat("{0}={1};", pair.Key, pair.Value);
                    }
                    sb.AppendLine();
                }
            }

            return sb.ToString();
        }

        public override string ToString()
        {
            string response = "";
            if (Direction == Directions.Response)
            {
                if (Result == ResponseTypes.Error)
                    response = "Error ";
                else if (Result == ResponseTypes.Rejection)
                {
                    response = string.Format("{0} Rejection (info: {1}) ", RejectionType, RejectionInfo);
                }
            }
            string times = this.GetStringHeader(Header.Timestamps);
            return String.Format("{0}{1}{2}{3}{4} {5}->{6} #{7}{8}{9}{10}: {11}",
                IsReadOnly ? "ReadOnly " : "", //0
                IsAlwaysInterleave ? "IsAlwaysInterleave " : "", //1
                PlacementStrategy != null ? "NewPlacement " : "", // 2
                response,  //3
                Direction, //4
                String.Format("{0}{1}{2}", SendingSilo, SendingGrain, SendingActivation), //5  //SendingAddress.ToString() - this may throw
                String.Format("{0}{1}{2}", TargetSilo, TargetGrain, TargetActivation), //6  //TargetAddress.ToString() - this may throw 
                Id, //7
                ResendCount > 0 ? "[ResendCount=" + ResendCount + "]" : "", //8
                ForwardCount > 0 ? "[ForwardCount=" + ForwardCount + "]" : "", //9
                string.IsNullOrEmpty(times) ? "" : "[" + times + "]", //10
                DebugContext); //11
        }

        /// <summary>
        /// Tags used to identify points in the message processing lifecycle for logging.
        /// Should be fewer than 32 since bit flags are used for filtering events.
        /// </summary>
        public enum LifecycleTag
        {
            Create = 0,
            EnqueueOutgoing,
            StartRouting,
            AsyncRouting,
            DoneRouting,
            SendOutgoing,
            ReceiveIncoming,
            RerouteIncoming,
            EnqueueForRerouting,
            EnqueueForForwarding,
            EnqueueIncoming,
            DequeueIncoming,
            CreateNewPlacement,
            TaskIncoming,
            TaskRedirect,
            EnqueueWaiting,
            EnqueueReady,
            EnqueueWorkItem,
            DequeueWorkItem,
            InvokeIncoming,
            CreateResponse,
        }

        /// <summary>
        /// Global function that is set to monitor message lifecycle events
        /// </summary>
        internal static Action<Message, LifecycleTag> OnTrace { private get; set; }

        internal void SetTargetPlacement(PlacementResult value)
        {
            if ((value.IsNewPlacement ||
                     (ContainsHeader(Header.TargetActivation) &&
                     !TargetActivation.Equals(value.Activation))))
            {
                RemoveHeader(Header.PriorMessageId);
                RemoveHeader(Header.PriorMessageTimes);
            }
            TargetActivation = value.Activation;
            TargetSilo = value.Silo;

            if (value.IsNewPlacement)
            {
                PlacementStrategy = value.PlacementStrategy;
            }
            else
            {
                RemoveHeader(Header.PlacementStrategy);
                GenericGrainType = null; // generic type information is only needed for new placements
            }

            if (!String.IsNullOrEmpty(value.GrainType))
                NewGrainType = value.GrainType;
        }


        public string GetTargetHistory()
        {
            StringBuilder history = new StringBuilder();
            history.Append("<");
            if (ContainsHeader(Message.Header.TargetSilo))
            {
                history.Append(TargetSilo).Append(":");
            }
            if (ContainsHeader(Message.Header.TargetGrain))
            {
                history.Append(TargetGrain).Append(":");
            }
            if (ContainsHeader(Message.Header.TargetActivation))
            {
                history.Append(TargetActivation);
            }
            history.Append(">");
            if (ContainsMetadata(Message.Metadata.TARGET_HISTORY))
            {
                history.Append("    ").Append(GetMetadata(Message.Metadata.TARGET_HISTORY));
            }
            return history.ToString();
        }

        public bool IsSameDestination(IOutgoingMessage other)
        {
            Message msg = (Message)other;
            if (msg == null) return false;
            return Object.Equals(TargetSilo, msg.TargetSilo);
        }

        internal ActivationData ActivationDataMetadata
        {
            get { return (ActivationData)GetMetadata(Metadata.ActivationData); }
            set { SetMetadata(Metadata.ActivationData, value); }
        }

        internal void DropExpiredMessage(MessagingStatisticsGroup.Phase phase)
        {
            MessagingStatisticsGroup.OnMessageExpired(phase);
            if (logger.IsVerbose2) logger.Verbose2("Dropping an expired message: {0}", this);
            this.ReleaseBodyAndHeaderBuffers();
        }
    }
}
