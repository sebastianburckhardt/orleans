using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.StorageClient;
using Orleans.AzureUtils;
using Orleans.Providers;
using Orleans.Serialization;


namespace Orleans.Storage
{
    /// <summary>
    /// Simple storage provider for writing grain state data to Azure table storage.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Required configuration params: <c>DataConnectionString</c>
    /// </para>
    /// <para>
    /// Optional configuration params: 
    /// <c>TableName</c> -- defaults to <c>OrleansGrainState</c>
    /// <c>DeleteStateOnClear</c> -- defaults to <c>false</c>
    /// </para>
    /// </remarks>
    /// <example>
    /// Example configuration for this storage provider in OrleansConfiguration.xml file:
    /// <code>
    /// &lt;OrleansConfiguration xmlns="urn:orleans">
    ///   &lt;Globals>
    ///     &lt;StorageProviders>
    ///       &lt;Provider Type="Orleans.Storage.AzureTableStorage" Name="AzureStore"
    ///         DataConnectionString="UseDevelopmentStorage=true"
    ///         DeleteStateOnClear="true"
    ///       />
    ///   &lt;/StorageProviders>
    /// </code>
    /// </example>
    public class AzureTableStorage : IStorageProvider
    {
        private const string DATA_CONNECTION_STRING = "DataConnectionString";
        private const string TABLE_NAME_PROPERTY = "TableName";
        private const string DELETE_ON_CLEAR_PROPERTY = "DeleteStateOnClear";
        private const string GRAIN_STATE_TABLE_NAME_DEFAULT = "OrleansGrainState";
        private string _dataConnectionString;
        private string _tableName;
        private GrainStateTableDataManager _tableDataManager;
        private bool _isDeleteStateOnClear;
        private static int _counter;
        private readonly int _id;

#if !DISABLE_STREAMS
        private const string USE_JSON_FORMAT_PROPERTY = "UseJsonFormat";
        private bool _useJsonFormat;
        private Newtonsoft.Json.JsonSerializerSettings _jsonSettings;
#endif
        /// <summary> Name of this storage provider instance. </summary>
        /// <see cref="IOrleansProvider#Name"/>
        public string Name { get; private set; }
        /// <summary> Logger used by this storage provider instance. </summary>
        /// <see cref="IStorageProvider#Log"/>
        public OrleansLogger Log { get; private set; }

        /// <summary>
        /// Default constructor
        /// </summary>
        public AzureTableStorage()
        {
            _tableName = GRAIN_STATE_TABLE_NAME_DEFAULT;
            _id = Interlocked.Increment(ref _counter);
        }

        /// <summary> Initialization function for this storage provider. </summary>
        /// <see cref="IOrleansProvider#Init"/>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            this.Name = name;

            if (!config.Properties.ContainsKey(DATA_CONNECTION_STRING) || string.IsNullOrWhiteSpace(config.Properties[DATA_CONNECTION_STRING]))
                throw new ArgumentException("DataConnectionString property not set");
            _dataConnectionString = config.Properties["DataConnectionString"];

            if (config.Properties.ContainsKey(TABLE_NAME_PROPERTY))
                _tableName = config.Properties[TABLE_NAME_PROPERTY];

            _isDeleteStateOnClear = config.Properties.ContainsKey(DELETE_ON_CLEAR_PROPERTY) &&
                "true".Equals(config.Properties[DELETE_ON_CLEAR_PROPERTY], StringComparison.OrdinalIgnoreCase);

            Log = providerRuntime.GetLogger("Storage.AzureTableStorage." + _id, Logger.LoggerType.Runtime);

#if !DISABLE_STREAMS
            if (config.Properties.ContainsKey(USE_JSON_FORMAT_PROPERTY))
            {
                _useJsonFormat = "true".Equals(config.Properties[USE_JSON_FORMAT_PROPERTY], StringComparison.OrdinalIgnoreCase);
            }
            if (_useJsonFormat)
            {
                _jsonSettings = new Newtonsoft.Json.JsonSerializerSettings();
                _jsonSettings.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.All;
            }
            Log.Info((int)ProviderErrorCode.AzureTableProvider_InitProvider, "Init: Name={0} Table={1} DeleteStateOnClear={2}{3}",
                    Name, _tableName, _isDeleteStateOnClear, (config.Properties.ContainsKey(USE_JSON_FORMAT_PROPERTY) ? ", UseJsonFormat=" + _useJsonFormat : ""));
#else
            Log.Info((int)ProviderErrorCode.AzureTableProvider_InitProvider, "Init: Name={0} Table={1} DeleteStateOnClear={2}", 
                    Name, _tableName, _isDeleteStateOnClear);
#endif

            Log.Info((int)ProviderErrorCode.AzureTableProvider_ParamConnectionString, "AzureTableStorage Provider is using DataConnectionString: {0}", ConfigUtilities.PrintDataConnectionInfo(_dataConnectionString));
            _tableDataManager = new GrainStateTableDataManager(_tableName, _dataConnectionString);
            return _tableDataManager.InitTableAsync();
        }

        // Internal method to initialize for testing
        internal void InitLogger(OrleansLogger logger)
        {
            Log = logger;
        }

        /// <summary> Shutdown this storage provider. </summary>
        /// <see cref="IStorageProvider#Close"/>
        public Task Close()
        {
            _tableDataManager = null;
            return TaskDone.Done;
        }

        /// <summary> Read state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider#ReadStateAsync"/>
        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (_tableDataManager == null) throw new ArgumentException("GrainState-Table property not initialized");
            string pk = grainReference.ToKeyString();
            if (Log.IsVerbose3) Log.Verbose3((int)ProviderErrorCode.AzureTableProvider_ReadingData, "Reading: GrainType={0} Pk={1} Grainid={2} from Table={3}", grainType, pk, grainReference, _tableName);
            string partitionKey = pk;
            string rowKey = grainType;
            GrainStateRecord record = await _tableDataManager.Read(partitionKey, rowKey);
            if (record != null)
            {
                var entity = record.Entity;
                if (entity != null)
                {
                    ConvertFromStorageFormat(grainState, entity);
                    grainState.Etag = record.ETag;
                }
            }
            // Else leave grainState in previous default condition
        }

        /// <summary> Write state data function for this storage provider. </summary>
        /// <see cref="IStorageProvider#WriteStateAsync"/>
        public async Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (_tableDataManager == null) 
                throw new ArgumentException("GrainState-Table property not initialized");
            string pk = grainReference.ToKeyString();
            if (Log.IsVerbose3)
                Log.Verbose3((int)ProviderErrorCode.AzureTableProvider_WritingData, "Writing: GrainType={0} Pk={1} Grainid={2} ETag={3} to Table={4}", grainType, pk, grainReference, grainState.Etag, _tableName);

            var entity = new GrainStateEntity { PartitionKey = pk, RowKey = grainType };
            ConvertToStorageFormat(grainState, entity);
            var record = new GrainStateRecord { Entity = entity, ETag = grainState.Etag };
            try
            {
                await _tableDataManager.Write(record);
                grainState.Etag = record.ETag;
            }
            catch (Exception exc)
            {
                Log.Error((int)ProviderErrorCode.AzureTableProvider_WriteError,
                    string.Format("Error Writing: GrainType={0} Grainid={1} ETag={2} to Table={3} Exception={4}", grainType, grainReference, grainState.Etag, _tableName, exc.Message), 
                    exc);
                throw;
            }
        }

        /// <summary> Clear / Delete state data function for this storage provider. </summary>
        /// <remarks>
        /// If the <c>DeleteStateOnClear</c> is set to <c>true</c> then the table row 
        /// for this grain will be deleted / removed, otherwise the table row will be 
        /// cleared by overwriting with default / null values.
        /// </remarks>
        /// <see cref="IStorageProvider#ClearStateAsync"/>
        public async Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (_tableDataManager == null) throw new ArgumentException("GrainState-Table property not initialized");
            string pk = grainReference.ToKeyString();
            if (Log.IsVerbose3) Log.Verbose3((int)ProviderErrorCode.AzureTableProvider_WritingData, "Clearing: GrainType={0} Pk={1} Grainid={2} ETag={3} DeleteStateOnClear={4} from Table={5}", grainType, pk, grainReference, grainState.Etag, _isDeleteStateOnClear, _tableName);
            var entity = new GrainStateEntity { PartitionKey = pk, RowKey = grainType };
            var record = new GrainStateRecord { Entity = entity, ETag = grainState.Etag };
            string operation = "Clearing";
            try
            {
                if (_isDeleteStateOnClear)
                {
                    operation = "Deleting";
                    await _tableDataManager.Delete(record);
                }
                else
                {
                    await _tableDataManager.Write(record);
                }
                grainState.Etag = record.ETag; // Update in-memory data to the new ETag
            }
            catch (Exception exc)
            {
                Log.Error((int)ProviderErrorCode.AzureTableProvider_DeleteError,
                    string.Format("Error {0}: GrainType={1} Grainid={2} ETag={3} from Table={4} Exception={5}", operation, grainType, grainReference, grainState.Etag, _tableName, exc.Message),
                    exc);
                throw;
            }
        }

        /// <summary>
        /// Serialize to Azure storage format in either binary or JSON format.
        /// </summary>
        /// <param name="grainState">The grain state data to be serialized</param>
        /// <param name="entity">The Azure table entity the data should be stored in</param>
        /// <remarks>
        /// See:
        /// http://msdn.microsoft.com/en-us/library/system.web.script.serialization.javascriptserializer.aspx
        /// for more on the JSON serializer.
        /// </remarks>
        internal void ConvertToStorageFormat(IGrainState grainState, GrainStateEntity entity)
        {
            // Dehydrate
            Dictionary<string, object> dataValues = grainState.AsDictionary();
#if !DISABLE_STREAMS
            if (_useJsonFormat)
            {
                // http://james.newtonking.com/json/help/index.html?topic=html/T_Newtonsoft_Json_JsonConvert.htm
                entity.StringData = Newtonsoft.Json.JsonConvert.SerializeObject(dataValues, _jsonSettings);   
            }else
#endif
            {
                // Convert to binary format
                entity.Data = SerializationManager.SerializeToByteArray(dataValues);
            }
        }

        /// <summary>
        /// Deserialize from Azure storage format
        /// </summary>
        /// <param name="grainState">The grain state data to be deserialized in to</param>
        /// <param name="entity">The Azure table entity the stored data</param>
        internal void ConvertFromStorageFormat(IGrainState grainState, GrainStateEntity entity)
        {
            Dictionary<string, object> dataValues = null;
            try
            {
                if (entity.Data != null)
                {
                    // Rehydrate
                    dataValues = SerializationManager.DeserializeFromByteArray<Dictionary<string, object>>(entity.Data);
                }
#if !DISABLE_STREAMS
                else if (entity.StringData != null)
                {
                    dataValues = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, object>>(entity.StringData, _jsonSettings);
                }
#endif
                if (dataValues != null)
                {
                    grainState.SetAll(dataValues);
                }
                // Else, no data found
            }
            catch (Exception exc)
            {
                var sb = new StringBuilder();
                if (entity.Data != null)
                {
                    sb.AppendFormat("Unable to convert from storage format GrainStateEntity.Data={0}", entity.Data);
                }
#if !DISABLE_STREAMS
                else if (entity.StringData != null)
                {
                    sb.AppendFormat("Unable to convert from storage format GrainStateEntity.StringData={0}", entity.StringData);
                }
#endif
                if (dataValues != null)
                {
                    int i = 1;
                    foreach (var dvKey in dataValues.Keys)
                    {
                        object dvValue = dataValues[dvKey];
                        sb.AppendLine();
                        sb.AppendFormat("Data #{0} Key={1} Value={2} Type={3}", i, dvKey, dvValue, dvValue.GetType());
                        i++;
                    }
                }
                Log.Error(0, sb.ToString(), exc);
                throw new AggregateException(sb.ToString(), exc);
            }
        }
    }

    internal class GrainStateTableDataManager : AzureTableDataManager<GrainStateEntity>
    {
        public GrainStateTableDataManager(string tableName, string storageConnectionString)
            : base(tableName, storageConnectionString)
        { }

        public async Task<GrainStateRecord> Read(string partitionKey, string rowKey)
        {
            if (logger.IsVerbose3) logger.Verbose3((int)ProviderErrorCode.AzureTableProvider_Storage_Reading, "Reading: PartitionKey={0} RowKey={1} from Table={2}", partitionKey, rowKey, TableName);
            try
            {
                Tuple<GrainStateEntity, string> data = await ReadSingleTableEntryAsync(partitionKey, rowKey);

                GrainStateEntity stateEntity = data.Item1;
                var record = new GrainStateRecord { Entity = stateEntity, ETag = data.Item2 };
                if (logger.IsVerbose3) logger.Verbose3((int)ProviderErrorCode.AzureTableProvider_Storage_DataRead, "Read: PartitionKey={0} RowKey={1} from Table={2} with ETag={3}", stateEntity.PartitionKey, stateEntity.RowKey, TableName, record.ETag);
                return record;
            }
            catch (Exception exc)
            {
                if (AzureStorageUtils.TableStorageDataNotFound(exc))
                {
                    if (logger.IsVerbose2) logger.Verbose2((int)ProviderErrorCode.AzureTableProvider_DataNotFound, "DataNotFound reading: PartitionKey={0} RowKey={1} from Table={2} Exception={3}", partitionKey, rowKey, TableName, exc);
                    return null;  // No data
                }
                throw;
            }
        }

        public async Task Write(GrainStateRecord record)
        {
            GrainStateEntity entity = record.Entity;
            if (logger.IsVerbose3) logger.Verbose3((int)ProviderErrorCode.AzureTableProvider_Storage_Writing, "Writing: PartitionKey={0} RowKey={1} to Table={2} with ETag={3}", entity.PartitionKey, entity.RowKey, TableName, record.ETag);
            string eTag;
            if (String.IsNullOrEmpty(record.ETag))
            {
                eTag = await CreateTableEntryAsync(record.Entity);
            }
            else
            {
                eTag = await UpdateTableEntryAsync(entity, record.ETag);
            }
            record.ETag = eTag;
        }

        public async Task Delete(GrainStateRecord record)
        {
            GrainStateEntity entity = record.Entity;
            if (logger.IsVerbose3) logger.Verbose3((int)ProviderErrorCode.AzureTableProvider_Storage_Writing, "Deleting: PartitionKey={0} RowKey={1} from Table={2} with ETag={3}", entity.PartitionKey, entity.RowKey, TableName, record.ETag);
            await DeleteTableEntryAsync(entity, record.ETag);
            record.ETag = null;
        }
    }

    [Serializable]
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class GrainStateEntity : TableServiceEntity
    {
        public byte[] Data { get; set; }
#if !DISABLE_STREAMS
        public string StringData { get; set; }
#endif
    }

    internal class GrainStateRecord
    {
        public string ETag { get; set; }
        public GrainStateEntity Entity { get; set; }
    }
}