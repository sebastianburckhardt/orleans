using System;
using System.Collections.Generic;
using System.Data.Services.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Net;
using System.Text;
using System.Threading;
using System.Xml;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;
using System.Data.Services.Client;

namespace PingUtil
{
    public abstract class AzureTableDataManager<T> where T : TableServiceEntity
    {
        private readonly static RetryPolicy DefaultTableCreationRetryPolicy = RetryPolicies.Retry(60, TimeSpan.FromSeconds(1)); // 60 x 1s
        private readonly static RetryPolicy DefaultTableOperationRetryPolicy = RetryPolicies.Retry(MAX_RETRIES, PAUSE_TIME_BETWEEN_RETRIES); // 5 x 100ms

        internal string TableName { get; private set; }
        protected RetryPolicy TableCreationRetryPolicy { get; set; }
        protected RetryPolicy TableOperationRetryPolicy { get; set; }
        protected string ConnectionString { get; set; }

        private CloudTableClient tableClient;

        private const int MAX_RETRIES = 5;
        private static readonly TimeSpan PAUSE_TIME_BETWEEN_RETRIES = TimeSpan.FromMilliseconds(100); // Milliseconds
        protected const string ANY_ETAG = "*";

        private readonly object lockable = new object();

        protected abstract Type ResolveEntityType(string name);

        internal AzureTableDataManager(string tableName, string storageConnectionString)
        {
            this.TableName = tableName;
            this.TableCreationRetryPolicy = DefaultTableCreationRetryPolicy;
            this.TableOperationRetryPolicy = DefaultTableOperationRetryPolicy;
            this.ConnectionString = storageConnectionString;
        }

        internal void InitTable()
        {
            var tableStore = GetCloudTableClient(true);

            bool didCreate = false;
            try
            {
                didCreate = tableStore.CreateTableIfNotExist(TableName);
            }
            catch (Exception )
            {
                throw;
            }
        }

        /// <summary>
        /// Read data entries and corresponding etag values for table rows matching the specified predicate
        /// Used http://convective.wordpress.com/2010/02/06/queries-in-azure-tables/
        /// to get the API details.
        /// </summary>
        /// <param name="predicate"></param>
        /// <returns></returns>
        protected IEnumerable<Tuple<T, string>> ReadTableEntriesAndEtags(Expression<Func<T, bool>> predicate)
        {
            CloudTableClient cloudTableClient = GetCloudTableClient(false);
            TableServiceContext svc = cloudTableClient.GetDataServiceContext();
            // Improve performance when table name differs from class name
            svc.ResolveType += ResolveEntityType;

            CloudTableQuery<T> cloudTableQuery = svc.CreateQuery<T>(TableName).Where(predicate).AsTableServiceQuery<T>(); // turn IQueryable into CloudTableQuery

            bool isLastErrorRetriable = true;
            Exception lastError = null;

            for (int i = 0; i <= MAX_RETRIES && isLastErrorRetriable; i++)
            {
                try
                {
                    //List<T> queryResults = query.ToList(); // ToList will actually execute the query and add entities to svc. However, this will not handle continuation tokens.
                    // CloudTableQuery.Execute will properly retrieve all the records from a table through the automatic handling of continuation tokens:
                    // http://convective.wordpress.com/2010/02/06/queries-in-azure-tables/
                    List<T> queryResults = cloudTableQuery.Execute().ToList(); // Execute is still lazy execution, need ToList to actually execute the query and add entities to svc.

                    // Data was read successfully if we got to here
                    return svc.Entities.Select(entity =>
                            new Tuple<T, string>((T)entity.Entity, entity.ETag));
                }
                catch (Exception exc)
                {
                    lastError = exc;
                    if (exc is WebException)
                    {
                        WebException we = (WebException)exc;
                        isLastErrorRetriable = true;
                        var statusCode = we.Status;
                        //logger.Warn(ErrorCode.AzureTable_07, String.Format("Intermediate issue reading Azure storage table {0}: HTTP status code={1} Exception Type={2} Message='{3}'",
                        //    TableName,
                        //    statusCode,
                        //    exc.GetType().FullName,
                        //    exc.Message),
                        //    exc);
                    }
                    else
                    {
                        HttpStatusCode httpStatusCode;
                        string restStatus;
                        if (EvaluateException(exc, out httpStatusCode, out restStatus, true))
                        {
                            isLastErrorRetriable = IsRetriableHttpError(httpStatusCode, restStatus);

                            //logger.Warn(ErrorCode.AzureTable_08, String.Format("Intermediate issue reading Azure storage table {0}:{1} IsRetriable={2} HTTP status code={3} REST status code={4} Exception Type={5} Message='{6}'",
                            //            TableName,
                            //            i == 0 ? "" : (" Repeat=" + i),
                            //            isLastErrorRetriable,
                            //            httpStatusCode,
                            //            restStatus,
                            //            exc.GetType().FullName,
                            //            exc.Message),
                            //            exc);
                        }
                        else
                        {
                            //logger.Error(ErrorCode.AzureTable_09, string.Format("Unexpected issue reading Azure storage table {0}: Exception Type={1} Message='{2}'",
                            //                 TableName,
                            //                 exc.GetType().FullName,
                            //                 exc.Message),
                            //             exc);
                            throw;
                        }
                    }
                    Thread.Sleep(PAUSE_TIME_BETWEEN_RETRIES);
                }
            } // End retry loop

            // Out of retries...
            var errorMsg = string.Format("Failed to read Azure storage table {0}: {1}", TableName, lastError.Message);
            //logger.Error(ErrorCode.AzureTable_10, errorMsg, lastError);
            throw new DataServiceQueryException(errorMsg, lastError);
        }

        /// <summary>
        /// Returns true if the specified HTTP status / error code is returned in a transient / retriable error condition
        /// </summary>
        /// <param name="httpStatusCode">HTTP error code value</param>
        /// <param name="restStatusCode">REST error code value</param>
        /// <returns><c>true</c> if this is a transient / retriable error condition</returns>
        internal static bool IsRetriableHttpError(HttpStatusCode httpStatusCode, string restStatusCode)
        {
            // Note: We ignore the 20X values as they are successful outcomes, not errors

            return (
                httpStatusCode == HttpStatusCode.RequestTimeout /* 408 */
                || httpStatusCode == HttpStatusCode.BadGateway          /* 502 */
                || httpStatusCode == HttpStatusCode.ServiceUnavailable  /* 503 */
                || httpStatusCode == HttpStatusCode.GatewayTimeout      /* 504 */
                || (httpStatusCode == HttpStatusCode.InternalServerError /* 500 */
                    && !String.IsNullOrEmpty(restStatusCode) && "OperationTimedOut".Equals(restStatusCode, StringComparison.InvariantCultureIgnoreCase))
            );
        }

        internal static string ExtractRestErrorCode(Exception exc)
        {
            // Sample of REST error message returned from Azure storage service
            //<?xml version="1.0" encoding="utf-8" standalone="yes"?>
            //<error xmlns="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata">
            //  <code>OperationTimedOut</code>
            //  <message xml:lang="en-US">Operation could not be completed within the specified time. RequestId:6b75e963-c56c-4734-a656-066cfd03f327 Time:2011-10-09T19:33:26.7631923Z</message>
            //</error>

            while (exc != null && !(exc is DataServiceClientException))
            {
                exc = exc.InnerException;
            }

            if (exc is DataServiceClientException)
            {
                try
                {
                    XmlDocument xml = new XmlDocument();
                    xml.LoadXml(exc.Message);
                    XmlNamespaceManager namespaceManager = new XmlNamespaceManager(xml.NameTable);
                    namespaceManager.AddNamespace("n", "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata");
                    string s = xml.SelectSingleNode("/n:error/n:code", namespaceManager).InnerText;
                    return s;
                }
                catch (Exception )
                {
                    //var log = Logger.GetLogger("AzureTableDataManager", Logger.LoggerType.Runtime);
                    //log.Warn(ErrorCode.AzureTable_11, String.Format("Problem extracting REST error code from Data='{0}'", exc.Message), e);
                }
            }
            return null;
        }


        internal static bool EvaluateException(
            Exception e,
            out HttpStatusCode httpStatusCode,
            out string restStatus,
            bool getExtendedErrors = false)
        {
            httpStatusCode = HttpStatusCode.Unused;
            restStatus = null;

            while (e != null)
            {
                if (e is DataServiceQueryException)
                {
                    DataServiceQueryException dsqe = e as DataServiceQueryException;
                    httpStatusCode = (HttpStatusCode)dsqe.Response.StatusCode;
                    if (getExtendedErrors)
                        restStatus = ExtractRestErrorCode(dsqe);
                    return true;
                }
                else if (e is DataServiceClientException)
                {
                    DataServiceClientException dsce = e as DataServiceClientException;
                    httpStatusCode = (HttpStatusCode)dsce.StatusCode;
                    if (getExtendedErrors)
                        restStatus = ExtractRestErrorCode(dsce);
                    return true;
                }
                else if (e is DataServiceRequestException)
                {
                    DataServiceRequestException dsre = e as DataServiceRequestException;
                    httpStatusCode = (HttpStatusCode)dsre.Response.First().StatusCode;
                    if (getExtendedErrors)
                        restStatus = ExtractRestErrorCode(dsre);
                    return true;
                }
                e = e.InnerException;
            }
            return false;
        }

        private CloudTableClient GetCloudTableClient(bool init)
        {
            if (init)
            {
                var storageAccount = GetCloudStorageAccount(ConnectionString);
                var client = storageAccount.CreateCloudTableClient();
                client.RetryPolicy = TableCreationRetryPolicy;
                return client;
            }
            else if (tableClient != null)
            {
                return this.tableClient;
            }
            else
            {
                lock (lockable)
                {
                    if (this.tableClient == null)
                    {
                        var storageAccount = GetCloudStorageAccount(ConnectionString);
                        this.tableClient = storageAccount.CreateCloudTableClient();
                        this.tableClient.RetryPolicy = TableOperationRetryPolicy;
                    }
                }
                return this.tableClient;
            }
        }

        private static CloudStorageAccount GetCloudStorageAccount(string storageConnectionString)
        {
            // Connection string must be specified always, even for development storage.
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentException("Azure storage connection string cannot be blank");
            }
            else
            {
                return CloudStorageAccount.Parse(storageConnectionString);
            }
        }
    }

    [Serializable]
    [DataServiceKey("PartitionKey", "RowKey")]
    public class SiloMetricsData : TableServiceEntity
    {
        public string HostName { get; set; }
        public string Address { get; set; }
        public int Port { get; set; }
        public string GatewayAddress { get; set; }
        public int GatewayPort { get; set; }
        public int Generation { get; set; }

        public double CPU { get; set; }
        public long Memory { get; set; }
        public int Activations { get; set; }
        public int SendQueue { get; set; }
        public int ReceiveQueue { get; set; }
        public int WorkQueue { get; set; }
        public bool LoadShedding { get; set; }
        public long ClientCount { get; set; }

        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("OrleansSiloMetricsData[");

            sb.Append(" DeploymentId=").Append(PartitionKey);
            sb.Append(" SiloId=").Append(RowKey);

            sb.Append(" Host=").Append(HostName);
            sb.Append(" Endpoint=").Append(Address + ":" + Port);
            sb.Append(" Generation=").Append(Generation);

            sb.Append(" CPU=").Append(CPU);
            sb.Append(" Memory=").Append(Memory);
            sb.Append(" Activations=").Append(Activations);
            sb.Append(" SendQueue=").Append(SendQueue);
            sb.Append(" ReceiveQueue=").Append(ReceiveQueue);
            sb.Append(" WorkQueue=").Append(WorkQueue);
            sb.Append(" LoadShedding=").Append(LoadShedding);
            sb.Append(" Clients=").Append(ClientCount);

            sb.Append(" ]");
            return sb.ToString();
        }
    }

    public class SiloMetricsDataReporter : AzureTableDataManager<SiloMetricsData>
    {
        protected const string INSTANCE_TABLE_NAME = "OrleansSiloMetrics";

        private string DeploymentId;

        public SiloMetricsDataReporter(string storageConnectionString, string deploymentId)
            : base(INSTANCE_TABLE_NAME, storageConnectionString)
        {
            InitTable();
            DeploymentId = deploymentId;
        }

        protected override Type ResolveEntityType(string name)
        {
            return typeof(SiloMetricsData);
        }

        private IEnumerable<SiloMetricsData> GetSiloMetrics()
        {
            // Get everything
            return ReadTableEntriesAndEtags(instance => instance.PartitionKey == this.DeploymentId).Select(tuple => tuple.Item1);
        }

        /// <summary>
        /// This function has ugly signature with ref instead of a return value, becuase we don't want to always update the l-value. 
        /// (In short we don't want to overwrite the earlier data with empty dictionary in case of exceptions)
        /// </summary>
        /// <param name="groupMembers">Dictionary of host name to ip endpoints</param>
        public void GetFromAzure(ref Dictionary<string, IPEndPoint> groupMembers)
        {
            Dictionary<string, IPEndPoint> newDict = new Dictionary<string, IPEndPoint>();
            try
            {
                foreach (var d in GetSiloMetrics())
                {
                    IPEndPoint endpoint = new IPEndPoint(IPAddress.Parse(d.Address), Agent.PING_PORT);
                    Log.Write(Log.Severity.Verbose, "Found group member : {0} on {1}", endpoint, d.HostName.ToUpper());
                    newDict.Add(d.HostName.ToUpper(), endpoint);
                }
                // everything fine so far
                if (newDict.Count > 0)
                {
                    groupMembers = newDict;
                }
            }
            catch (Exception ex)
            {
                Log.Write(Log.Severity.Error, "Could not retrive data from azure table. {0}", ex);
            }
        }

        //private static void ValidateTablesIsStatic(Dictionary<string, IPEndPoint> addresses, Dictionary<string, IPEndPoint> newDict)
        //{
        //    if (addresses.Count > 0)
        //    {
        //        // assert if there is any missing machines
        //        foreach (string host in addresses.Keys)
        //        {
        //            if (!newDict.ContainsKey(host))
        //                throw new ApplicationException("Missing data from azure metrics table query");
        //        }
        //        foreach (string host in newDict.Keys)
        //        {
        //            if (!addresses.ContainsKey(host))
        //                throw new ApplicationException("Phantom data from azure metrics table query");
        //        }
        //    }
        //}
    }
}
