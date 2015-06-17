using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    public interface ICapCloudBlob: ICloudBlob
    {
        /// <summary>
        /// Loads the blob's content to the target stream.
        /// </summary>
        /// <param name="target">The stream that receives the contents.</param>
        /// <param name="accessCondition">Preconditions for performing the operation.</param>
        /// <param name="options">Other options.</param>
        /// <param name="operationContext">Operation state.</param>
        /// <param name="sla">Consistency-based SLA.</param>
        void DownloadToStreamWithSLA(Stream target, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null, ConsistencySLAEngine session = null);
       
        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="offset"></param>
        /// <param name="length"></param>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <param name="sla">Consistency-based SLA.</param>
        void DownloadRangeToStream(Stream target, long? offset, long? length, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null, ServiceLevelAgreement sla = null);
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <returns></returns>
        /// <param name="sla">Consistency-based SLA.</param>
        bool Exists(BlobRequestOptions options = null, OperationContext operationContext = null, ServiceLevelAgreement sla = null);
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="accessCondition"></param>
        /// <param name="options"></param>
        /// <param name="operationContext"></param>
        /// <param name="sla">Consistency-based SLA.</param>
        void FetchAttributes(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null, ServiceLevelAgreement sla = null);
    }
}
