using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    /// <summary>
    /// Allows operations on blobs that are replicated across any number of Azure storage sites.
    /// Read operations are controlled by consistency-based SLAs that indirectly select the replica 
    /// from which a blob's contents or metadata is read.
    /// </summary>

    public class CapCloudBlob: ICloudBlob
    {

        /// <summary>
        /// The service level agreement used to select replicas from which to read
        /// </summary>
        public ServiceLevelAgreement Sla {
            get { return engine.Sla; }
            set { engine.Sla = value; } 
        }

        // The class that implements the read/write protocol
        private ReadWriteFramework protocol;

        // The SLA Engine used to select sites for reads and writes
        public ConsistencySLAEngine engine;

        /// <summary>
        /// Creates a new instance of a CapCloudBlob.
        /// </summary>
        /// <param name="strong">A reference to the strongly consistent copy of the blob.</param>
        /// <param name="eventual">A reference to the eventually consistent copy of the blob.</param>
        /// <param name="engine">The SLA enforcement engine.</param>
        public CapCloudBlob(string name, ReplicaConfiguration configuration, ConsistencySLAEngine engine)
        {
            this.protocol = new ReadWriteFramework(name, configuration, engine);
            this.engine = engine;
        }

        /*
         * Below is a shim for each of the methods in the ICloudBlob interface (in alphabetical order).
         * Most of the implementations simply call on the main primary blob using protocol.MainPrimary().
         * Read operations should call protocol.Read with a delegate for the operation.
         * Write operations should call protocol.Write with a delegate for the operation.
         */

        public void AbortCopy(string copyId, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.MainPrimary().AbortCopy(copyId, accessCondition, options, operationContext);
        }

        public Task AbortCopyAsync(string copyId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().AbortCopyAsync(copyId, accessCondition, options, operationContext, cancellationToken);
        }

        public Task AbortCopyAsync(string copyId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().AbortCopyAsync(copyId, accessCondition, options, operationContext);
        }

        public Task AbortCopyAsync(string copyId, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().AbortCopyAsync(copyId, cancellationToken);
        }

        public Task AbortCopyAsync(string copyId)
        {
            return protocol.MainPrimary().AbortCopyAsync(copyId);
        }

        public string AcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return protocol.MainPrimary().AcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext);
        }

        public Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().AcquireLeaseAsync(leaseTime, proposedLeaseId, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().AcquireLeaseAsync(leaseTime, proposedLeaseId, accessCondition, options, operationContext);
        }

        public Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().AcquireLeaseAsync(leaseTime, proposedLeaseId, cancellationToken);
        }

        public Task<string> AcquireLeaseAsync(TimeSpan? leaseTime, string proposedLeaseId)
        {
            return protocol.MainPrimary().AcquireLeaseAsync(leaseTime, proposedLeaseId);
        }

        public ICancellableAsyncResult BeginAbortCopy(string copyId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginAbortCopy(copyId, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginAbortCopy(string copyId, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginAbortCopy(copyId, callback, state);
        }

        public ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginAcquireLease(leaseTime, proposedLeaseId, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginAcquireLease(TimeSpan? leaseTime, string proposedLeaseId, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginAcquireLease(leaseTime, proposedLeaseId, callback, state);
        }

        public ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginBreakLease(breakPeriod, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginBreakLease(TimeSpan? breakPeriod, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginBreakLease(breakPeriod, callback, state);
        }

        public ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginChangeLease(proposedLeaseId, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginChangeLease(string proposedLeaseId, AccessCondition accessCondition, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginChangeLease(proposedLeaseId, accessCondition, callback, state);
        }

        public ICancellableAsyncResult BeginDelete(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDelete(deleteSnapshotsOption, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDelete(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDelete(callback, state);
        }

        public ICancellableAsyncResult BeginDeleteIfExists(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDeleteIfExists(deleteSnapshotsOption, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDeleteIfExists(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDeleteIfExists(callback, state);
        }

        public ICancellableAsyncResult BeginDownloadRangeToByteArray(byte[] target, int index, long? blobOffset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadRangeToByteArray(target, index, blobOffset, length, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadRangeToByteArray(byte[] target, int index, long? blobOffset, long? length, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadRangeToByteArray(target, index, blobOffset, length, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadRangeToStream(Stream target, long? offset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadRangeToStream(target, offset, length, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadRangeToStream(Stream target, long? offset, long? length, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadRangeToStream(target, offset, length, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToByteArray(byte[] target, int index, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToByteArray(target, index, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToByteArray(byte[] target, int index, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToByteArray(target, index, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToFile(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToFile(path, mode, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToFile(string path, FileMode mode, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToFile(path, mode, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToStream(Stream target, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToStream(target, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginDownloadToStream(Stream target, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginDownloadToStream(target, callback, state);
        }

        public ICancellableAsyncResult BeginExists(BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginExists(options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginExists(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginExists(callback, state);
        }

        public ICancellableAsyncResult BeginFetchAttributes(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginFetchAttributes(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginFetchAttributes(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginFetchAttributes(callback, state);
        }

        public ICancellableAsyncResult BeginOpenRead(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginOpenRead(callback, state);
        }

        public ICancellableAsyncResult BeginOpenRead(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginOpenRead(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginReleaseLease(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginReleaseLease(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginReleaseLease(AccessCondition accessCondition, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginReleaseLease(accessCondition, callback, state);
        }

        public ICancellableAsyncResult BeginRenewLease(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginRenewLease(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginRenewLease(AccessCondition accessCondition, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginRenewLease(accessCondition, callback, state);
        }

        public ICancellableAsyncResult BeginSetMetadata(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginSetMetadata(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginSetMetadata(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginSetMetadata(callback, state);
        }

        public ICancellableAsyncResult BeginSetProperties(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginSetProperties(accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginSetProperties(AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginSetProperties(callback, state);
        }

        public ICancellableAsyncResult BeginStartCopyFromBlob(Uri source, AccessCondition sourceAccessCondition, AccessCondition destAccessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginStartCopyFromBlob(source, sourceAccessCondition, destAccessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginStartCopyFromBlob(Uri source, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginStartCopyFromBlob(source, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromByteArray(byte[] buffer, int index, int count, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromByteArray(buffer, index, count, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromByteArray(byte[] buffer, int index, int count, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromByteArray(buffer, index, count, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromFile(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromFile(path, mode, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromFile(string path, FileMode mode, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromFile(path, mode, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromStream(Stream source, long length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromStream(source, length, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromStream(Stream source, long length, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromStream(source, length, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromStream(Stream source, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromStream(source, accessCondition, options, operationContext, callback, state);
        }

        public ICancellableAsyncResult BeginUploadFromStream(Stream source, AsyncCallback callback, object state)
        {
            return protocol.MainPrimary().BeginUploadFromStream(source, callback, state);
        }

        public BlobType BlobType
        {
            get { return protocol.MainPrimary().BlobType; }
        }

        public TimeSpan BreakLease(TimeSpan? breakPeriod = null, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return protocol.MainPrimary().BreakLease(breakPeriod, accessCondition, options, operationContext);
        }

        public Task<TimeSpan> BreakLeaseAsync(TimeSpan? breakPeriod, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().BreakLeaseAsync(breakPeriod, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<TimeSpan> BreakLeaseAsync(TimeSpan? breakPeriod, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().BreakLeaseAsync(breakPeriod, accessCondition, options, operationContext);
        }

        public Task<TimeSpan> BreakLeaseAsync(TimeSpan? breakPeriod, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().BreakLeaseAsync(breakPeriod, cancellationToken);
        }

        public Task<TimeSpan> BreakLeaseAsync(TimeSpan? breakPeriod)
        {
            return protocol.MainPrimary().BreakLeaseAsync(breakPeriod);
        }

        public string ChangeLease(string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            return protocol.MainPrimary().ChangeLease(proposedLeaseId, accessCondition, options, operationContext);
        }

        public Task<string> ChangeLeaseAsync(string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ChangeLeaseAsync(proposedLeaseId, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<string> ChangeLeaseAsync(string proposedLeaseId, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().ChangeLeaseAsync(proposedLeaseId, accessCondition, options, operationContext);
        }

        public Task<string> ChangeLeaseAsync(string proposedLeaseId, AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ChangeLeaseAsync(proposedLeaseId, accessCondition, cancellationToken);
        }

        public Task<string> ChangeLeaseAsync(string proposedLeaseId, AccessCondition accessCondition)
        {
            return protocol.MainPrimary().ChangeLeaseAsync(proposedLeaseId, accessCondition);
        }

        public CopyState CopyState
        {
            get { return protocol.MainPrimary().CopyState; }
        }

        public void Delete(DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.Delete(deleteSnapshotsOption, ac, options, operationContext), ac);
        }

        public Task DeleteAsync(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DeleteAsync(deleteSnapshotsOption, accessCondition, options, operationContext, cancellationToken);
        }

        public Task DeleteAsync(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DeleteAsync(deleteSnapshotsOption, accessCondition, options, operationContext);
        }

        public Task DeleteAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DeleteAsync(cancellationToken);
        }

        public Task DeleteAsync()
        {
            return protocol.MainPrimary().DeleteAsync();
        }

        public bool DeleteIfExists(DeleteSnapshotsOption deleteSnapshotsOption = DeleteSnapshotsOption.None, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            bool result = false;
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => result = blob.DeleteIfExists(deleteSnapshotsOption, ac, options, operationContext), ac);
            return result;
        }

        public Task<bool> DeleteIfExistsAsync(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DeleteIfExistsAsync(deleteSnapshotsOption, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<bool> DeleteIfExistsAsync(DeleteSnapshotsOption deleteSnapshotsOption, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DeleteIfExistsAsync(deleteSnapshotsOption, accessCondition, options, operationContext);
        }

        public Task<bool> DeleteIfExistsAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DeleteIfExistsAsync(cancellationToken);
        }

        public Task<bool> DeleteIfExistsAsync()
        {
            return protocol.MainPrimary().DeleteIfExistsAsync();
        }

        public int DownloadRangeToByteArray(byte[] target, int index, long? blobOffset, long? length, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            int result = 0;
            protocol.Read(blob => result = blob.DownloadRangeToByteArray(target, index, blobOffset, length, accessCondition, options, operationContext));
            return result;
        }

        public Task<int> DownloadRangeToByteArrayAsync(byte[] target, int index, long? blobOffset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadRangeToByteArrayAsync(target, index, blobOffset, length, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<int> DownloadRangeToByteArrayAsync(byte[] target, int index, long? blobOffset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DownloadRangeToByteArrayAsync(target, index, blobOffset, length, accessCondition, options, operationContext);
        }

        public Task<int> DownloadRangeToByteArrayAsync(byte[] target, int index, long? blobOffset, long? length, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadRangeToByteArrayAsync(target, index, blobOffset, length, cancellationToken);
        }

        public Task<int> DownloadRangeToByteArrayAsync(byte[] target, int index, long? blobOffset, long? length)
        {
            return protocol.MainPrimary().DownloadRangeToByteArrayAsync(target, index, blobOffset, length);
        }

        public void DownloadRangeToStream(Stream target, long? offset, long? length, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.Read(blob => blob.DownloadRangeToStream(target, offset, length, accessCondition, options, operationContext));
        }

        public Task DownloadRangeToStreamAsync(Stream target, long? offset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadRangeToStreamAsync(target, offset, length, accessCondition, options, operationContext, cancellationToken);
        }

        public Task DownloadRangeToStreamAsync(Stream target, long? offset, long? length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DownloadRangeToStreamAsync(target, offset, length, accessCondition, options, operationContext);
        }

        public Task DownloadRangeToStreamAsync(Stream target, long? offset, long? length, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadRangeToStreamAsync(target, offset, length, cancellationToken);
        }

        public Task DownloadRangeToStreamAsync(Stream target, long? offset, long? length)
        {
            return protocol.MainPrimary().DownloadRangeToStreamAsync(target, offset, length);
        }

        public int DownloadToByteArray(byte[] target, int index, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            int result = 0;
            protocol.Read(blob => result = blob.DownloadToByteArray(target, index, accessCondition, options, operationContext));
            return result;
        }

        public Task<int> DownloadToByteArrayAsync(byte[] target, int index, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToByteArrayAsync(target, index, accessCondition, options, operationContext, cancellationToken);
        }

        public Task<int> DownloadToByteArrayAsync(byte[] target, int index, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DownloadToByteArrayAsync(target, index, accessCondition, options, operationContext);
        }

        public Task<int> DownloadToByteArrayAsync(byte[] target, int index, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToByteArrayAsync(target, index, cancellationToken);
        }

        public Task<int> DownloadToByteArrayAsync(byte[] target, int index)
        {
            return protocol.MainPrimary().DownloadToByteArrayAsync(target, index);
        }

        public void DownloadToFile(string path, FileMode mode, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.Read(blob => blob.DownloadToFile(path, mode, accessCondition, options, operationContext));
        }

        public Task DownloadToFileAsync(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToFileAsync(path, mode, accessCondition, options, operationContext, cancellationToken);
        }

        public Task DownloadToFileAsync(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DownloadToFileAsync(path, mode, accessCondition, options, operationContext);
        }

        public Task DownloadToFileAsync(string path, FileMode mode, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToFileAsync(path, mode, cancellationToken);
        }

        public Task DownloadToFileAsync(string path, FileMode mode)
        {
            return protocol.MainPrimary().DownloadToFileAsync(path, mode);
        }

        public void DownloadToStream(Stream target, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            BlobRequestOptions bro = (options == null) ? new BlobRequestOptions() : options;
            bro.DisableContentMD5Validation = true;
            protocol.Read(blob => blob.DownloadToStream(target, accessCondition, bro, operationContext));
        }

        public Task DownloadToStreamAsync(Stream target, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToStreamAsync(target, accessCondition, options, operationContext, cancellationToken);
        }

        public Task DownloadToStreamAsync(Stream target, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().DownloadToStreamAsync(target, accessCondition, options, operationContext);
        }

        public Task DownloadToStreamAsync(Stream target, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().DownloadToStreamAsync(target, cancellationToken);
        }

        public Task DownloadToStreamAsync(Stream target)
        {
            return protocol.MainPrimary().DownloadToStreamAsync(target);
        }

        public void EndAbortCopy(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndAbortCopy(asyncResult);
        }

        public string EndAcquireLease(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndAcquireLease(asyncResult);
        }

        public TimeSpan EndBreakLease(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndBreakLease(asyncResult);
        }

        public string EndChangeLease(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndChangeLease(asyncResult);
        }

        public void EndDelete(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndDelete(asyncResult);
        }

        public bool EndDeleteIfExists(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndDeleteIfExists(asyncResult);
        }

        public int EndDownloadRangeToByteArray(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndDownloadRangeToByteArray(asyncResult);
        }

        public void EndDownloadRangeToStream(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndDownloadRangeToStream(asyncResult);
        }

        public int EndDownloadToByteArray(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndDownloadToByteArray(asyncResult);
        }

        public void EndDownloadToFile(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndDownloadToFile(asyncResult);
        }

        public void EndDownloadToStream(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndDownloadToStream(asyncResult);
        }

        public bool EndExists(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndExists(asyncResult);
        }

        public void EndFetchAttributes(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndFetchAttributes(asyncResult);
        }

        public Stream EndOpenRead(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndOpenRead(asyncResult);
        }

        public void EndReleaseLease(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndReleaseLease(asyncResult);
        }

        public void EndRenewLease(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndRenewLease(asyncResult);
        }

        public void EndSetMetadata(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndSetMetadata(asyncResult);
        }

        public void EndSetProperties(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndSetProperties(asyncResult);
        }

        public string EndStartCopyFromBlob(IAsyncResult asyncResult)
        {
            return protocol.MainPrimary().EndStartCopyFromBlob(asyncResult);
        }

        public void EndUploadFromByteArray(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndUploadFromByteArray(asyncResult);
        }

        public void EndUploadFromFile(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndUploadFromFile(asyncResult);
        }

        public void EndUploadFromStream(IAsyncResult asyncResult)
        {
            protocol.MainPrimary().EndUploadFromStream(asyncResult);
        }

        public bool Exists(BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            bool result = false;
            protocol.Read(blob => result = blob.Exists(options, operationContext));
            return result;
        }

        public Task<bool> ExistsAsync(BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ExistsAsync(options, operationContext, cancellationToken);
        }

        public Task<bool> ExistsAsync(BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().ExistsAsync(options, operationContext);
        }

        public Task<bool> ExistsAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ExistsAsync(cancellationToken);
        }

        public Task<bool> ExistsAsync()
        {
            return protocol.MainPrimary().ExistsAsync();
        }

        public void FetchAttributes(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.Read(blob => blob.FetchAttributes(accessCondition, options, operationContext));
        }

        public Task FetchAttributesAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().FetchAttributesAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task FetchAttributesAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().FetchAttributesAsync(accessCondition, options, operationContext);
        }

        public Task FetchAttributesAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().FetchAttributesAsync(cancellationToken);
        }

        public Task FetchAttributesAsync()
        {
            return protocol.MainPrimary().FetchAttributesAsync();
        }

        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy, SharedAccessBlobHeaders headers, string groupPolicyIdentifier, string sasVersion)
        {
            return protocol.MainPrimary().GetSharedAccessSignature(policy, headers, groupPolicyIdentifier,sasVersion);
        }

        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy, SharedAccessBlobHeaders headers, string groupPolicyIdentifier)
        {
            return protocol.MainPrimary().GetSharedAccessSignature(policy, headers, groupPolicyIdentifier);
        }

        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy, SharedAccessBlobHeaders headers)
        {
            return protocol.MainPrimary().GetSharedAccessSignature(policy, headers);
        }

        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy, string groupPolicyIdentifier)
        {
            return protocol.MainPrimary().GetSharedAccessSignature(policy, groupPolicyIdentifier);
        }

        public string GetSharedAccessSignature(SharedAccessBlobPolicy policy)
        {
            return protocol.MainPrimary().GetSharedAccessSignature(policy);
        }

        public bool IsSnapshot
        {
            get { return protocol.MainPrimary().IsSnapshot; }
        }

        public IDictionary<string, string> Metadata
        {
            get { return protocol.PrevRead().Metadata; }
        }

        public string Name
        {
            get { return protocol.MainPrimary().Name; }
        }

        public Stream OpenRead(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            Stream result = null;
            protocol.Read(blob => result = blob.OpenRead(accessCondition, options, operationContext));
            return result;
        }

        public Task<Stream> OpenReadAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().OpenReadAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task<Stream> OpenReadAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().OpenReadAsync(accessCondition, options, operationContext);
        }

        public Task<Stream> OpenReadAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().OpenReadAsync(cancellationToken);
        }

        public Task<Stream> OpenReadAsync()
        {
            return protocol.MainPrimary().OpenReadAsync();
        }

        public BlobProperties Properties
        {
            get { return protocol.PrevRead().Properties; }
        }

        public void ReleaseLease(AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.MainPrimary().ReleaseLease(accessCondition, options, operationContext);
        }

        public Task ReleaseLeaseAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ReleaseLeaseAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task ReleaseLeaseAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().ReleaseLeaseAsync(accessCondition, options, operationContext);
        }

        public Task ReleaseLeaseAsync(AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().ReleaseLeaseAsync(accessCondition, cancellationToken);
        }

        public Task ReleaseLeaseAsync(AccessCondition accessCondition)
        {
            return protocol.MainPrimary().ReleaseLeaseAsync(accessCondition);
        }

        public void RenewLease(AccessCondition accessCondition, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            protocol.MainPrimary().RenewLease(accessCondition, options, operationContext);
        }

        public Task RenewLeaseAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().RenewLeaseAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task RenewLeaseAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().RenewLeaseAsync(accessCondition, options, operationContext);
        }

        public Task RenewLeaseAsync(AccessCondition accessCondition, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().RenewLeaseAsync(accessCondition, cancellationToken);
        }

        public Task RenewLeaseAsync(AccessCondition accessCondition)
        {
            return protocol.MainPrimary().RenewLeaseAsync(accessCondition);
        }

        public CloudBlobClient ServiceClient
        {
            get { return protocol.MainPrimary().ServiceClient; }
        }

        public void SetMetadata(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.SetMetadata(ac, options, operationContext), ac);
        }

        public Task SetMetadataAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().SetMetadataAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task SetMetadataAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().SetMetadataAsync(accessCondition, options, operationContext);
        }

        public Task SetMetadataAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().SetMetadataAsync(cancellationToken);
        }

        public Task SetMetadataAsync()
        {
            return protocol.MainPrimary().SetMetadataAsync();
        }

        public void SetProperties(AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.SetProperties(ac, options, operationContext), ac);
        }

        public Task SetPropertiesAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().SetPropertiesAsync(accessCondition, options, operationContext, cancellationToken);
        }

        public Task SetPropertiesAsync(AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().SetPropertiesAsync(accessCondition, options, operationContext);
        }

        public Task SetPropertiesAsync(CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().SetPropertiesAsync(cancellationToken);
        }

        public Task SetPropertiesAsync()
        {
            return protocol.MainPrimary().SetPropertiesAsync();
        }

        public StorageUri SnapshotQualifiedStorageUri
        {
            get { return protocol.MainPrimary().SnapshotQualifiedStorageUri; }
        }

        public Uri SnapshotQualifiedUri
        {
            get { return protocol.MainPrimary().SnapshotQualifiedUri; }
        }

        public DateTimeOffset? SnapshotTime
        {
            get { return protocol.MainPrimary().SnapshotTime; }
        }

        public string StartCopyFromBlob(Uri source, AccessCondition sourceAccessCondition = null, AccessCondition destAccessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            string result = null;
            AccessCondition ac = (destAccessCondition == null) ? AccessCondition.GenerateEmptyCondition() : destAccessCondition;
            protocol.Write(blob => result = blob.StartCopyFromBlob(source, sourceAccessCondition, ac, options, operationContext), ac);
            return result;
        }

        public Task<string> StartCopyFromBlobAsync(Uri source, AccessCondition sourceAccessCondition, AccessCondition destAccessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().StartCopyFromBlobAsync(source, sourceAccessCondition, destAccessCondition, options, operationContext, cancellationToken);
        }

        public Task<string> StartCopyFromBlobAsync(Uri source, AccessCondition sourceAccessCondition, AccessCondition destAccessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().StartCopyFromBlobAsync(source, sourceAccessCondition, destAccessCondition, options, operationContext);
        }

        public Task<string> StartCopyFromBlobAsync(Uri source, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().StartCopyFromBlobAsync(source, cancellationToken);
        }

        public Task<string> StartCopyFromBlobAsync(Uri source)
        {
            return protocol.MainPrimary().StartCopyFromBlobAsync(source);
        }

        public int StreamMinimumReadSizeInBytes
        {
            get
            {
                return protocol.MainPrimary().StreamMinimumReadSizeInBytes;
            }
            set
            {
                protocol.SetProperty(blob => blob.StreamMinimumReadSizeInBytes = value);
            }
        }

        public int StreamWriteSizeInBytes
        {
            get
            {
                return protocol.MainPrimary().StreamWriteSizeInBytes;
            }
            set
            {
                protocol.SetProperty(blob => blob.StreamWriteSizeInBytes = value);
            }
        }

        public void UploadFromByteArray(byte[] buffer, int index, int count, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.UploadFromByteArray(buffer, index, count, ac, options, operationContext), ac);
        }

        public Task UploadFromByteArrayAsync(byte[] buffer, int index, int count, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext, cancellationToken);
        }

        public Task UploadFromByteArrayAsync(byte[] buffer, int index, int count, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().UploadFromByteArrayAsync(buffer, index, count, accessCondition, options, operationContext);
        }

        public Task UploadFromByteArrayAsync(byte[] buffer, int index, int count, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromByteArrayAsync(buffer, index, count, cancellationToken);
        }

        public Task UploadFromByteArrayAsync(byte[] buffer, int index, int count)
        {
            return protocol.MainPrimary().UploadFromByteArrayAsync(buffer, index, count);
        }

        public void UploadFromFile(string path, FileMode mode, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.UploadFromFile(path, mode, ac, options, operationContext), ac);
        }

        public Task UploadFromFileAsync(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromFileAsync(path, mode, accessCondition, options, operationContext, cancellationToken);
        }

        public Task UploadFromFileAsync(string path, FileMode mode, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().UploadFromFileAsync(path, mode, accessCondition, options, operationContext);
        }

        public Task UploadFromFileAsync(string path, FileMode mode, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromFileAsync(path, mode, cancellationToken);
        }

        public Task UploadFromFileAsync(string path, FileMode mode)
        {
            return protocol.MainPrimary().UploadFromFileAsync(path, mode);
        }

        public void UploadFromStream(Stream source, long length, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.UploadFromStream(source, length, ac, options, operationContext), ac);
        }

        public void UploadFromStream(Stream source, AccessCondition accessCondition = null, BlobRequestOptions options = null, OperationContext operationContext = null)
        {
            AccessCondition ac = (accessCondition == null) ? AccessCondition.GenerateEmptyCondition() : accessCondition;
            protocol.Write(blob => blob.UploadFromStream(source, ac, options, operationContext), ac);
        }


        public Task UploadFromStreamAsync(Stream source, long length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, length, accessCondition, options, operationContext, cancellationToken);
        }

        public Task UploadFromStreamAsync(Stream source, long length, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, length, accessCondition, options, operationContext);
        }

        public Task UploadFromStreamAsync(Stream source, long length, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, length, cancellationToken);
        }

        public Task UploadFromStreamAsync(Stream source, long length)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, length);
        }

        public Task UploadFromStreamAsync(Stream source, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, accessCondition, options, operationContext, cancellationToken);
        }

        public Task UploadFromStreamAsync(Stream source, AccessCondition accessCondition, BlobRequestOptions options, OperationContext operationContext)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, accessCondition, options, operationContext);
        }

        public Task UploadFromStreamAsync(Stream source, CancellationToken cancellationToken)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source, cancellationToken);
        }

        public Task UploadFromStreamAsync(Stream source)
        {
            return protocol.MainPrimary().UploadFromStreamAsync(source);
        }

        public CloudBlobContainer Container
        {
            get { return protocol.MainPrimary().Container; }
        }

        public CloudBlobDirectory Parent
        {
            get { return protocol.MainPrimary().Parent; }
        }

        public StorageUri StorageUri
        {
            get { return protocol.MainPrimary().StorageUri; }
        }

        public Uri Uri
        {
            get { return protocol.MainPrimary().Uri; }
        }

    }
}
