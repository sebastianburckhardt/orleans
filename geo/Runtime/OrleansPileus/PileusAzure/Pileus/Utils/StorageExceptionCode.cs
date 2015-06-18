using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.WindowsAzure.Storage.Pileus.Utils
{
    /// <summary>
    /// A convenient way to check for different exception codes from Azure Storage.
    /// </summary>
    static public class StorageExceptionCode
    {
        // Note: Instead of checking the HttpStatusCode, this could check the text of the message.
        // ex.Message.Contains("404")
        // ex.GetBaseException().Message.Contains("404")

        static public bool NotModified(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 304;
        }

        static public bool BadRequest(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 400;
        }

        static public bool Forbidden(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 403;
        }

        static public bool NotFound(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 404;
        }

        static public bool Conflict(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 409;
        }

        static public bool PreconditionFailed(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 412;
        }

        static public bool InternalError(StorageException ex)
        {
            // could be retried
            return ex.RequestInformation.HttpStatusCode == 500;
        }

        static public bool NotImplemented(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 501;
        }

        static public bool ServiceUnavailable(StorageException ex)
        {
            // could be retried
            return ex.RequestInformation.HttpStatusCode == 503;
        }

        static public bool VersionNotSupported(StorageException ex)
        {
            return ex.RequestInformation.HttpStatusCode == 505;
        }

    }
}
