using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Pileus;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;
using Microsoft.WindowsAzure.Storage.Pileus.Utils;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Constraint;
using System.Diagnostics;
using System.IO;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration.Actions;
using System.Threading;

namespace GeoOrleans.Runtime.OrleansPileus.Common
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public class Utils
    {
        public static Dictionary<string, CloudStorageAccount> GetStorageAccounts(bool useHttps)
        {
            Dictionary<string, CloudStorageAccount> result = new Dictionary<string, CloudStorageAccount>();
            string accountName = "";
            string accountKey = "";
            StorageCredentials creds;
            CloudStorageAccount httpAcc;


            #region azure
            /*
            accountName = "geopileususwest";
            accountKey = "wCcSNHKbMbjNM6lldtv4xIKzwGy52F9+04Z5uy62juqeAhcBCfrstJZdz6qi6UWXFlY1BmJa3Fag8edrsucT6g==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            */

            /*
            accountName = "geopileuseurope";
            accountKey = "wCcSNHKbMbjNM6lldtv4xIKzwGy52F9+04Z5uy62juqeAhcBCfrstJZdz6qi6UWXFlY1BmJa3Fag8edrsucT6g==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            */
            #endregion

            #region local
            accountName = "devstoreaccount1";
            httpAcc = CloudStorageAccount.Parse("UseDevelopmentStorage=true");
            result.Add(httpAcc.Credentials.AccountName, httpAcc);
            #endregion

            return result;
        }


        public static byte[] GetBlob(string pBlobName, CapCloudBlobContainer pContainer)
        {
            try
            {
                MemoryStream ms = new MemoryStream();
                ICloudBlob blob = pContainer.GetBlobReference(pBlobName);
                blob.DownloadToStream(ms);
                byte[] data = ms.GetBuffer();
                ms.Close();
                return data;
            }
            catch (StorageException se)
            {
                if (StorageExceptionCode.NotFound(se))
                {
                    return null;
                }
            }
            catch (Exception e)
            {
                Trace.TraceError(e.ToString());
                throw e;
            }
            return null;

        }

        /// <summary>
        /// Acquires Blob
        /// </summary>
        /// <param name="blobName"></param>
        /// <param name="cont"></param>
        /// <returns></returns>
        public static byte[] GetBlob(string pBlobName, CapCloudBlobContainer pContainer, ReadWriteFramework pReadWriteFramework)
        {
            Trace.TraceInformation("GetBlob");

           MemoryStream ms = new MemoryStream();
           pReadWriteFramework.Read(blob => blob.DownloadToStream(ms));
            byte[] data = ms.GetBuffer();
            ms.Close();
            return data;

        }

        public static void PutBlob(string pBlobName, byte[] pData, CapCloudBlobContainer pContainer)
        {
            Trace.TraceInformation("PutBlob");
           AccessCondition ac = AccessCondition.GenerateEmptyCondition();
            ICloudBlob blob = pContainer.GetBlobReference(pBlobName);
            using (var ms = new MemoryStream(pData))
            {
                blob.UploadFromStream(ms,ac);
              //  AccessCondition ac = AccessCondition.GenerateEmptyCondition();
            //    pReadWriteFramework.Write(blob => blob.UploadFromStream(ms, ac), ac, sessions, pContainer.Monitor);
            }

        }


        /// <summary>
        /// Puts Bob into appropariate container
        /// </summary>
        /// <param name="blobName"></param>
        /// <param name="data"></param>
        /// <param name="cont"></param>
        /// <returns></returns>
        public static  void PutBlob(string pBlobName, byte[] pData, CapCloudBlobContainer pContainer, ReadWriteFramework pReadWriteFramework)
        {
            List<SessionState> sessions = new List<SessionState>();
            sessions.Add(pContainer.Sessions["strong"]);

            using (var ms = new MemoryStream(pData))
            {
                AccessCondition ac = AccessCondition.GenerateEmptyCondition();
                pReadWriteFramework.Write(blob => blob.UploadFromStream(ms, ac), ac, sessions, pContainer.Monitor);
            }

        }



        /// <summary>
        /// Creates a simple SLA with a single desired consistency and a large latency.
        /// This forces reads to be performed at the closest replica with that consistency.
        /// </summary>
        /// <param name="cons"></param>
        /// <returns></returns>
        public static ServiceLevelAgreement CreateConsistencySla(Consistency cons)
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(cons.ToString("g"));
            SubSLA subSla1 = new SubSLA(2000, cons, 0, 1);
            sla.Add(subSla1);
            return sla;
        }

        public static ServiceLevelAgreement CreateConsistencySla(Consistency cons, int latency, string Name)
        {
            ServiceLevelAgreement sla = new ServiceLevelAgreement(Name);
            SubSLA subSla1 = new SubSLA(latency, cons, 0, 1);
            sla.Add(subSla1);
            return sla;
        }


        public static string PrintCurrentConfiguration(string pContainerName)
        {
            string result = null;
            ReplicaConfiguration config = ClientRegistry.GetConfiguration(pContainerName, false);
            result += "Current configuration for " + config.Name + ":" + "\r\n";
            result += "Primary: ";
            bool first = true;
            foreach (string name in config.PrimaryServers)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    result += ", ";
                }
                result += name;
            };
            result += "\r\n";
            result += "Secondaries: ";
            first = true;
            foreach (string name in config.SecondaryServers)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    result += ", ";
                }
                result += (name);
            };
            result += "\r\n";
            return result;
        }

    }
}
