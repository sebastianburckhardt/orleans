using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace PileusApp
{
    /// <summary>
    /// Note that changes in this class cannot commit into github repository. 
    /// To disable this, and save changes into github run the following command: git update-index --no-assume-unchanged Account.cs
    /// </summary>
    public static class Account
    {
        /// <summary>
        /// Returns a Dictionary of storage name along with the storage account. 
        /// 
        /// Note that the key of the Dictionary which is <see cref="CloudStorageAccount.Credentials.AccountName"/>.
        /// The notion of servername and storage account name are used interchanbly in this project.
        /// </summary>
        /// <param name="useHttps"></param>
        /// <returns></returns>
        public static Dictionary<string, CloudStorageAccount> GetStorageAccounts(bool useHttps)
        {
            Dictionary<string, CloudStorageAccount> result = new Dictionary<string, CloudStorageAccount>();
            string accountName = "";
            string accountKey = "";
            StorageCredentials creds;
            CloudStorageAccount httpAcc;


            #region doug
            /*
            accountName = "dbteuropestorage";
            accountKey = "wCcSNHKbMbjNM6lldtv4xIKzwGy52F9+04Z5uy62juqeAhcBCfrstJZdz6qi6UWXFlY1BmJa3Fag8edrsucT6g==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            accountName = "dbteastasiastorage";
            accountKey = "mGarqTpCLNW5Lc+hVz18pFIGgP8/4fbL0EGdfA939X8y0DkaNTcOieuL8Hd0lOqDiH/7+8J5+DgD/M7l+H7qDg==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            accountName = "dbtsouthstorage";
            accountKey = "d0TDFITH4QMDmVQwtguVUPr7gjD4Pt3ZIhEFWXdflObcyWx7KXzGAYaP2cGzZKM7jAyi/D8t5PZH1XsQg+O3RA==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            accountName = "dbtwestusstorage";
            accountKey = "6zNr+YZ+sE5bhH+kbQRWF06t+zCVmjfhjl77AJb8HkyybgS/sF4ol4ndJVJhUCFvi8TOyBWzcUri9bHDkMPc1w==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            accountName = "dbtbrazilstorage";
            accountKey = "hEqR1N6ZzobmAAk/YaJTaeN/NvCwC8tb2lb4Agx63HiTsi6jqvMIelIPML1t0e6g9g0fUFqr2EPsLgBfEeBYAw==";
            creds = new StorageCredentials(accountName, accountKey);
            httpAcc = new CloudStorageAccount(creds, useHttps);
            result.Add(httpAcc.Credentials.AccountName, httpAcc);

            accountName = "dbtjapanweststorage";
            accountKey = "BUYvgCD8m4TNgW7WqVDVsD0hu9UCZBbKbTgrsVpwiXyGiI6KX5MR6O3XGUgSsW+Wv1a4bVNH0Fs6VKclDCoAFw==";
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

    }
}
