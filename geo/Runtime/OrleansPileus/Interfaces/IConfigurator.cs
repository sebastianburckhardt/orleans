
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Orleans;
using Microsoft.WindowsAzure.Storage.Pileus.Configuration;


namespace GeoOrleans.Runtime.OrleansPileus.Interfaces
{
    /// <summary>
    /// Grain interface IConfigurator
    /// </summary>
    public interface IConfigurator : IGrainWithStringKey
    {
        Task<bool> forceReconfigure(List<string> pServers);


        Task startConfigurator();

        Task<Dictionary<string, CloudBlobContainer>> getContainers();

        Task receiveUsageData(ClientUsageData pClientData);

    }
}
