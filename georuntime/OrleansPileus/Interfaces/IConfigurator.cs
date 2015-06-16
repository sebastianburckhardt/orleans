﻿using Common;
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

namespace OrleansPileus.Interfaces
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public interface IConfigurator : IGrainWithStringKey
    {
        Task forceReconfigure();


        Task startConfigurator();

        Task<Dictionary<string, CloudBlobContainer>> getContainers();
    }
}
