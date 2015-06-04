﻿using Orleans;
using System.Threading.Tasks;

namespace Hello.Interfaces
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public interface ITCPReceiverGrain : IGrain
    {
          Task<string> listenMessages();
    }
}
