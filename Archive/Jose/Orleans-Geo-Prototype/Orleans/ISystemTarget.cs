using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans
{
    /// <summary>
    /// For internal use only.
    /// This is a markup interface for system target.
    /// System target are internal runtime objects that share some behaivior with grains, but also impose certain restrictions. In particular:
    /// System target are asynchronusly addressable actors, just like any IAddressable.
    /// Proxy class is being generated for ISystemTarget, just like for IAddressable
    /// System target are scheduled by the runtime scheduler and follow turn based concurrency.
    ///      Unlike IAddressable, ISystemTarget imposes an additional restriction on its public inteface:
    ///      all  methods must have as a first argument SiloAddress, which is the explicit address of the ISystemTarget which is the destination of this message.
    /// </summary> 
    internal interface ISystemTarget : IAddressable
    {
    }

    /// <summary>
    /// For internal use only.
    /// Internal interface implemented by SystemTarget classes to expose the necessary internal info that allows this.AsReference to for for SystemTarget's same as it does for a grain class.
    /// </summary>
    internal interface ISystemTargetBase
    {
        SiloAddress CurrentSilo { get; }
        GrainId Grain { get; }
    }

    // Common internal interface for SystemTarget and ActivationData.
    internal interface IInvokable
    {
        IGrainMethodInvoker GetInvoker(int interfaceId, string genericGrainType = null);
    }
}
