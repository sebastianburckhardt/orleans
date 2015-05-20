using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;
using System.Threading;


using SimpleGrain;
using UnitTestGrains;

namespace ProxyErrorGrain
{
    [LocalPlacementAttribute]
    public interface IProxyErrorGrain : ISimpleGrain
    {
        Task ConnectTo(IErrorGrain errorGrain);
        Task SetAError(int a);
        Task<string> GetRuntimeInstanceId();
        Task ConnectToProxy(IProxyErrorGrain proxy);
        Task<string> GetProxyRuntimeInstanceId();
        Task<string> GetActivationId();
        //Task MoveActivation(SiloAddress fromSilo, ActivationId activationId, SiloAddress toSilo);
    }
}