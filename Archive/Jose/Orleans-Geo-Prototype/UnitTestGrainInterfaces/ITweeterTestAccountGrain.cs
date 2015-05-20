using System.Collections.Generic;
using System;
using System.Threading.Tasks;

namespace Orleans.Samples.Tweeter.GrainInterfaces
{
    public interface ITweetTestPublisher : IGrain
    {
        Task<long> UserId { get; }

        Task<string> UserAlias { get; }

        Task<string> DisplayName { get; }
    }


    public interface ITweetTestSubscriber : IGrainObserver
    {
    }


    public interface ITweeterTestAccountGrain : IGrain, ITweetTestPublisher, ITweetTestSubscriber
    {
    }
}
