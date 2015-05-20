using System.Threading.Tasks;

using Orleans.Samples.Tweeter.GrainInterfaces;

namespace Orleans.Samples.Tweeter.Grains
{
    public interface ITweeterTestAccountState : IGrainState
    {
        long UserId { get; }
        string UserAlias { get;}
        string DisplayName { get; }
    }

    public class TweeterTestAccountGrain : GrainBase<ITweeterTestAccountState>, ITweeterTestAccountGrain
    {
        
        #region ITweeterActor properties

        Task<long> ITweetTestPublisher.UserId { get
        {
            Logger.GetLogger("TweeterTest", Logger.LoggerType.Grain).Info("Reading UserId; value is {0}", State.UserId);
            return Task.FromResult(State.UserId); } }
        Task<string> ITweetTestPublisher.UserAlias { get { return Task.FromResult(State.UserAlias); } }
        Task<string> ITweetTestPublisher.DisplayName { get { return Task.FromResult(State.DisplayName); } }

        #endregion
    }
}
