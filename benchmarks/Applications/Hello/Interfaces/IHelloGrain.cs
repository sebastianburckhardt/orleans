using Orleans;

namespace Interfaces
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public interface IHelloGrain : IGrain
    {
        public Task Hello(string s);
    }
}
