using Orleans;
using System.Threading.Tasks;

namespace Hello.Interfaces
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public interface ITCPSenderGrain : IGrain
    {
        Task<string> SayHello(string s);
    }
}
