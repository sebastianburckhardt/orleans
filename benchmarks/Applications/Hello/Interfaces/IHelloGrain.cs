using Orleans;
using System.Threading.Tasks;

namespace Hello.Interfaces
{
    /// <summary>
    /// Grain interface IGrain1
    /// </summary>
    public interface IHelloGrain : IGrain
    {
        Task<string> Hello(string s);
    }
}
