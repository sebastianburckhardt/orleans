using Orleans;
using Hello.Interfaces;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Hello.Grains
{
    /// <summary>
    /// Grain implementation class Grain1.
    /// </summary>
    public class HelloGrain : Grain, IHelloGrain
    {
        public async Task<string> Hello(string arg)
        {
            // echo
            return arg;
        }
    }
}
