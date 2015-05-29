using Orleans;
using Hello.Interfaces;
using System.Threading.Tasks;
using Common;
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
            using (new TraceInterval("Orleans - answer", int.Parse(arg)))
            {
                return "Hello From Orleans #" + arg;
            }
        }
    }
}
