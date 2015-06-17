using Orleans;
using GeoOrleans.Benchmarks.Hello.Interfaces;
using System.Threading.Tasks;
using GeoOrleans.Benchmarks.Common;
using GeoOrleans.Runtime.Common;

#pragma warning disable 1998

namespace GeoOrleans.Benchmarks.Hello.Grains
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
