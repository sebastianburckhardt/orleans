using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface IOrderTestGrain : IGrain
    {
        Task<string> Name { get; }

        Task Next(string client, int number, List<int> previous, int delay);
    }

}
