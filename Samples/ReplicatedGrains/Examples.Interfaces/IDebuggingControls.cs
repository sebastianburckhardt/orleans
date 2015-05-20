using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace Examples.Interfaces
{
      public interface IDebuggingControls : IGrain
    {
        Task SetArtificialReadDelay(uint delay);
        Task SetArtificialWriteDelay(uint delay);

        Task SetStalenessBound(uint bound);
    }
}
