using Examples.Interfaces;
using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReplicatedGrains
{

    public class DebuggingControls : Grain, IDebuggingControls
    {
        // meant for single-silo debugging, thus static fields are shared by all grains
        public static uint ArtificialReadDelay =0;
        public static uint ArtificialWriteDelay = 0;
        public static uint StalenessBound = 0;
        public static bool Trace = true;

        public async Task SetArtificialReadDelay(uint delay)
        {
            DebuggingControls.ArtificialReadDelay = delay;
        }
        public async Task SetArtificialWriteDelay(uint delay)
        {
            DebuggingControls.ArtificialWriteDelay = delay;
        }
        public async Task SetStalenessBound(uint bound)
        {
            DebuggingControls.StalenessBound = bound;
        }
    }
}
