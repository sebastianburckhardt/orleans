using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.QueuedGrains;

namespace ReplicatedEventSample.Grains
{
    [Serializable]
    public class Outcome
    {
        /// <summary>
        /// Name of the game participant
        /// </summary>
        public string Name;

        /// <summary>
        /// Score achieved
        /// </summary>
        public int Score;

        /// <summary>
        /// Time at which the score was achieved
        /// </summary>
        public DateTime Timestamp;

    }
}
