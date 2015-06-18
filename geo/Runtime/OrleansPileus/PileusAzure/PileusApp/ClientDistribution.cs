using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MathNet.Numerics.Distributions;

namespace PileusApp
{

    /// <summary>
    /// Returns the probability of a given tick with normal distribution.
    /// 
    /// TODO: extend this class to support various distributions and even traces.
    /// </summary>
    public class ClientDistribution
    {

        private int totalTicks;
        private Normal normalDis;

        int round;

        public ClientDistribution(int totalTicks,double mean , double stddev)
        {
            this.totalTicks = totalTicks;

            normalDis = new Normal(mean, stddev);
            round = 0;
        }

        public double GetNextProbability(int currentTick )
        {
            if (currentTick > (round + 1) * totalTicks)
            {
                //we start over.
                round++;
            }

            return normalDis.Density(currentTick - (round * totalTicks));
        }

    }
}
