using Computation.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using Orleans;

#pragma warning disable 1998

namespace Computation.Grains
{
    /// <summary>
    /// An implementation of a non-persistent, non-sequenced leaderboard grain
    /// To be used for reference performance
    /// </summary>
    public class ComputationGrain : Orleans.Grain, Computation.Interfaces.IComputationGrain
    {

        byte[] payload = new byte[100];

        #region Queries

        // public Task<Score[]> GetTopTen()
        public Task<byte[]> Read(string post)
        {
            return Task.FromResult(payload);

        }

        #endregion

        #region Updates

        public Task Write(int pTime)
        {
            var start = DateTime.Now;
            var end = DateTime.Now;

            int i = 0;
            while ((end - start).TotalMilliseconds < pTime)
            {
                i++;
                end = DateTime.Now;
            }
            return TaskDone.Done;
        }


        #endregion


    }

}
