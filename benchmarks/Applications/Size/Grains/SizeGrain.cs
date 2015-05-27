using Size.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using Orleans;

#pragma warning disable 1998

namespace Size.Grains
{
    /// <summary>
    /// An implementation of a non-persistent, non-sequenced leaderboard grain
    /// To be used for reference performance
    /// </summary>
    public class SizeGrain : Orleans.Grain, Size.Interfaces.ISizeGrain

    {

        private byte[] payload;


        #region Queries

       // public Task<Score[]> GetTopTen()
        public Task<byte[]> Read(string post)
        {
           return Task.FromResult(payload);

        }

        #endregion

       #region Updates

        public Task Write(byte[] pPayload)
        {
            this.payload = pPayload;
            return TaskDone.Done;
        }


    #endregion
          
      
    }
    
}
