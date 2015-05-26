using Leaderboard.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using Orleans;

#pragma warning disable 1998

namespace Leaderboard.Grains
{
    /// <summary>
    /// An implementation of a non-persistent, non-sequenced leaderboard grain
    /// To be used for reference performance
    /// </summary>
    public class LeaderGrain : Orleans.Grain, Leaderboard.Interfaces.ILeaderboardGrain

    {

        private List<Score> topTenScores_ = new List<Score>();


        #region Queries

       // public Task<Score[]> GetTopTen()
        public Task<Score[]> GetTopTen(string post)
        {
           return Task.FromResult(topTenScores_.ToArray());

        }

        #endregion

       #region Updates

        public Task Post(Leaderboard.Interfaces.Score score)
        {
            topTenScores_.Add(score);
            topTenScores_ = topTenScores_.OrderByDescending((Score s) => s.Points).Take(10).ToList();
            return TaskDone.Done;
        }


    #endregion
          
      
    }
    
}
