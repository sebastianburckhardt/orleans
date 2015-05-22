using Leaderboard.Interfaces;
using Orleans;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System; 

#pragma warning disable 1998

namespace Leaderboard.Grains
{
    /// <summary>
    /// An implementation of a non-persistent, non-sequenced leaderboard grain
    /// To be used for reference performance
    /// </summary>
    public class LeaderGrain : Orleans.Grain, Leaderboard.Interfaces.ILeaderBoardGrain

    {
       
         List<Score> topTenScores_ { get; set; }


        #region Queries

        public async Task<Score[]> GetTopTen()
        {
           return (topTenScores_.ToArray());
        }

        #endregion

        #region Updates

        public async Task Post(Leaderboard.Interfaces.Score score)
        {
            topTenScores_.Add(score);
            topTenScores_ = topTenScores_.OrderBy((Score s) => s.Points).Take(10).ToList();
        }

    #endregion
          
      
    }
    
}
