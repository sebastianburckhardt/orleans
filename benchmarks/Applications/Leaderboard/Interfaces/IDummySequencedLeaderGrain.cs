using Orleans;
using System;
using System.Threading.Tasks;

namespace Leaderboard.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface IDummySequencedLeaderboardGrain : Orleans.IGrain
    {
       Task PostNow(Score score);
       Task PostLater(Score score);

       Task<Score[]> GetExactTopTen(string reqId);
       Task<Score[]> GetApproxTopTen(string reqId);


    }

}
