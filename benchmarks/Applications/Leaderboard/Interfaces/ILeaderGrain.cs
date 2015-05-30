using Orleans;
using System;
using System.Threading.Tasks;

namespace Leaderboard.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ILeaderboardGrain : Orleans.IGrain
    {
        Task Post(Score score);
        Task<Score[]> GetTopTen(string reqId);

    }

}
