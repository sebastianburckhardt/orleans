using Orleans;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace GeoOrleans.Benchmarks.Leaderboard.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface IPersistentLeaderboardGrain : Orleans.IGrain
    {
        Task Post(Score score, bool persist);

        Task<List<Score>> GetTopTen(string reqId, bool reread);
    }

}
