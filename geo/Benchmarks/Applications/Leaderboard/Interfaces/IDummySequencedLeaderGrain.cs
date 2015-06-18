using Orleans;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;


namespace GeoOrleans.Benchmarks.Leaderboard.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface IDummySequencedLeaderboardGrain : Orleans.IGrain
    {
        Task PostNow(Score score);
        Task PostLater(Score score);

        Task DummyCall();

        Task<List<Score>> GetExactTopTen(string reqId);
        Task<List<Score>> GetApproxTopTen(string reqId);


    }

}
