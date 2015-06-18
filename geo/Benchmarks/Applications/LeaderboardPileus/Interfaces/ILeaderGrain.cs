using Orleans;
using System;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace GeoOrleans.Benchmarks.LeaderboardPileus.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ILeaderboardGrain : Orleans.IGrainWithStringKey
    {
        Task Post(Score score);
        Task<List<Score>> GetTopTen(string reqId);

    }

}
