using LeaderboardPileus.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Linq;
using System;
using Orleans;
using Orleans.Providers;

#pragma warning disable 1998

namespace LeaderboardPileus.Grains
{

    public interface ILeaderboardGrainState : IGrainState
    {
        List<Score> TopTenScores { get; set; }
    }

    /// <summary>
    /// An implementation of a non-persistent, non-sequenced leaderboard grain
    /// To be used for reference performance
    /// </summary>
    [StorageProvider(ProviderName = "AzureStore")]
    public class PersistentLeaderGrain : Grain<ILeaderboardGrainState>, LeaderboardPileus.Interfaces.IPersistentLeaderboardGrain
    {
        public async Task Post(Score score, bool persist)
        {
            if (State.TopTenScores == null)
            {
                State.TopTenScores = new List<Score>();
            }
            State.TopTenScores.Add(score);
            State.TopTenScores = State.TopTenScores.OrderByDescending((s) => s.Points).Take(10).ToList();
            if (persist)
            {
                await State.WriteStateAsync();
            }
        }

        public async Task<List<Score>> GetTopTen(string reqId, bool reread)
        {
            if (reread)
            {
                await State.ReadStateAsync();
            }
            return State.TopTenScores;
        }
    }

}
