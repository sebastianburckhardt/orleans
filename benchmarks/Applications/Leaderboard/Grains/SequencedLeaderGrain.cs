using Orleans;
using ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Leaderboard.Interfaces;
using Orleans.Providers;
#pragma warning disable 1998

namespace Leaderboard.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    // all operations are synchronous

    [StorageProvider(ProviderName = "AzureStore")]
    public class SequencedLeaderboardGrain : SequencedGrain<SequencedLeaderboardGrain.State>, Leaderboard.Interfaces.ISequencedLeaderboardGrain
    {
        [Serializable]
        public new class State
        {
            public List<Score> TopTenScores { get; set; }
        }

        #region Queries

        public async Task<Score[]> GetApproxTopTen(string post)
        {
            return (await GetLocalStateAsync()).TopTenScores.ToArray();
        }

        public async Task<Score[]> GetExactTopTen(string post)
        {
            return (await GetGlobalStateAsync()).TopTenScores.ToArray();
        }

        #endregion

        #region Updates

        public async Task PostNow(Score score)
        {
            await UpdateGloballyAsync(new ScorePostedEvent() { Score = score });
        }


        public async Task PostLater(Score score)
        {
            await UpdateLocallyAsync(new ScorePostedEvent() { Score = score });
        }

        [Serializable]
        public class ScorePostedEvent : IAppliesTo<State>
        {
            public Score Score { get; set; } // the posted score
            public void Update(State state)
            {
                // add the score to the list of scores
                state.TopTenScores.Add(Score);
                // sort the list of scores and keep only top 10
                state.TopTenScores = state.TopTenScores.OrderBy((Score s) => s.Points).Take(10).ToList();
            }
        }

        #endregion


    }
}

