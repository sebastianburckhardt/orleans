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
    public class LeaderBoardGrain : SequencedGrain<LeaderBoardGrain.State>, ILeaderBoardGrain
    {
        [Serializable]
        public new class State
        {
            public List<Score> TopTenScores { get; set; }
        }

        #region Queries

        public async Task<Score[]> GetTopTen(string post)
        {
            return (await GetLocalStateAsync()).TopTenScores.ToArray();
        }

        #endregion

        #region Updates

        public async Task Post(Score score)
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

