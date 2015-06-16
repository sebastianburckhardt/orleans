using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using LeaderboardPileus.Interfaces;
using Orleans.Providers;
using OrleansPileus.ReplicatedGrains;
#pragma warning disable 1998

namespace LeaderboardPileus.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    // all operations are synchronous

    public class SequencedLeaderboardGrain : OrleansPileus.ReplicatedGrains.PileusSequencedGrain<SequencedLeaderboardGrain.State>, LeaderboardPileus.Interfaces.ISequencedLeaderboardGrain
    {
        [Serializable]
        public new class State
        {
            public List<Score> topTenScores { get; set; }

            public State()
            {
                topTenScores = new List<Score>();
            }

        }

        #region Queries

        public async Task<List<Score>> GetApproxTopTen(string post)
        {
            State s = await GetLocalStateAsync();
            s = (await GetLocalStateAsync());
            if (s != null)
            {
                return s.topTenScores;
            }
            else
            {
                return null;
            }
        }

        public async Task<List<Score>> GetExactTopTen(string post)
        {
            State s = await GetGlobalStateAsync();
            s = (await GetGlobalStateAsync());
            if (s != null)
            {
                return s.topTenScores;
            }
            else
            {
                return null;
            }
        }

        #endregion

        #region Updates

        public async Task PostNow(Score score)
        {
            await UpdateGloballyAsync(new ScorePostedEvent() { Score = score });
        }


        public async Task PostLater(Score score)
        {
            await UpdateLocallyAsync(new ScorePostedEvent() { Score = score },false);
        }

        public override Task OnActivateAsync()
        {

            return base.OnActivateAsync();
        }

        [Serializable]
        public class ScorePostedEvent : IAppliesTo<State>
        {
            public Score Score { get; set; } // the posted score
            public void Update(State state)
            {
                if (state == null)
                {
                    state = new State();
                }
                // add the score to the list of scores
                state.topTenScores.Add(Score);
                // sort the list of scores and keep only top 10
                state.topTenScores = state.topTenScores.OrderByDescending((Score s) => s.Points).Take(10).ToList();
            }
        }

        #endregion


    }
}

