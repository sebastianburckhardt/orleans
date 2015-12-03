﻿
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans;
using Orleans.Providers;
using Orleans.Replication;

 

namespace Examples.Grains
{

    /// <summary>
    /// The state of the leaderboard grain
    /// </summary>
    [Serializable]
    public class LeaderBoardState : GrainState
    {
        // we save the top ten scores in this list.
        // it's a public property so it gets serialized/deserialized
        public List<Score> TopTenScores { get; set; }

        // We define a default constructor to ensure that the list is never null
        public LeaderBoardState()
        {
            TopTenScores = new List<Score>();
        }
    }

    /// <summary>
    /// The class that defines the update operation when a new score is posted
    /// </summary>
    [Serializable]
    public class ScorePostedEvent : IUpdateOperation<LeaderBoardState>
    {
        /// <summary>
        /// The posted score.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public Score Score { get; set; }

        /// <summary>
        /// How to update the leaderboard state based on a posted score.
        /// </summary>
        /// <param name="state"></param>
        public void Update(LeaderBoardState leaderboard)
        {
            // add the score to the list of scores
            leaderboard.TopTenScores.Add(Score);
            // sort by points, descending
            leaderboard.TopTenScores.Sort((s1,s2) => s2.Points.CompareTo(s1.Points));
            // keep only the first 10
            while (leaderboard.TopTenScores.Count > 10)
                leaderboard.TopTenScores.RemoveAt(10);
        }
    }

   /// <summary>
   /// The grain implementation
   /// </summary>
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class LeaderBoardGrain : QueuedGrain<LeaderBoardState>, ILeaderBoardGrain
    {
        public Task<Score[]> GetTopTen()
        {
            return Task.FromResult(TentativeState.TopTenScores.ToArray());
        }

        public Task Post(Score score)
        {
            EnqueueUpdate(new ScorePostedEvent() { Score = score });
            return TaskDone.Done;
        }
    }

   
}

   