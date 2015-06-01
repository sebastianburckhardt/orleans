using Orleans;
using ReplicatedGrains;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Examples.Interfaces;
using Orleans.Providers;
#pragma warning disable 1998

namespace Examples.Grains
{

    // An implementation of the leaderboard based on sequenced updates.
    [StorageProvider(ProviderName = "AzureStore")]
    public class CommentsGrain : SequencedGrain<CommentsGrain.State>, ICommentsGrain
    {

        // Grain Operations

        public async Task<Entry[]> Get(DateTime olderthan, int limit)
        {
            IEnumerable<Entry> allentries = (await GetLocalStateAsync()).Entries;
            return allentries.Where(e => e.Timestamp <= olderthan).OrderByDescending(e => e.Timestamp).Take(limit).ToArray();
        } 
        public async Task Post(Entry entry)
        {
            await UpdateLocallyAsync(new PostUpdate() { Entry = entry },false);
        }
        public async Task Delete(Entry entry)
        {
            await UpdateLocallyAsync(new DeleteUpdate() { Entry = entry },false);
        }
        public async Task DeleteRange(DateTime from, DateTime to)
        {
            await UpdateLocallyAsync(new DeleteRangeUpdate() { From = from, To = to },false);
        }


        // Definition of State and Updates

        [Serializable]
        public new class State
        {
            public List<Entry> Entries { get; set; }
        }

        [Serializable]
        public class PostUpdate : IAppliesTo<State>
        {
            public Entry Entry { get; set; }
            public void Update(State s)
            {
                s.Entries.Add(Entry);
            }
        }
    
        [Serializable]
        public class DeleteUpdate : IAppliesTo<State>
        {
            public Entry Entry { get; set; }
            public void Update(State s)
            {
                s.Entries.Remove(Entry);
            }
        }
  
        [Serializable]
        public class DeleteRangeUpdate : IAppliesTo<State>
        {
            public DateTime From { get; set; }
            public DateTime To { get; set; }
            public void Update(State s)
            {
                s.Entries.RemoveAll(e => e.Timestamp >= From && e.Timestamp <= To);
            }
        }
 


    }
}

   