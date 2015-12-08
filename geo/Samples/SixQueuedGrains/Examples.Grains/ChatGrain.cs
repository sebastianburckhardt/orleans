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

    #region Classes for State And Updates

    /// <summary>
    /// The state of the chat grain
    /// </summary>
    public class ChatState : GrainState
    {
        // the current list of comments.
        // it's a public property so it gets serialized/deserialized
        public List<Comment> Entries { get; set; }

        // We define a default constructor to ensure that the list is never null
        public ChatState()
        {
            Entries = new List<Comment>();
        }
    }


    /// <summary>
    /// The class that defines the update operation when a comment is posted
    /// </summary>
    [Serializable]
    public class CommentPostedEvent : IUpdateOperation<ChatState>
    {
        /// <summary>
        /// The posted comment.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public Comment Comment { get; set; }

        /// <summary>
        /// Effect of posting a comment.
        /// </summary>
        public void Update(ChatState state)
        {
            state.Entries.Add(Comment);
        }
    }

    /// <summary>
    /// The class that defines the update operation when a comment is deleted
    /// </summary>
    [Serializable]
    public class CommentDeletedEvent : IUpdateOperation<ChatState>
    {
        /// <summary>
        /// The deleted comment.
        /// We define this as a public property so it gets serialized/deserialized.
        /// </summary>
        public Comment Comment { get; set; }

        /// <summary>
        /// Effect of deleting a comment.
        /// </summary>
        public void Update(ChatState state)
        {
            var ignore = state.Entries.Remove(Comment);
            // deleting a non-existing comment is a no-op.
        }
    }

    /// <summary>
    /// The class that defines the update operation when all comments in a date range are deleted
    /// </summary>
    [Serializable]
    public class CommentRangeDeletedEvent : IUpdateOperation<ChatState>
    {
        // define the range.
        // These are public properties so they get serialized/deserialized.
        public DateTime From { get; set; }
        public DateTime To { get; set; }

        /// <summary>
        /// Effect of deleting all comments in the range.
        /// </summary>
        public void Update(ChatState state)
        {
            state.Entries.RemoveAll(e => e.Timestamp >= From && e.Timestamp <= To);
        }
    }


    #endregion

    /// <summary>
    /// The grain implementation for the chat grain.
    /// </summary>
    [ReplicationProvider(ProviderName = "SharedStorage")]
    public class ChatGrain : QueuedGrain<ChatState>, IChatGrain
    {
        public async Task<Comment[]> Get(DateTime olderthan, int limit)
        {
            return TentativeState.Entries
                .Where(e => e.Timestamp <= olderthan)
                .OrderByDescending(e => e.Timestamp)
                .Take(limit)
                .ToArray();
        } 
 
        public Task<Comment> Post(string user, string text)
        {
            var comment = new Comment() { User = user, Text = text, Timestamp = DateTime.UtcNow };
            EnqueueUpdate(new CommentPostedEvent() { Comment = comment });
            return Task.FromResult(comment);
        }
        public Task Delete(Comment comment)
        {
            EnqueueUpdate(new CommentDeletedEvent() { Comment = comment });
            return TaskDone.Done;
        }
        public Task DeleteRange(DateTime from, DateTime to)
        {
            EnqueueUpdate(new CommentRangeDeletedEvent() { From = from, To = to });
            return TaskDone.Done;
        }

    }
}

   