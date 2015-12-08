using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Examples.Interfaces
{
    /// <summary>
    /// The grain interface for the chat grain.
    /// </summary>
    public interface IChatGrain : IGrain
    {
        /// <summary>
        /// Return a subsequence of all comments, sorted from newer to older.
        /// </summary>
        /// <param name="olderthan">The datetime from where to start.</param>
        /// <param name="limit">A limit on the number of returned entries.</param>
        /// <returns></returns>
        Task<Comment[]> Get(DateTime olderthan, int limit);

        /// <summary>
        /// Add a new comment to the list of comments.
        /// <returns>the created comment object</returns>
        /// </summary>
        Task<Comment> Post(string user, string text);

        /// <summary>
        /// Delete a specific comment from the list.
        /// Has no effect if no matching comment is found.
        /// </summary>
        Task Delete(Comment entry);

        /// <summary>
        /// Delete all comments within the specified range.
        /// </summary>
        Task DeleteRange(DateTime from, DateTime to);
    }

    /// <summary>
    /// The class defining chat entries.
    /// </summary>
    [Serializable]
    public struct Comment : IEquatable<Comment>
    {
        /// <summary>
        /// The user who wrote the comment.
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// The UTC timestamp of the comment.
        /// </summary>
        public DateTime Timestamp { get; set; }

        /// <summary>
        /// The posted text.
        /// </summary>
        public string Text { get; set; }


        public bool Equals(Comment other)
        {
            return this.User == other.User && this.Timestamp == other.Timestamp && this.Text == other.Text;
        }
        public override int GetHashCode()
        {
            return this.User.GetHashCode() ^ this.Timestamp.GetHashCode() ^ this.Text.GetHashCode();
        }
    }

}
