using Orleans;
using System;
using System.Threading.Tasks;

namespace Leaderboard.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ILeaderBoardGrain : Orleans.IGrain
    {
        Task Post(Score score);
        Task<Score[]> GetTopTen();
    }

    public struct Score : IEquatable<Score>
    {
        // Player Name
        public string Name;
        // Points
        public long Points;
        public bool Equals(Score s) { return s.Name == Name && s.Points == Points; }
        public override string ToString() { return  Name + "-" + Points; }
        // Converts string representation to Score Object
        public static Score fromString(string pScore) { 
            string[] ss = pScore.Split('-');
            return new Score
            {
                Name = ss[0],
                Points = long.Parse(ss[1])
            };
        }
    }
}
