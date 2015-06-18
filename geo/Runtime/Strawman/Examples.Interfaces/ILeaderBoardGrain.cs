using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Runtime.Strawman.ReplicatedGrains.Examples.Interfaces
{
    // The grain supports two operations, to post a score and to read top ten scores
    public interface ILeaderBoardGrain : IGrain
    {
        Task Post(Score score);
        Task<Score[]> GetTopTen();
    }

    public struct Score : IEquatable<Score>
    {
        public string Name;
        public long Points;
        public bool Equals(Score s) { return s.Name == Name && s.Points == Points; }
    }


}
