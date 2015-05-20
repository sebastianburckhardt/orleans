using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Orleans;

using System.Collections;

namespace BenchmarkGrains
{

    public interface IChainedGrain : IGrain
    {
        Task<int> Id { get; }
        Task<int> X { get; }
        Task<IChainedGrain> Next { get; }
        //[ReadOnly]
        Task<int> GetCalculatedValue();
        Task SetNext(IChainedGrain next);
        //[ReadOnly]
        Task Validate(bool nextIsSet);
        Task PassThis(IChainedGrain next);
    }
}
