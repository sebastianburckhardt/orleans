using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Examples.Interfaces
{
    /// <summary>
    /// The grain interface for the counter grain.
    /// </summary>
    public interface ICounterGrain : IGrain
    {
        /// <summary>
        /// Return the current count.
        /// </summary>
        Task<int> Get();

        /// <summary>
        /// Increment the current count.
        /// </summary>
        Task Increment();
    }
}
