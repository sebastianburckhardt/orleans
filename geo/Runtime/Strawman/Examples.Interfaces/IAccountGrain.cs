using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Runtime.Strawman.ReplicatedGrains.Examples.Interfaces
{
    public interface IAccountGrain : IGrain
    {
      
        Task<uint> EstimatedBalance();
        Task<uint> ActualBalance();
        Task ReliableDeposit(uint amount);
        Task UnreliableDeposit(uint amount);
        Task<bool> Withdraw(uint amount);
    }

  
}
