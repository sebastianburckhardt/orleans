﻿using Orleans;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Examples.Interfaces
{
    public interface IAccountGrain : IGrainWithStringKey
    {
      
        Task<uint> EstimatedBalance();
        Task<uint> ActualBalance();

        Task ReliableDeposit(uint amount);
        Task UnreliableDeposit(uint amount);
        
        Task<bool> Withdraw(uint amount);
    }


  
}
