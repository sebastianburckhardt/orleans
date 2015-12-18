using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Replication
{
    // marker class for replicated grains of all kinds
    public abstract class ReplicatedGrain<T> : Grain
    {

    }
}
