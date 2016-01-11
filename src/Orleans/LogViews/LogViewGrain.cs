using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    // marker class for grains that use a log view provider to manage their state
    public abstract class LogViewGrain<TLogView> : Grain
    {

    }
}
