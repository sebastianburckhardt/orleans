using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.LogViews
{
    // superclass for all grains that use a log view provider to manage their state
    // (such as event-sourced grains and queued grains)
    public abstract class LogViewGrain<TLogView> : Grain
    {

    }
}
