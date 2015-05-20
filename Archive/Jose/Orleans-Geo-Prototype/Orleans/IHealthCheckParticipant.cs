using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;

using Orleans.Counters;


namespace Orleans
{
    internal interface IHealthCheckParticipant
    {
        bool CheckHealth(DateTime lastCheckTime);
    }
}

