using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using Microsoft.AspNet.SignalR;
using System.Threading;
using System.Net.WebSockets;
using GeoOrleans.Runtime.Common;
using System.Threading.Tasks;
using System.Text;

namespace Conductor.Webrole
{
    public class CommandHub : Hub
    {



        public void Send(string command)
        {
            var conductor = Conductor.Instance;

            conductor.Hub = this;

            conductor.Typed(command);
        }

    }
}