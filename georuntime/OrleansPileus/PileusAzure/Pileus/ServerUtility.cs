using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Microsoft.WindowsAzure.Storage.Pileus
{
    public class ServerUtility
    {
        private ServerState ss;
        private float utility;

        public ServerUtility(ServerState s, float u)
        {
            ss = s;
            utility = u;
        }

        public ServerState Server { get { return ss; } }
        public float Utility { get { return utility; } }
    }
}