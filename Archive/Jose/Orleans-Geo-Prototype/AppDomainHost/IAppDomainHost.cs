using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans.Host
{
    public interface IAppDomainHost
    {
        MarshalByRefObject Load(string className, string assemblyName, object[] args);
        void Ping();
    }
}
