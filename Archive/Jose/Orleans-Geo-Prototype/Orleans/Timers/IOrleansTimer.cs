using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Orleans
{
    /// <summary>
    /// OrleansTimer instances represent a pending timer.
    /// The only operation that is available is Dispose, which will cancel the pending timer message.
    /// </summary>
    public interface IOrleansTimer : IDisposable
    {
    }
}
