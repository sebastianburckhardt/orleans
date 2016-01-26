using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.EventSourcing
{
    /// <summary>
    /// Grains can implement this interface to override the default mechanism for determining stream names.
    /// </summary>
    public interface ICustomStreamName
    {
        string GetStreamName();
    }
}
