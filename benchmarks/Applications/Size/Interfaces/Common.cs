using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Size.Interfaces
{




    /// <summary>
    /// Request types:
    /// 1) Read
    /// 2) Write
    /// </summary>
    public enum SizeRequestT
    {
        READ_SYNC,
        WRITE_SYNC,
        READ_ASYNC,
        WRITE_ASYNC
    }



}
