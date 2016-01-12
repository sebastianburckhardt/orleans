using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime.LogViews
{
    [Serializable]
    public class TaggedEntry
    {

        public object Entry { get; set; }

        public Guid Guid { get; set; }

    }
}
