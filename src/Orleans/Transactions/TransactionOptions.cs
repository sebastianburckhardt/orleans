using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Transactions
{
    public class TransactionOptions
    {

        public bool ReadOnly;

        public bool MustBeOutermostScope;

    }
}
