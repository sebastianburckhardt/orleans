using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PileusApp.YCSB
{

    public enum YCSBOperationType
    {
        READ,
        UPDATE
    }

    public class YCSBOperation
    {
        public YCSBOperation(string keyName, YCSBOperationType type)
        {
            this.KeyName = keyName;
            this.Type = type;
        }

        public string KeyName { get; set; }
        public YCSBOperationType Type { get; set; }
    }
}
