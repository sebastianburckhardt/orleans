using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace Orleans.Management.Deployment
{
    public class TargetLocation
    {
        public string Host { get; set; }
        public string Path { get; set; }

        public TargetLocation(string host, string path)
        {
            this.Host = host;
            this.Path = path;
        }

        public string GetUncPath()
        {
            Debug.Assert(this.Host != null);
            Debug.Assert(this.Path != null);

            return @"\\" + this.Host + @"\" + this.Path.Replace(':', '$') + @"\";
        }

        public TargetLocation AddToPath(string dirName)
        {
            if (string.IsNullOrWhiteSpace(dirName))
            {
                return this;
            }
            else
            {
                return new TargetLocation(this.Host, this.Path + @"\" + dirName);
            }
        }
    }
}
