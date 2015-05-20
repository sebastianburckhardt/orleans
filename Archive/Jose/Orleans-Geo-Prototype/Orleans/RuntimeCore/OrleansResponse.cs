using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Orleans.Serialization;

namespace Orleans
{
    [Serializable]
    internal class OrleansResponse
    {
        public bool ExceptionFlag { get; private set; }
        public Exception Exception { get; private set; }
        public object Data { get; private set; }

        public OrleansResponse(object data)
        {
            this.Exception = data as Exception;
            if (this.Exception == null)
            {
                this.Data = data;
                this.ExceptionFlag = false;
            }
            else
            {
                this.Data = null;
                this.ExceptionFlag = true;
            }
        }

        static public OrleansResponse ExceptionResponse(Exception exc)
        {
            OrleansResponse resp = new OrleansResponse(null);
            resp.ExceptionFlag = true;
            resp.Exception = exc;
            resp.Data = null;
            return resp;
        }

        public override string ToString()
        {
            return String.Format("OrleansResponse ExceptionFlag={0}", ExceptionFlag);
        }

        private static OrleansResponse done = new OrleansResponse(null);
        public static OrleansResponse Done { get { return done; } }
    }
}
