using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#pragma warning disable 1998

namespace Leaderboard
{


    public class GetRequest : IHttpRequest
    {
        public GetRequest(int nr)
        {
            this.nr = nr;
        }

        private int nr;

        public string Signature
        {
            get { return string.Format("GET hello?nr={0}", nr); }
        }

        public string Body
        {
            get { return null; }
        }

        public async Task<string> ProcessRequestOnServer()
        {
            return "Hello #" + nr;
        }

        public async Task ProcessResponseOnClient(string response)
        {
            Util.Assert(response == "Hello #" + nr, "incorrect response");
        }

        public async Task ProcessErrorResponseOnClient(int statuscode, string response)
        {
            Util.Fail("Unexpected error message");
        }
    }

}