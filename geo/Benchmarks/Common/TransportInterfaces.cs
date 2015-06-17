using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks.Common
{
  
    public interface IRequestDispatcher
    {
        // factory methods for creating request objects that can handle a given request on a server
        IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body = null);
    }



    public interface IRequest
    {
        // determinest the type of this request, using custom format: (GET|PUT|POST|WS) followed by whitespace, then
        // the last part of the URL (without the protocol or hostname)
        //
        // Examples:
        // POST hello?nr=2
        // GET hello/accounts/sburckha/overview?access_token=29f287h93847y987t98
        // WS hello/chat
        //
        string Signature { get; }

    }

    public interface IHttpRequest : IRequest
    {
        string Body { get; }

        Task<string> ProcessRequestOnServer();

        //Changed PorcessResponseOnClient to return string encoded "result" back to the conductor.
        Task<string> ProcessResponseOnClient(string response);
    }

    public interface ISocket
    {
        Task Send(string message);

        Task Close(string message);

    }

    public interface ISocketRequest : IRequest
    {
        Task ProcessConnectionOnServer(ISocket socket);

        Task ProcessMessageOnServer(ISocket socket, string message);

        Task ProcessCloseOnServer(ISocket socket, string message);

        Task<string> ProcessConnectionOnClient(ISocket socket);

        Task<string> ProcessMessageOnClient(ISocket socket, string message);

        Task<string> ProcessCloseOnClient(ISocket socket, string message);

    }
 
}
