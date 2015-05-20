using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;

namespace Common
{
   

    public interface IBenchmark : IRequestDispatcher
    {
        // the name of this benchmark
        string Name { get; }

        IEnumerable<IScenario> Scenarios { get; }

        // factory methods for creating request objects on the server
        IRequest ParseRequest(string verb, IEnumerable<string> urlpath, NameValueCollection arguments, string body = null);
    }

    public interface IScenario
    {
        string Name { get;  }

        int NumRobots { get; }

        Task<string> ConductorScript(IConductorContext context);

        Task<string> RobotScript(IRobotContext context, int workernumber, string parameters);
    }


    public interface IConductorContext
    {
        int NumRobots { get; }

        Task<string> RunRobot(int robotnumber, string parameters);

    }

 

    public interface IRobotContext
    {
       //send an http request to the service. The task finishes after the response has been processed.
        Task ServiceRequest(IHttpRequest request);

        //send an socket request to the service. The task finishes after the socket close has been processed.
        Task ServiceConnection(ISocketRequest request);

    }
 
 


}
