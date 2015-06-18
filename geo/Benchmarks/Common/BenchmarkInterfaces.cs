using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Net.WebSockets;
using System.Text;
using System.Threading.Tasks;

namespace GeoOrleans.Benchmarks.Common
{
   
    public interface IBenchmark : IRequestDispatcher
    {
        // the name of this benchmark
        string Name { get; }

        IEnumerable<IScenario> Scenarios { get; }

        IEnumerable<IScenario> generateScenariosFromJSON(string pJsonFile);

    }

    public interface IScenario
    {
        string Name { get;  }

        int NumRobots { get; }

        Task<string> ConductorScript(IConductorContext context);

        Task<string> RobotScript(IRobotContext context, int workernumber, string parameters);

        String RobotServiceEndpoint(int workernumber);

    }


    public interface IConductorContext
    {
        int NumRobots { get; }

        Task<string> RunRobot(int robotnumber, string parameters);

        //identifies this test instance. Can be used to name test-specific files and directories.
        string TestName { get; }

        // trace an event to the conductor console
        Task Trace(string info);
    }

 

    public interface IRobotContext
    {
       //send an http request to the service. The task finishes after the response has been processed.
        //each robot can optionally return a string encoded result back to the conductor.
        Task<string> ServiceRequest(IHttpRequest request);

        //send an socket request to the service. The task finishes after the socket close has been processed.
        Task<string> ServiceConnection(ISocketRequest request);

        //identifies this test instance. Can be used to name test-specific files and directories.
        string TestName { get; }

        // the number of this robot
        int RobotNumber { get; }

        // trace an event to the conductor console
        Task Trace(string info);
    }
 
 


}
