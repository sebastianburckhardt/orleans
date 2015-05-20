using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace UnitTestGrainInterfaces
{
    public interface ITestDeploymentGrain: ISiloGrain // this interface exposes some test specific methods
    {
        Task<bool> LoadDeployment(int d, int n);
        Task<bool> SetListOfDeployments(List<IDeploymentGrain> deployments);
    }
}
