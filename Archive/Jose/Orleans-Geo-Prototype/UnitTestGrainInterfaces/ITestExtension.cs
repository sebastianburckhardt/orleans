using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrainInterfaces
{
    public interface ITestExtension : IGrain, IGrainExtension
    {
        Task<string> CheckExtension_1();

        Task<string> CheckExtension_2();
    }

    public interface IGenericTestExtension<T> : IGrain, IGrainExtension
    {
        Task<T> CheckExtension_1();

        Task<string> CheckExtension_2();
    }
}