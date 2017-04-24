using System.Threading.Tasks;
using Orleans;

namespace UnitTests.GrainInterfaces
{
    public interface ITransactionTestGrain : IGrainWithGuidKey
    {
        [Transaction(TransactionOption.Required)]
        Task Set(int newValue);

        [Transaction(TransactionOption.Required)]
        Task<int> Add(int numberToAdd);

        [Transaction(TransactionOption.Required)]
        Task<int> Get();

        [Transaction(TransactionOption.Required)]
        Task<int> AddAndThrow(int numberToAdd);
    }
}
