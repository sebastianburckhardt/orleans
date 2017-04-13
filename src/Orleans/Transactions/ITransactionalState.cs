
using Orleans.Facet;

namespace Orleans.Transactions
{
    /// <summary>
    /// Stateful facet that respects Orleans transaction semantics
    /// </summary>
    public interface ITransactionalState<out TState> : IGrainFacet
        where TState : class, new()
    {
        TState State { get; }
        void Save();
    }
}
