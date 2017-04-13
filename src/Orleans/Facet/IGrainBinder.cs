

using System.Threading.Tasks;

namespace Orleans.Facet
{
    /// <summary>
    /// Marker interface for facet, indicating that it must be bound to the grain.
    /// </summary>
    public interface IGrainBinder
    {
        Task BindAsync(Grain grain);
    }
}
