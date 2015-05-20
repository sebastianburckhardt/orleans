using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Providers
{
    /// <summary>
    /// Marker interface to be implemented by any app bootstrap classes that want to be loaded and auto-run during silo startup
    /// </summary>
    public interface IBootstrapProvider : IOrleansProvider
    {
    }

    /// <summary>
    /// Some constant values used by bootstrap provider loader
    /// </summary>
    internal static class BootstrapProviderConstants
    {
        public const string ConfigCategoryName = "Bootstrap";
    }
}
