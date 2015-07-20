using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans.Runtime;

namespace Orleans.GrainDirectory
{
    interface IGrainRegistrar
    {
        /// <summary>
        /// Registers a new activation with the directory service.
        /// <para>This method must be called from a scheduler thread.</para>
        /// </summary>
        /// <param name="address">The address of the activation to register.</param>
        /// <returns>The address registered for the grain's single activation.</returns>
        Task<ActivationAddress> RegisterAsync(ActivationAddress address);        
    }
}
