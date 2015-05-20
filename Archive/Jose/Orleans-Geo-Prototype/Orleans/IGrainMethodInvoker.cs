using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;


namespace Orleans
{
    /// <summary>
    /// This interface is for internal use only.
    /// An implementation of this interface is generated for every grain interface as part of the client-side code generation.
    /// </summary>
    public interface IGrainMethodInvoker
    {
        /// <summary> The interface id that this invoker supports. </summary>
        int InterfaceId { get; }

        /// <summary>
        /// This method is for internal use only.
        /// Invoke a grain method.
        /// Invoker classes in generated code implement this method to provide a method call jump-table to map invoke data to a strongly typed call to the correct method on the correct interface.
        /// </summary>
        /// <param name="grain">Reference to the grain to be invoked.</param>
        /// <param name="interfaceId">Interface id of the method to be called.</param>
        /// <param name="methodId">Method id of the method to be called.</param>
        /// <param name="arguments">Arguments to be passed to the method being invoked.</param>
        /// <returns>Value promise for the result of the method invoke.</returns>
        Task<object> Invoke(IAddressable grain, int interfaceId, int methodId, object[] arguments);
    }

    /// <summary>
    /// This interface is for internal use only.
    /// An implementation of this interface is generated for every grain extension as part of the client-side code generation.
    /// </summary>
    public interface IGrainExtensionMethodInvoker : IGrainMethodInvoker
    {
        /// <summary>
        /// This method is for internal use only.
        /// Invoke a grain extension method.
        /// </summary>
        /// <param name="extension">Reference to the extension to be invoked.</param>
        /// <param name="interfaceId">Interface id of the method to be called.</param>
        /// <param name="methodId">Method id of the method to be called.</param>
        /// <param name="arguments">Arguments to be passed to the method being invoked.</param>
        /// <returns>Value promise for the result of the method invoke.</returns>
        Task<object> Invoke(IGrainExtension extension, int interfaceId, int methodId, object[] arguments);
    }
}
