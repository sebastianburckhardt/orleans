using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Orleans
{
    /// <summary>
    /// For internal (run-time) use only.
    /// Base class of all the activation attributes 
    /// </summary>
    [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Performance", "CA1813:AvoidUnsealedAttributes"), AttributeUsage(System.AttributeTargets.All)]
    public abstract class GeneratedAttribute : Attribute
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// Type for which this activation is implemented
        /// </summary>
        public string ForGrainType { get; protected set; }

        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        /// <param name="forGrainType">type argument</param>
        protected GeneratedAttribute(string forGrainType)
        {
            ForGrainType = forGrainType;
        }
        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        protected GeneratedAttribute() { }
    }
    
    /// <summary>
    /// For internal (run-time) use only.
    /// </summary>
    [AttributeUsage(System.AttributeTargets.Class)]
    public sealed class GrainStateAttribute : GeneratedAttribute
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        /// <param name="forGrainType">type argument</param>
        public GrainStateAttribute(string forGrainType)
        {
            ForGrainType = forGrainType;
        }
    }
    /// <summary>
    /// For internal (run-time) use only.
    /// </summary>
    [AttributeUsage(System.AttributeTargets.Class)]
    public sealed class MethodInvokerAttribute : GeneratedAttribute
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        /// <param name="forGrainType">type argument</param>
        public MethodInvokerAttribute(string forGrainType, int interfaceId)
        {
            ForGrainType = forGrainType;
            InterfaceId = interfaceId;
        }

        public int InterfaceId { get; private set; }
    }
    /// <summary>
    /// For internal (run-time) use only.
    /// </summary>
    [AttributeUsage(System.AttributeTargets.Class)]
    public sealed class GrainReferenceAttribute : GeneratedAttribute
    {
        /// <summary>
        /// For internal (run-time) use only.
        /// </summary>
        /// <param name="forGrainType">type argument</param>
        public GrainReferenceAttribute(string forGrainType)
        {
            ForGrainType = forGrainType;
        }
    }
}
