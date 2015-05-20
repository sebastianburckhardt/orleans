using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.Runtime
{
    /// <summary>
    /// Type Code Mapping functions.
    /// </summary>
    internal static class TypeCodeMapper
    {
        internal static int GetImplementationTypeCode(int interfaceId, string grainClassNamePrefix = null)
        {
            int typeCode;
            IGrainTypeResolver grainTypeResolver = GrainClient.InternalCurrent.GrainTypeResolver;
            if (!grainTypeResolver.TryGetGrainTypeCode(interfaceId, out typeCode, grainClassNamePrefix))
            {
                var loadedAssemblies = grainTypeResolver.GetLoadedGrainAssemblies();
                throw new ArgumentException(
                    String.Format("Cannot find a type code for an implementation class for grain interface: {0}{2}. Make sure the grain assembly was correctly deployed and loaded in the silo.{1}",
                                  interfaceId,
                                  String.IsNullOrEmpty(loadedAssemblies) ? String.Empty : String.Format(" Loaded grain assemblies: {0}", loadedAssemblies),
                                  String.IsNullOrEmpty(grainClassNamePrefix) ? String.Empty : ", grainClassNamePrefix=" + grainClassNamePrefix));
            }
            return typeCode;
        }

        internal static int GetImplementationTypeCode(string grainImplementationClassName)
        {
            int typeCode;
            IGrainTypeResolver grainTypeResolver = GrainClient.InternalCurrent.GrainTypeResolver;
            if (!grainTypeResolver.TryGetGrainTypeCode(grainImplementationClassName, out typeCode))
                throw new ArgumentException(String.Format("Cannot find a type code for an implementation grain class: {0}. Make sure the grain assembly was correctly deployed and loaded in the silo.", grainImplementationClassName));

            return typeCode;
        }

        internal static GrainId ComposeGrainId(int baseTypeCode, Guid primaryKey, Type interfaceType, string keyExt = null)
        {
            return GrainId.GetGrainId(ComposeGenericTypeCode(interfaceType, baseTypeCode),
                primaryKey, keyExt);
        }

        internal static GrainId ComposeGrainId(int baseTypeCode, long primaryKey, Type interfaceType, string keyExt = null)
        {
            return GrainId.GetGrainId(ComposeGenericTypeCode(interfaceType, baseTypeCode),
                primaryKey, keyExt);
        }

        internal static long ComposeGenericTypeCode(Type interfaceType, int baseTypeCode)
        {
            if (!interfaceType.IsGenericType)
                return baseTypeCode;

            string args = TypeUtils.GetGenericTypeArgs(interfaceType.GetGenericArguments(), t => true);
            int hash = Utils.CalculateIdHash(args);
            return (((long)(hash & 0x00FFFFFF)) << 32) + baseTypeCode;
        }
    }
}
