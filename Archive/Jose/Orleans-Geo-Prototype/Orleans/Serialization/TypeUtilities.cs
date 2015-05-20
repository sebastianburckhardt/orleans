using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text;


namespace Orleans.Serialization
{
    internal static class TypeUtilities
    {
        internal static bool IsOrleansPrimitive(this Type t)
        {
            return t.IsPrimitive || t.IsEnum || t.Equals(typeof(string)) || t.Equals(typeof(DateTime)) || (t.IsArray && t.GetElementType().IsOrleansPrimitive());
        }

        static readonly Dictionary<RuntimeTypeHandle, bool> shallowCopyableValueTypes = new Dictionary<RuntimeTypeHandle, bool>();
        static readonly Dictionary<RuntimeTypeHandle, string> typeNameCache = new Dictionary<RuntimeTypeHandle, string>();
        static readonly Dictionary<RuntimeTypeHandle, string> typeKeyStringCache = new Dictionary<RuntimeTypeHandle, string>();
        static readonly Dictionary<RuntimeTypeHandle, byte[]> typeKeyCache = new Dictionary<RuntimeTypeHandle, byte[]>();

        static TypeUtilities()
        {
            shallowCopyableValueTypes[typeof(DateTime).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(TimeSpan).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(IPAddress).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(IPEndPoint).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(SiloAddress).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(GrainId).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(ActivationId).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(ActivationAddress).TypeHandle] = true;
            shallowCopyableValueTypes[typeof(CorrelationId).TypeHandle] = true;
        }

        internal static bool IsOrleansShallowCopyable(this Type t)
        {
            if (t.IsPrimitive || t.IsEnum || t.Equals(typeof(string)) || t.Equals(typeof(Immutable<>)))
            {
                return true;
            }

            if (t.GetCustomAttributes(typeof(ImmutableAttribute), false).Length > 0)
            {
                return true;
            }

            if (t.IsGenericType && t.GetGenericTypeDefinition().Equals(typeof(Immutable<>)))
            {
                return true;
            }

            if (t.IsValueType && !t.IsGenericType && !t.IsGenericTypeDefinition)
            {
                bool result;
                lock (shallowCopyableValueTypes)
                {
                    if (shallowCopyableValueTypes.TryGetValue(t.TypeHandle, out result))
                    {
                        return result;
                    }
                }
                result = t.GetFields().All(f => !f.FieldType.Equals(t) && f.FieldType.IsOrleansShallowCopyable());
                lock (shallowCopyableValueTypes)
                {
                    shallowCopyableValueTypes[t.TypeHandle] = result;
                }
                return result;
            }

            return false;
        }

        internal static bool IsSpecializationOf(this Type t, Type match)
        {
            return t.IsGenericType && t.GetGenericTypeDefinition().Equals(match);
        }

        /// <summary>
        /// For internal use only.
        /// Public for testing purposes.
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static string OrleansTypeName(this Type t)
        {
            string name;
            lock (typeNameCache)
            {
                if (typeNameCache.TryGetValue(t.TypeHandle, out name))
                {
                    return name;
                }
            }
            name = TypeUtils.GetTemplatedName(t);
            lock (typeNameCache)
            {
                typeNameCache[t.TypeHandle] = name;
            }
            return name;
        }

        public static byte[] OrleansTypeKey(this Type t)
        {
            byte[] key;
            lock (typeKeyCache)
            {
                if (typeKeyCache.TryGetValue(t.TypeHandle, out key))
                {
                    return key;
                }
            }
            key = Encoding.UTF8.GetBytes(t.OrleansTypeKeyString());
            lock (typeNameCache)
            {
                typeKeyCache[t.TypeHandle] = key;
            }
            return key;
        }

        public static string OrleansTypeKeyString(this Type t)
        {
            string key;
            lock (typeKeyStringCache)
            {
                if (typeKeyStringCache.TryGetValue(t.TypeHandle, out key))
                {
                    return key;
                }
            }

            var sb = new StringBuilder();
            if (t.IsGenericTypeDefinition)
            {
                sb.Append(GetBaseTypeKey(t));
                sb.Append('\'');
                sb.Append(t.GetGenericArguments().Length);
            }
            else if (t.IsGenericType)
            {
                sb.Append(GetBaseTypeKey(t));
                sb.Append('<');
                var first = true;
                foreach (var genericArgument in t.GetGenericArguments())
                {
                    if (!first)
                    {
                        sb.Append(',');
                    }
                    first = false;
                    sb.Append(OrleansTypeKeyString(genericArgument));
                }
                sb.Append('>');
            }
            else if (t.IsArray)
            {
                sb.Append(OrleansTypeKeyString(t.GetElementType()));
                sb.Append('[');
                if (t.GetArrayRank() > 1)
                {
                    sb.Append(',', t.GetArrayRank() - 1);
                }
                sb.Append(']');
            }
            else
            {
                sb.Append(GetBaseTypeKey(t));
            }

            key = sb.ToString();
            lock (typeKeyStringCache)
            {
                typeKeyStringCache[t.TypeHandle] = key;
            }

            return key;
        }

        private static string GetBaseTypeKey(Type t)
        {
            string namespacePrefix = "";
            if ((t.Namespace != null) && !t.Namespace.StartsWith("System.") && !t.Namespace.Equals("System"))
            {
                namespacePrefix = t.Namespace + '.';
            }

            if (t.IsNestedPublic)
            {
                return namespacePrefix + OrleansTypeKeyString(t.DeclaringType) + "." + t.Name;
            }

            return namespacePrefix + t.Name;
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static string GetLocationSafe(this Assembly a)
        {
            if (a.IsDynamic)
            {
                return "dynamic";
            }

            try
            {
                return a.Location;
            }
            catch (Exception)
            {
                return "unknown";
            }
        }
    }
}