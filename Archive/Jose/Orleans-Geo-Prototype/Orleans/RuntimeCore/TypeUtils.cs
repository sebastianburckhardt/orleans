using System;
using System.Collections.Generic;
using System.Linq;
using System.CodeDom;
using System.Reflection;

namespace Orleans
{
    /// <summary>
    /// A collection of utility functions for dealing with Type information.
    /// </summary>
    internal static class TypeUtils
    {
        public static string GetSimpleTypeName(Type t, Func<Type, bool> fullName=null)
        {
            if (t.IsNestedPublic)
            {
                return GetTemplatedName(t.DeclaringType) + "." + GetUntemplatedTypeName(t.Name);
            }
            if (t.IsGenericType)
            {
                return GetSimpleTypeName(fullName != null && fullName(t) ? GetFullName(t) : t.Name);
            }
            else
            {
                return fullName != null && fullName(t) ? GetFullName(t) : t.Name;
            }
        }

        public static string GetUntemplatedTypeName(string typeName)
        {
            int i = typeName.IndexOf('`');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            i = typeName.IndexOf('<');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            return typeName;            
        }

        public static string GetSimpleTypeName(string typeName)
        {
            int i = typeName.IndexOf('`');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            i = typeName.IndexOf('[');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            i = typeName.IndexOf('<');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            return typeName;
        }

        public static bool IsConcreteTemplateType(Type t)
        {
            if (t.IsGenericType)
            {
                return true;
            }
            if (t.IsArray)
            {
                return IsConcreteTemplateType(t.GetElementType());
            }
            return false;
        }

        public static string GetTemplatedName(Type t, Func<Type, bool> fullName=null)
        {
            if (t.IsGenericType)
            {
                return GetTemplatedName(GetSimpleTypeName(t, fullName), t, fullName);
            }
            else if (t.IsArray)
            {
                return GetTemplatedName(t.GetElementType(), fullName) + "[" + new string(',', t.GetArrayRank() - 1) + "]";
            }
            else
            {
                return GetSimpleTypeName(t, fullName);
            }
        }

        public static string GetTemplatedName(string baseName, Type t, Func<Type, bool> fullName)
        {
            if (t.IsGenericType)
            {
                string s = baseName;
                s += "<";
                s += GetGenericTypeArgs(t.GetGenericArguments(), fullName);
                s += ">";
                return s;
            }
            else
            {
                return baseName;
            }
        }

        public static string GetGenericTypeArgs(Type[] args, Func<Type, bool> fullName)
        {
            string s = String.Empty;

            bool first = true;
            foreach (var genericParameter in args)
            {
                if (!first)
                {
                    s += ",";
                }
                if (!genericParameter.IsGenericType)
                {
                    s += GetSimpleTypeName(genericParameter, fullName);
                }
                else
                {
                    s += GetTemplatedName(genericParameter, fullName);
                }
                first = false;
            }

            return s;
        }

        public static string GetParameterizedTemplateName(Type t, bool applyRecursively = false, Func<Type, bool> fullName = null)
        {
            if (fullName == null)
                fullName = tt => false;

            return GetParameterizedTemplateName(t, fullName, applyRecursively);
        }

        public static string GetParameterizedTemplateName(Type t, Func<Type,bool> fullName, bool applyRecursively = false)
        {
            if (t.IsGenericType)
            {
                return GetParameterizedTemplateName(GetSimpleTypeName(t, fullName), t, applyRecursively, fullName);
            }
            else
            {
                return t.Name;
            }
        }

        public static string GetParameterizedTemplateName(string baseName, Type t, bool applyRecursively = false, Func<Type, bool> fullName = null)
        {
            if (fullName == null)
                fullName = tt => false;

            if (t.IsGenericType)
            {
                string s = baseName;
                s += "<";
                bool first = true;
                foreach (var genericParameter in t.GetGenericArguments())
                {
                    if (!first)
                    {
                        s += ",";
                    }
                    if (applyRecursively && genericParameter.IsGenericType)
                    {
                        s += GetParameterizedTemplateName(genericParameter, applyRecursively);
                    }
                    else
                    {
                        s += genericParameter.FullName==null || !fullName(genericParameter) ? genericParameter.Name : genericParameter.FullName;
                    }
                    first = false;
                }
                s += ">";
                return s;
            }
            else
            {
                return baseName;
            }
        }

        public static string GetRawClassName(Type t)
        {
            string baseName = t.Namespace + "." + t.Name;
            // baseName will be of the form Namespace.TypeName`1 for generic types
            return baseName;
        }

        public static string GetRawClassName(string baseName, Type t)
        {
            if (t.IsGenericType)
            {
                return baseName + '`' + t.GetGenericArguments().Length;
            }
            else
            {
                return baseName;
            }
        }

        public static string GetRawClassName(string typeName)
        {
            int i = typeName.IndexOf('[');
            if (i > 0)
            {
                typeName = typeName.Substring(0, i);
            }
            return typeName;
        }

        public static Type[] GenericTypeArgs(string className)
        {
            List<Type> typeArgs = new List<Type>();
            string genericTypeDef = GenericTypeArgsString(className).Replace("[]", "##"); // protect array arguments
            string[] genericArgs = genericTypeDef.Split('[', ']');
            foreach (string genericArg in genericArgs)
            {
                string typeArg = genericArg.Trim('[', ']');
                if (typeArg.Length > 0 && typeArg != ",")
                    typeArgs.Add(Type.GetType(typeArg.Replace("##", "[]"))); // restore array arguments
            }
            return typeArgs.ToArray();
        }

        public static string GenericTypeArgsString(string className)
        {
            int startIndex = className.IndexOf('[');
            int endIndex = className.LastIndexOf(']');
            return className.Substring(startIndex + 1, endIndex - startIndex - 1);
        }

        public static CodeTypeParameterCollection GenericTypeParameters(Type t)
        {
            if (t.IsGenericType)
            {
                var p = new CodeTypeParameterCollection();
                foreach (var genericParameter in t.GetGenericTypeDefinition().GetGenericArguments())
                {
                    var param = new CodeTypeParameter(genericParameter.Name);
                    if ((genericParameter.GenericParameterAttributes & GenericParameterAttributes.ReferenceTypeConstraint) != GenericParameterAttributes.None)
                    {
                        param.Constraints.Add(" class");
                    }
                    if ((genericParameter.GenericParameterAttributes & GenericParameterAttributes.NotNullableValueTypeConstraint) != GenericParameterAttributes.None)
                    {
                        param.Constraints.Add(" struct");
                    }
                    var constraints = genericParameter.GetGenericParameterConstraints();
                    foreach (var constraintType in constraints)
                    {
                        param.Constraints.Add(new CodeTypeReference(TypeUtils.GetParameterizedTemplateName(constraintType)));
                    }
                    if ((genericParameter.GenericParameterAttributes & GenericParameterAttributes.DefaultConstructorConstraint) != GenericParameterAttributes.None)
                    {
                        param.HasConstructorConstraint = true;
                    }
                    p.Add(param);
                }
                return p;
            }
            else
            {
                return null;
            }
        }

        public static bool IsGenericClass(string name)
        {
            return name.Contains("`") || name.Contains("[");
        }

        public static string GetFullName(Type t)
        {
            if (t == null) throw new ArgumentNullException("t");
            return (t.FullName == null) ? t.Namespace + "." + t.Name : t.FullName;
        }

        public static string GetParameterString(CodeTypeParameterCollection genericTypeParams)
        {
            var result = "";
            if (genericTypeParams != null)
            {
                result = "";
                for (var j = 0; j < genericTypeParams.Count; j++)
                {
                    result += (j == 0 ? "<" : ",") + genericTypeParams[j].Name;
                }
                result += ">";
            }
            return result;
        }

        public static bool AssemblyContainsAttribute(Assembly assembly, Type attributeType)
        {
            try
            {
                return assembly.GetCustomAttributes(attributeType, false).Length > 0;
            }
            catch (Exception exc)
            {
                string err = string.Format("Type load error inspecting Assembly {0} for Attribute {1}", assembly.FullName, attributeType.FullName);
                if (exc.GetType() == typeof(TypeLoadException) && exc.Message.Contains("System.Runtime.CompilerServices.ExtensionAttribute"))
                {
                    err += " -- Possible cause might be loading .NET 4.5 assembly when only .NET 4.0 installed";
                }
                err += " - Skipping assembly inspection.";
                throw new OrleansException(err, exc);
            }
        }

        public static bool TryFindType(string fullName, IEnumerable<Assembly> assemblies, out Type result)
        {
            if (null == assemblies)
            {
                throw new ArgumentNullException("assemblies");
            }

            if (string.IsNullOrWhiteSpace(fullName))
            {
                throw new ArgumentException("A FullName must not be null nor consist of only whitespace.", "fullName");
            }

            foreach (var assembly in assemblies)
            {
                result = assembly.GetType(fullName, false);
                if (result != null)
                {
                    return true;
                }
            }

            result = null;
            return false;
        }

        public static bool TryFindType(string fullName, out Type result)
        {
            return TryFindType(fullName, AppDomain.CurrentDomain.GetAssemblies(), out result);
        }

        public static Type FindType(string fullName, IEnumerable<Assembly> assemblies)
        {
            Type result;
            if (TryFindType(fullName, assemblies, out result))
            {
                return result;
            }
            else
            {
                throw new KeyNotFoundException(string.Format("Unable to find type named {0}", fullName));
            }
        }
        public static Type FindType(string fullName)
        {
            return FindType(fullName, AppDomain.CurrentDomain.GetAssemblies());
        }
    }
}
