﻿using System;
﻿using System.Collections;
﻿using System.Collections.Generic;
using System.Globalization;
﻿using System.Linq;
﻿using System.Reflection;
﻿using System.Security.Cryptography;
﻿using System.Text;

namespace Orleans
{
    /// <summary>
    /// General purpose utility functions.
    /// </summary>
    public static class OrleansUtils
    {
        /// <summary>
        /// Returns a human-readable text string that describes an IEnumerable collection of objects.
        /// </summary>
        /// <typeparam name="T">The type of the list elements.</typeparam>
        /// <param name="collection">The IEnumerable to describe.</param>
        /// <returns>A string assembled by wrapping the string descriptions of the individual
        /// elements with square brackets and separating them with commas.</returns>
        public static string IEnumerableToString<T>(IEnumerable<T> collection, Func<T, string> toString = null,
                                                        string separator = ", ", bool putInBrackets = true)
        {
            return Utils.IEnumerableToString(collection, toString, separator, putInBrackets);
        }

        /// <summary>
        /// Returns a short string representing Guid.
        /// </summary>
        /// <param name="guid">The Guid to print.</param>
        /// <returns>A string represting the first eight characters of guid.ToString().</returns>
        public static string ToShortString(this Guid guid)
        {
            return guid.ToString().Substring(0, 8);
        }

    }


    /// <summary>
    /// The Utils class contains a variety of utility methods for use in application and grain code.
    /// </summary>
    internal static class Utils
    {
        /// <summary>
        /// Returns a human-readable text string that describes an IEnumerable collection of objects.
        /// </summary>
        /// <typeparam name="T">The type of the list elements.</typeparam>
        /// <param name="collection">The IEnumerable to describe.</param>
        /// <returns>A string assembled by wrapping the string descriptions of the individual
        /// elements with square brackets and separating them with commas.</returns>
        public static string IEnumerableToString<T>(IEnumerable<T> collection, Func<T, string> toString = null, 
                                                        string separator = ", ", bool putInBrackets = true)
        {
            if (collection == null)
            {
                if (putInBrackets) return "[]";
                else return "null";
            }
            StringBuilder str = new StringBuilder();
            if (putInBrackets) str.Append("[");
            IEnumerator<T> enumerator = collection.GetEnumerator();
            bool firstDone = false;
            while (enumerator.MoveNext())
            {
                T value = enumerator.Current;
                string val;
                if (toString != null)
                    val = toString(value);
                else
                    val = value == null ? "null" : value.ToString();

                if (firstDone)
                {
                    str.Append(separator);
                    str.Append(val);
                }
                else
                {
                    str.Append(val);
                    firstDone = true;
                }
            }
            if (putInBrackets) str.Append("]");
            return str.ToString();
        }

        /// <summary>
        /// Returns a human-readable text string that describes a dictionary that maps objects to objects.
        /// </summary>
        /// <typeparam name="T1">The type of the dictionary keys.</typeparam>
        /// <typeparam name="T2">The type of the dictionary elements.</typeparam>
        /// <param name="separateWithNewLine">Whether the elements should appear separated by a new line.</param>
        /// <param name="dict">The dictionary to describe.</param>
        /// <returns>A string assembled by wrapping the string descriptions of the individual
        /// pairs with square brackets and separating them with commas.
        /// Each key-value pair is represented as the string description of the key followed by
        /// the string description of the value,
        /// separated by " -> ", and enclosed in curly brackets.</returns>
        public static string DictionaryToString<T1, T2>(ICollection<KeyValuePair<T1, T2>> dict, string separator = "\n")
        {
            if (dict == null || dict.Count == 0)
            {
                return "[]";
            }
            StringBuilder str = new StringBuilder("[");
            IEnumerator<KeyValuePair<T1, T2>> enumerator = dict.GetEnumerator();
            int index = 0;
            while (enumerator.MoveNext())
            {
                KeyValuePair<T1, T2> pair = enumerator.Current;
                str.Append("{");
                str.Append(pair.Key);
                str.Append(" -> ");
                str.Append(pair.Value);
                str.Append("}");
                //str += "{" + pair.Key + " -> " + pair.Value + "}";
                if (index++ < dict.Count - 1)
                    str.Append(separator);
            }
            str.Append("]");
            return str.ToString();
        }

        /// <summary>
        /// Returns a human-readable text string that describes a dictionary that maps objects to lists of objects.
        /// </summary>
        /// <typeparam name="T1">The type of the dictionary keys.</typeparam>
        /// <typeparam name="T2">The type of the list elements.</typeparam>
        /// <param name="dict">The dictionary to describe.</param>
        /// <returns>A string assembled by wrapping the string descriptions of the individual
        /// pairs with square brackets and separating them with commas.
        /// Each key-value pair is represented as the string descripotion of the key followed by
        /// the string description of the value list (created using <see cref="IEnumerableToString"/>),
        /// separated by " -> ", and enclosed in curly brackets.</returns>
        public static string DictionaryOfListsToString<T1, T2>(Dictionary<T1, List<T2>> dict)
        {
            if (dict == null || dict.Count == 0)
            {
                return "[]";
            }
            StringBuilder str = new StringBuilder("[");
            Dictionary<T1, List<T2>>.Enumerator enumerator = dict.GetEnumerator();
            while (enumerator.MoveNext())
            {
                KeyValuePair<T1, List<T2>> pair = enumerator.Current;
                str.Append("{");
                str.Append(pair.Key);
                str.Append(" -> ");
                str.Append(Utils.IEnumerableToString(pair.Value));
                str.Append("}\n");
                //str += "{" + pair.Key + " -> " + Utils.IEnumerableToString(pair.Value) + "}" + "\n";
            }
            str.Append("]");
            return str.ToString();
        }

        public static string TimeSpanToString(TimeSpan timeSpan)
        {
            //00:03:32.8289777
            return String.Format("{0}h:{1}m:{2}s.{3}ms", timeSpan.Hours, timeSpan.Minutes, timeSpan.Seconds, timeSpan.Milliseconds);
        }

        /// <summary>
        /// Calculates an integer hash value based on the consistent identity hash of a string.
        /// </summary>
        /// <param name="text">The string to hash.</param>
        /// <returns>An integer hash for the string.</returns>
        public static int CalculateIdHash(string text)
        {
            SHA256 sha = new SHA256CryptoServiceProvider(); // This is one implementation of the abstract class SHA1.
            int hash = 0;
            try
            {
                byte[] data = Encoding.Unicode.GetBytes(text);
                byte[] result = sha.ComputeHash(data);
                //Debug.Assert((result.Length % 4) == 0); // SHA1 is 160 bits
                for (int i = 0; i < result.Length; i += 4)
                {
                    int tmp = (result[i] << 24) | (result[i + 1] << 16) | (result[i + 2] << 8) | (result[i + 3]);
                    hash = hash ^ tmp;
                }
                //string hash = BitConverter.ToString(cryptoTransformSHA1.ComputeHash(buffer)).Replace("-", "");
            }
            finally
            {
                sha.Dispose();
            }
            return hash;
        }

        public static bool TryFindException(Exception original, Type targetType, out Exception target)
        {
            if (original.GetType().Equals(targetType))
            {
                target = original;
                return true;
            }
            else if (original is AggregateException)
            {
                var baseEx = original.GetBaseException();
                if (baseEx.GetType().Equals(targetType))
                {
                    target = baseEx;
                    return true;
                }
                else
                {
                    var newEx = ((AggregateException)original).Flatten();
                    foreach (var exc in newEx.InnerExceptions)
                    {
                        if (exc.GetType().Equals(targetType))
                        {
                            target = newEx;
                            return true;
                        }
                    }
                }
            }
            target = null;
            return false;
        }

        /// <summary>
        /// This method is for internal use only.
        /// </summary>
        /// <param name="types"></param>
        /// <returns></returns>
        public static IEnumerable<Type> LeafTypes(this IEnumerable<Type> types)
        {
            Type[] allTypes = types.ToArray();
            return allTypes.Where(i => !allTypes.Any(i2 => i != i2 && i.IsAssignableFrom(i2)));
        }

        public static byte[] ParseHexBytes(this string s)
        {
            var result = new byte[s.Length / 2];
            for (int i = 0; i < s.Length - 1; i += 2) // allow for \r at end of line
            {
                result[i / 2] = (byte)Int32.Parse(s.Substring(i, 2), NumberStyles.HexNumber);
            }
            return result;
        }

        public static string ToHexString(this byte[] bytes)
        {
            var sb = new StringBuilder();
            foreach (var b in bytes)
            {
                sb.Append(b.ToString("x2"));
            }
            return sb.ToString();
        }

        public static void SafeExecute(Action action, Logger logger = null, string caller = null)
        {
            SafeExecute(action, logger, caller==null ? (Func<string>)null : () => caller);
        }

        // a function to safely execute an action without any exception being thrown.
        // callerGetter function is called only in faulty case (now string is generated in the success case).
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static void SafeExecute(Action action, Logger logger, Func<string> callerGetter)
        {
            try
            {
                action();
            }
            catch (Exception exc)
            {
                try
                {
                    if (logger != null)
                    {
                        string caller = null;
                        if (callerGetter != null)
                        {
                            caller = callerGetter();
                        }
                        logger.Warn(ErrorCode.Runtime_Error_100325, String.Format("Ignoring {0} exception thrown from an action called by {1}.", exc.GetType().FullName, caller==null ? "" : caller), exc);
                    }
                }
                catch (Exception)
                {
                    // now really, really ignore.
                }
            }
        }

        /// <summary>
        /// Get the last characters of a string
        /// </summary>
        /// <param name="s"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static string Tail(this string s, int count)
        {
            return s.Substring(Math.Max(0, s.Length - count));
        }

        public static TimeSpan Since(DateTime start)
        {
            return DateTime.UtcNow.Subtract(start);
        }

        public static List<T> ObjectToList<T>(object data)
        {
            if (data is List<T>) return (List<T>) data;

            T[] dataArray;
            if (data is ArrayList)
            {
                dataArray = (T[]) (data as ArrayList).ToArray(typeof(T));
            }
            else if (data is ICollection<T>)
            {
                dataArray = (data as ICollection<T>).ToArray();
            }
            else
            {
                throw new InvalidCastException(string.Format(
                    "Connet convert type {0} to type List<{1}>", 
                    TypeUtils.GetFullName(data.GetType()),
                    TypeUtils.GetFullName(typeof(T))));
            }
            List<T> list = new List<T>();
            list.AddRange(dataArray);
            return list;
        }

        public static AggregateException Flatten(this ReflectionTypeLoadException rtle)
        {
            // if ReflectionTypeLoadException is thrown, we need to provide the
            // LoaderExceptions property in order to make it meaningful.
            var all = new List<Exception> { rtle };
            all.AddRange(rtle.LoaderExceptions);
            throw new AggregateException("A ReflectionTypeLoadException has been thrown. The original exception and the contents of the LoaderExceptions property have been aggregated for your convenence.", all);
        }
    }
}
