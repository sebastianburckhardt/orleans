// -----------------------------------------------------------------------
// <copyright file="SecUtil.cs" company="Microsoft">
//    Copyright (c) Microsoft. All rights reserved.
//    This code is licensed under the Microsoft Public License.
//    THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF
//    ANY KIND, EITHER EXPRESS OR IMPLIED, INCLUDING ANY
//    IMPLIED WARRANTIES OF FITNESS FOR A PARTICULAR
//    PURPOSE, MERCHANTABILITY, OR NON-INFRINGEMENT.
// </copyright>
// -----------------------------------------------------------------------

namespace Common
{
    using System;
    using System.Collections;
    using System.Diagnostics;
    using System.Globalization;
    using System.Net;
    using System.Security;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using Microsoft.WindowsAzure;
    using Microsoft.WindowsAzure.Storage.Auth;

    internal static class SecUtility
    {
        internal const int Infinite = Int32.MaxValue;
        internal static readonly string ValidTableNameRegex = @"^([a-zA-Z])([a-zA-Z]|\d){2,62}$";
        internal static readonly string ValidContainerNameRegex = @"^([a-z]|\d)([a-z]|\d|-(?!\-)){1,61}([a-z]|\d)$";

        internal static bool ValidateParameter(ref string param, bool checkForNull, bool checkIfEmpty, bool checkForCommas, int maxSize)
        {
            if (param == null)
            {
                return !checkForNull;
            }

            param = param.Trim();
            if ((checkIfEmpty && param.Length < 1) ||
                 (maxSize > 0 && param.Length > maxSize) ||
                 (checkForCommas && param.Contains(",")))
            {
                return false;
            }

            return true;
        }

        internal static void CheckParameter(ref string param, bool checkForNull, bool checkIfEmpty, bool checkForCommas, int maxSize, string paramName)
        {
            if (param == null)
            {
                if (checkForNull)
                {
                    throw new ArgumentNullException(paramName);
                }

                return;
            }

            param = param.Trim();
            if (checkIfEmpty && param.Length < 1)
            {
                throw new ArgumentException(string.Format(CultureInfo.InstalledUICulture, "The parameter '{0}' must not be empty.", paramName), paramName);
            }

            if (maxSize > 0 && param.Length > maxSize)
            {
                throw new ArgumentException(string.Format(CultureInfo.InstalledUICulture, "The parameter '{0}' is too long: it must not exceed {1} chars in length.", paramName, maxSize.ToString(CultureInfo.InvariantCulture)), paramName);
            }

            if (checkForCommas && param.Contains(","))
            {
                throw new ArgumentException(string.Format(CultureInfo.InstalledUICulture, "The parameter '{0}' must not contain commas.", paramName), paramName);
            }
        }

        internal static void CheckArrayParameter(ref string[] param, bool checkForNull, bool checkIfEmpty, bool checkForCommas, int maxSize, string paramName)
        {
            if (param == null)
            {
                throw new ArgumentNullException(paramName);
            }

            if (param.Length < 1)
            {
                throw new ArgumentException(string.Format(CultureInfo.InstalledUICulture, "The array parameter '{0}' should not be empty.", paramName), paramName);
            }

            Hashtable values = new Hashtable(param.Length);
            for (int i = param.Length - 1; i >= 0; i--)
            {
                SecUtility.CheckParameter(ref param[i], checkForNull, checkIfEmpty, checkForCommas, maxSize,
                    paramName + "[ " + i.ToString(CultureInfo.InvariantCulture) + " ]");
                if (values.Contains(param[i]))
                {
                    throw new ArgumentException(string.Format(CultureInfo.InstalledUICulture, "The array '{0}' should not contain duplicate values.", paramName), paramName);
                }
                else
                {
                    values.Add(param[i], param[i]);
                }
            }
        }

        /*
        internal static void SetUtcTime(DateTime value, out DateTime res)
        {
            res = Configuration.MinSupportedDateTime;
            if ((value.Kind == DateTimeKind.Local && value.ToUniversalTime() < Configuration.MinSupportedDateTime) ||
                 value < Configuration.MinSupportedDateTime)
            {
                throw new ArgumentException("Invalid time value!");
            }
            if (value.Kind == DateTimeKind.Local)
            {
                res = value.ToUniversalTime();
            }
            else
            {
                res = value;
            }
        }
        */

        internal static bool IsValidTableName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return false;
            }
            Regex reg = new Regex(ValidTableNameRegex);
            if (reg.IsMatch(name))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        internal static bool IsValidContainerName(string name)
        {
            if (string.IsNullOrEmpty(name))
            {
                return false;
            }
            Regex reg = new Regex(ValidContainerNameRegex);
            if (reg.IsMatch(name))
            {
                return true;
            }
            else
            {
                return false;
            }
        }

        // the table storage system currently does not support the StartsWith() operation in 
        // queries. As a result we transform s.StartsWith(substring) into s.CompareTo(substring) > 0 && 
        // s.CompareTo(NextComparisonString(substring)) < 0
        // we assume that comparison on the service side is as ordinal comparison
        internal static string NextComparisonString(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new ArgumentException("The string argument must not be null or empty!");
            }
            string ret;
            char last = s[s.Length - 1];
            if ((int)last + 1 > (int)char.MaxValue)
            {
                throw new ArgumentException("Cannot convert the string.");
            }
            // don't use "as" because we want to have an explicit exception here if something goes wrong
            last = (char)((int)last + 1);
            ret = s.Substring(0, s.Length - 1) + last;
            return ret;
        }

        // we use a normal character as the separator because of string comparison operations
        // these have to be valid characters
        //internal const char KeySeparator = 'a';
        internal const char EscapeCharacter = '^';
        internal static readonly string EscapeCharacterString = new string(EscapeCharacter, 1);

        // Some characters can cause problems when they are contained in columns 
        // that are included in queries. We are very defensive here and escape a wide range 
        // of characters for key columns (as the key columns are present in most queries)
        internal static bool IsInvalidKeyCharacter(char c)
        {
            return ((c < 32)
                || (c >= 127 && c < 160)
                || (c == '#')
                || (c == '&')
                || (c == '+')
                || (c == '/')
                || (c == '?')
                || (c == ':')
                || (c == '%')
                || (c == '\\'));
        }

        internal static string CharToEscapeSequence(char c)
        {
            string ret;
            ret = EscapeCharacterString + string.Format(CultureInfo.InvariantCulture, "{0:X2}", (int)c);
            return ret;
        }

        internal static string Escape(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return s;
            }
            StringBuilder ret = new StringBuilder();
            foreach (char c in s)
            {
                if (c == EscapeCharacter || IsInvalidKeyCharacter(c))
                {
                    ret.Append(CharToEscapeSequence(c));
                }
                else
                {
                    ret.Append(c);
                }
            }
            return ret.ToString();
        }

        internal static string UnEscape(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                return s;
            }
            StringBuilder ret = new StringBuilder();
            char c;
            for (int i = 0; i < s.Length; i++)
            {
                c = s[i];
                if (c == EscapeCharacter)
                {
                    if (i + 2 >= s.Length)
                    {
                        throw new FormatException("The string " + s + " is not correctly escaped!");
                    }
                    int ascii = Convert.ToInt32(s.Substring(i + 1, 2), 16);
                    ret.Append((char)ascii);
                    i += 2;
                }
                else
                {
                    ret.Append(c);
                }
            }
            return ret.ToString();
        }



        internal static void CheckAllowInsecureEndpoints(bool allowInsecureRemoteEndpoints, StorageCredentials info, Uri baseUri)
        {
            if (info == null)
            {
                throw new ArgumentNullException("info");
            }
            if (allowInsecureRemoteEndpoints)
            {
                return;
            }
            if (baseUri == null || string.IsNullOrEmpty(baseUri.Scheme))
            {
                throw new SecurityException("allowInsecureRemoteEndpoints is set to false (default setting) but the endpoint URL seems to be empty or there is no URL scheme." +
                                            "Please configure the provider to use an https endpoint for the storage endpoint or " +
                                            "explicitly set the configuration option allowInsecureRemoteEndpoints to true.");
            }
            if (baseUri.Scheme.ToUpper(CultureInfo.InvariantCulture) == Uri.UriSchemeHttps.ToUpper(CultureInfo.InvariantCulture))
            {
                return;
            }
            if (baseUri.IsLoopback)
            {
                return;
            }
            throw new SecurityException("The provider is configured with allowInsecureRemoteEndpoints set to false (default setting) but the endpoint for " +
                                        "the storage system does not seem to be an https or local endpoint. " +
                                        "Please configure the provider to use an https endpoint for the storage endpoint or " +
                                        "explicitly set the configuration option allowInsecureRemoteEndpoints to true.");
        }
    }

    internal static class Constants
    {
        internal const int MaxTableUsernameLength = 256;
        internal const int MaxTableApplicationNameLength = 256;
    }
}