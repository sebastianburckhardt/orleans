#define USE_MD5
using System;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;

namespace Orleans
{
    [Serializable]
    internal abstract class UniqueIdentifier : IEquatable<UniqueIdentifier>, IComparable<UniqueIdentifier>
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Security", "CA2104:DoNotDeclareReadOnlyMutableReferenceTypes")]
        protected readonly internal UniqueKey Key;

        protected UniqueIdentifier()
        { }

        protected UniqueIdentifier(UniqueKey key)
        {
            Key = key;
        }

        public override string ToString()
        {
            return Key.ToString();
        }

        internal byte[] ToByteArray()
        {
            return Key.ToByteArray();
        }

        public override bool Equals(object obj)
        {
            var other = obj as UniqueIdentifier;
            return other != null && GetType() == other.GetType() && Key.Equals(other.Key);
        }

        public override int GetHashCode()
        {
            return Key.GetHashCode();
        }

        public uint GetHashCode_Modulo(uint umod)
        {
            int key = Key.GetHashCode();
            int mod = (int)umod;
            key = ((key % mod) + mod) % mod; // key should be positive now. So assert with checked.
            return checked((uint)key);
        }

        #region IEquatable<UniqueIdentifier> Members

        public virtual bool Equals(UniqueIdentifier other)
        {
            return other != null && GetType() == other.GetType() && Key.Equals(other.Key);
        }

        #endregion

        #region IComparable<UniqueIdentifier> Members

        public int CompareTo(UniqueIdentifier other)
        {
            return Key.CompareTo(other.Key);
        }

        #endregion
    }

    [Serializable]
    internal class UniqueKey : IComparable<UniqueKey>, IEquatable<UniqueKey>
    {
        private const ulong TypeCodeDataMask = 0xFFFFFFFF; // Lowest 4 bytes
        internal const int InternalKeySize = 16; // 16 bytes == 2 * Marshal.SizeOf(typeof(ulong))
        internal const int KeySizeBytes = InternalKeySize + 8; // + type code data (ulong) = 24 bytes

        /// <summary>
        /// Type id values encoded into UniqueKeys
        /// </summary>
        public enum Category : byte
        {
            None = 0,
            SystemTarget = 1,
            SystemGrain = 2,
            Grain = 3,
            ClientGrain = 4,
            Task = 5, // TODO: Refactor TaskId to avoid overloading usage of UniqueKey.IdCategory
            KeyExtGrain = 6,
            ClientAddressableObject = 7,
        }

        private UInt64 n0, n1;
        private UInt64 typeCodeData;
        private string keyExt;

        [NonSerialized]
        private int uniformHashCache;
        [NonSerialized]
        private int hashCache;

        public int BaseTypeCode
        {
            get { return (int)(typeCodeData & TypeCodeDataMask); }
        }

        public Category IdCategory
        {
            get { return (Category)((typeCodeData >> 56) & 0xFF); }
        }

        // [mlr][todo] should IsLongKey check that IdCategory is Grain?
        public bool IsLongKey
        {
            get { return n0 == 0; }
        }

        public bool IsSystemTargetKey
        {
            get { return IdCategory == Category.SystemTarget; }
        }

        public bool IsSystemGrainKey
        {
            get { return IdCategory == Category.SystemGrain; }
        }

        public bool HasKeyExt
        {
            get { return IdCategory == Category.KeyExtGrain; }
        }

        internal static readonly UniqueKey Empty =
            new UniqueKey
                {
                    n0 = 0,
                    n1 = 0,
                    typeCodeData = 0,
                    keyExt = null
                };

        internal static int CalculateByteArrayLengthForBinaryTokenStreamReader(int keyExtLen)
        {
            // [mlr] this method is needed to satisfy the needs of BinaryTokenStreamReader.GetGrainId().
            return KeySizeBytes + keyExtLen;
        }

        public static UniqueKey FromByteArray(byte[] bytes)
        {
            // [mlr] this method has been separated into two methods to satisfy the needs of 
            // BinaryTokenStreamReader.GetGrainId() et al, which seems unable to gracefully deal with
            // length-encoded buffers such as this one.
            return FromByteArray(bytes, -1);
        }

        internal static UniqueKey FromByteArray(byte[] bytes, int keyExtByteCount)
        {
            var offset = 0;
            if (keyExtByteCount == -1)
            {
                // [mlr] a value of -1 indicates that the key extension length should be read from the input.
                keyExtByteCount = bytes[0];
                offset = 1;
            }
            else if (keyExtByteCount < -1)
                throw new ArgumentException("The key extension length ({0}) should be -1 or greater.", "keyExtByteCount");

            if (bytes.Length < offset + InternalKeySize)
            {
                throw new ArgumentException(
                    string.Format("Not enough data provided ({0} bytes) to create UniqueKey object (minimum {1} bytes required).",
                                  bytes.Length, InternalKeySize));
            }

            var n0 = BitConverter.ToUInt64(bytes, offset + 0);
            var n1 = BitConverter.ToUInt64(bytes, offset + 8);

            ulong typeCodeData = 0;
            string keyExt = null;
            if (bytes.Length > offset + InternalKeySize)
            {
                typeCodeData = BitConverter.ToUInt64(bytes, offset + InternalKeySize);
                Category category = (Category)((typeCodeData >> 56) & 0xFF);
                if (category == Category.KeyExtGrain)
                {
                    // [mlr] at this point, keyExtByteCount must be at least 1, to account
                    // for the 'isNotNull' flag field.
                    if (keyExtByteCount == 0)
                        throw new InvalidDataException("Premature end of buffer for key extended grain.");

                    switch (bytes[offset + KeySizeBytes])
                    {
                        default:
                            throw new InvalidDataException("Unrecognized value for 'isNotNull' flag in data stream.");
                        case 0:
                            // [mlr] 'keyExt' is null.
                            if (keyExtByteCount != 1)
                                throw new InvalidDataException("End of buffer expected for key extended grain.");
                            break;
                        case 1:
                            // [mlr] 'keyExt' is not null.
                            if (1 == keyExtByteCount)
                                keyExt = "";
                            else
                            {
                                // [mlr] 'keyExt' is not null, nor empty.
                                var keyExtBytes = new byte[keyExtByteCount - 1];
                                Array.Copy(bytes, offset + KeySizeBytes + 1, keyExtBytes, 0, keyExtByteCount - 1);
                                keyExt = Encoding.UTF8.GetString(keyExtBytes);
                            }
                            break;
                    }
                }
            }

            return
                new UniqueKey
                    {
                        n0 = n0,
                        n1 = n1,
                        typeCodeData = typeCodeData,
                        keyExt = keyExt
                    };
        }

        internal static UniqueKey Parse(string input)
        {
            var trimmed = input.Trim();

            // [mlr] first, for convenience we attempt to parse the string using GUID syntax. this is needed by unit
            // tests but i don't know if it's needed for production.
            Guid guid;
            if (Guid.TryParse(trimmed, out guid))
                return NewKey(guid);
            else
            {
                var fields = trimmed.Split('+');
                var n0 = ulong.Parse(fields[0].Substring(0, 16), NumberStyles.HexNumber);
                var n1 = ulong.Parse(fields[0].Substring(16, 16), NumberStyles.HexNumber);
                var typeCodeData = ulong.Parse(fields[0].Substring(32, 16), NumberStyles.HexNumber);
                string keyExt = null;
                switch (fields.Length)
                {
                    default:
                        throw new InvalidDataException("UniqueKey hex strings cannot contain more than one + separator.");
                    case 1:
                        break;
                    case 2:
                        if (fields[1] != "null")
                        {
#if PRINT_KEY_EXT_AS_HEX_BYTES
                            keyExt = Encoding.UTF8.GetString(fields[1].ParseHexBytes());
#else
                            keyExt = fields[1];
#endif
                        }
                        break;
                }
                return
                    new UniqueKey
                        {
                            n0 = n0,
                            n1 = n1,
                            typeCodeData = typeCodeData,
                            keyExt = keyExt
                        };
            }
        }

        private static UniqueKey NewKey(ulong n0, ulong n1, Category category, long typeData, string keyExt)
        {
            // [mlr] in the string representation of a key, we grab the least significant half of n1.
            // therefore, if n0 is non-zero and n1 is 0, then the string representation will always be
            // 0x0 and not useful for identification of the grain.
            if (n1 == 0 && n1 != 0)
                throw new ArgumentException("n0 cannot be zero unless n1 is non-zero.", "n0");
            if (category != Category.KeyExtGrain && keyExt != null)
                throw new ArgumentException("Only key extended grains can specify a non-null key extension.");

            var typeCodeData = ((ulong)category << 56) + ((ulong)typeData & 0x00FFFFFFFFFFFFFF);
            return
                new UniqueKey
                    {
                        n0 = n0,
                        n1 = n1,
                        typeCodeData = typeCodeData,
                        keyExt = keyExt
                    };
        }

        internal static UniqueKey NewKey(long longKey, Category category = Category.None, long typeData = 0, string keyExt = null)
        {
            ThrowIfIsSystemTargetKey(category);

            var n1 = unchecked((ulong)longKey);
            return NewKey(0, n1, category, typeData, keyExt);
        }

        public static UniqueKey NewKey()
        {
            return NewKey(Guid.NewGuid());
        }

        internal static UniqueKey NewKey(Guid guid, Category category = Category.None, long typeData = 0, string keyExt = null)
        {
            ThrowIfIsSystemTargetKey(category);

            var guidBytes = guid.ToByteArray();
            var n0 = BitConverter.ToUInt64(guidBytes, 0);
            var n1 = BitConverter.ToUInt64(guidBytes, 8);
            return NewKey(n0, n1, category, typeData, keyExt);
        }

        public static UniqueKey NewSystemTargetKey(short systemId, IPEndPoint endpoint = null)
        {
            int address, port;
            if (endpoint == null)
            {
                address = 0;
                port = 0;
            }
            else
            {
                address = BitConverter.ToInt32(endpoint.Address.GetAddressBytes(), 0);
                port = endpoint.Port;
            }

            var n1 =
                unchecked(
                    ((ulong)address << 32) |
                        ((ulong)(ushort)systemId << 16) |
                        (ushort)port);

            return NewKey(0, n1, Category.SystemTarget, 0, null);
        }

        private void ThrowIfIsNotLong()
        {
            if (!IsLongKey)
                throw new InvalidOperationException("this key cannot be interpreted as a long value");
        }

        private void ThrowIfIsNotSystemTargetKey()
        {
            if (!IsSystemTargetKey)
                throw new InvalidOperationException("this key cannot be interpreted as a system key");
        }

        private static void ThrowIfIsSystemTargetKey(Category category)
        {
            if (category == Category.SystemTarget)
                throw new ArgumentException(
                    "This overload of NewKey cannot be used to construct an instance of UniqueKey containing a SystemTarget id.");
        }

        private void ThrowIfHasKeyExt(string methodName)
        {
            if (HasKeyExt)
                throw new InvalidOperationException(
                    string.Format(
                        "This overload of {0} cannot be used if the grain uses the primary key extension feature.",
                        methodName));
        }

        public long PrimaryKeyToLong(out string extendedKey)
        {
            // [mlr][todo] should i check that IdCategory is Grain?
            ThrowIfIsNotLong();

            extendedKey = this.keyExt;
            return unchecked((long)n1);
        }

        public long PrimaryKeyToLong()
        {
            ThrowIfHasKeyExt("UniqueKey.PrimaryKeyToLong");
            string unused;
            return PrimaryKeyToLong(out unused);
        }

        public Guid PrimaryKeyToGuid(out string extendedKey)
        {
            extendedKey = this.keyExt;
            return ConvertToGuid();
        }

        public Guid PrimaryKeyToGuid()
        {
            ThrowIfHasKeyExt("UniqueKey.PrimaryKeyToGuid");
            string unused;
            return PrimaryKeyToGuid(out unused);
        }

        [Pure]
        public short PrimaryKeyToSystemId()
        {
            ThrowIfIsNotSystemTargetKey();
            var sid = unchecked((short)(n1 >> 16));
            return sid;
        }

        public override bool Equals(object o)
        {
            return o is UniqueKey && Equals((UniqueKey)o);
        }

        // We really want Equals to be as fast as possible, as a minimum cost, as close to native as possible.
        // No function calls, no boxing, inline.
        public bool Equals(UniqueKey other)
        {
            return n0 == other.n0
                   && n1 == other.n1
                   && typeCodeData == other.typeCodeData
                   && (!HasKeyExt || keyExt == other.keyExt);
        }

        // We really want CompareTo to be as fast as possible, as a minimum cost, as close to native as possible.
        // No function calls, no boxing, inline.
        public int CompareTo(UniqueKey other)
        {
            return typeCodeData < other.typeCodeData ? -1
               : typeCodeData > other.typeCodeData ? 1
               : n0 < other.n0 ? -1
               : n0 > other.n0 ? 1
               : n1 < other.n1 ? -1
               : n1 > other.n1 ? 1
               : !HasKeyExt || keyExt == null ? 0
               : String.Compare(keyExt, other.keyExt, StringComparison.Ordinal);
        }

        // GetHashCode is used in Dictionaries. It does not have to spread the keys uniformly, as GetUniformHashCode has to,
        // but it is very advantageous that it does so, resulting in improved Dictionary performance.
        // However, GetHashCode also has to be fast, since it is called a lot of times on a UniqueKey as a key to interning data structure.
        // So we can't compute JenkinsHash every time. 
        // Instead, we use a compromise between a "good" uniformly spread but slow JenkinsHash and a "reasonbaly spread" and faster Knuth's hash.
        // A trivial (n0 ^ n1 ^ typeCodeData) performs badly in terms of collisions.
        // Implementation wise, we prefer to inline, since this is a really hot path (no function calls here).
        public override int GetHashCode()
        {
            // Disabling this ReSharper warning; hashCache is a logically read-only variable, so accessing them in GetHashCode is safe.
            // ReSharper disable NonReadonlyFieldInGetHashCode
            if (hashCache == 0)
            {
                //hashCache = unchecked((int)(n0 ^ n1 ^ typeCodeData));
                // Constants from Donald Knuth's MMIX
                const ulong a = 6364136223846793005;
                const ulong c = 1442695040888963407;
                unchecked
                {
                    ulong r1 = a * n1 + c;
                    ulong r2 = a * n0 + c;
                    int n = 2 + (int)((n1 ^ n0) % 7);
                    for (int i = 0; i < n; i++)
                    {
                        r1 = a * r1 + c;
                        r2 = a * r2 + c;
                    }
                    hashCache = (int)(r1 ^ r2 ^ typeCodeData);
                }
                if (HasKeyExt && keyExt != null)
                    hashCache = unchecked((int) (((UInt32) (hashCache)) ^ ((UInt32) keyExt.GetHashCode())));

            }
            return hashCache;
            // ReSharper restore NonReadonlyFieldInGetHashCode
        }

        internal int GetUniformHashCode()
        {
            // Disabling this ReSharper warning; hashCache is a logically read-only variable, so accessing them in GetHashCode is safe.
            // ReSharper disable NonReadonlyFieldInGetHashCode
            if (uniformHashCache == 0)
            {
                JenkinsHash jenkinsHash = JenkinsHash.Factory.GetHashGenerator();
                uint n;
                if (HasKeyExt && keyExt != null)
                {
                    byte[] bytes = ToByteArray();
                    n = jenkinsHash.ComputeHash(bytes);
                }
                else
                {
                    n = jenkinsHash.ComputeHash(typeCodeData, n0, n1);
                }
                // Unchecked is required because the Jenkins hash is an unsigned 32-bit integer, 
                // which we need to convert to a signed 32-bit integer.
                uniformHashCache = unchecked((int)n);
            }
            return uniformHashCache;
            // ReSharper restore NonReadonlyFieldInGetHashCode
        }

        private void PutBytes(byte[] dst, ref int index, byte[] src)
        {
            var a0 = BitConverter.GetBytes(n0);
            src.CopyTo(dst, index);
            index += a0.Length;
        }

        private static void PutByte(byte[] dst, ref int index, byte src)
        {
            dst[index] = src;
            ++index;
        }

        private void PutKey(byte[] dst, ref int index)
        {
            PutBytes(dst, ref index, BitConverter.GetBytes(n0));
            PutBytes(dst, ref index, BitConverter.GetBytes(n1));
        }

        public byte[] ToByteArray()
        {
            byte[] keyExtBytes = null;
            int keyExtLen = 0;
            if (HasKeyExt)
            {
                // [mlr] we add one to account for a flag that will differentiate
                // between null and an empty string, as Encoding.UTF8.GetBytes("") => byte[0].
                if (keyExt == null)
                    keyExtLen = 1;
                else
                {
                    keyExtBytes = Encoding.UTF8.GetBytes(keyExt);
                    keyExtLen = keyExtBytes.Length + 1;
                }
            }

            int sz = 1 + KeySizeBytes + keyExtLen;
            var result = new byte[sz];

            int i = 0;
            PutByte(result, ref i, (byte)keyExtLen);
            PutKey(result, ref i);
            PutBytes(result, ref i, BitConverter.GetBytes(typeCodeData));
            if (HasKeyExt)
            {
                if (keyExt == null)
                {
                    PutByte(result, ref i, 0);
                }
                else
                {
                    PutByte(result, ref i, 1);
                    PutBytes(result, ref i, keyExtBytes);
                }
            }
            return result;
        }

        private Guid ConvertToGuid()
        {
            const int sz = InternalKeySize;
            var bytes = new byte[sz];

            int i = 0;
            PutKey(bytes, ref i);

            return new Guid(bytes);
        }

        public override string ToString()
        {
            return ToHexString();
        }

        internal string ToHexString()
        {
            var s = new StringBuilder();
            s.AppendFormat("{0:x16}{1:x16}{2:x16}", n0, n1, typeCodeData);
            if (HasKeyExt)
            {
                s.Append("+");
                if (keyExt == null)
                    s.Append("null");
                else
                {
#if PRINT_KEY_EXT_AS_HEX_BYTES
                    var keyExtBytes = Encoding.UTF8.GetBytes(keyExt);
                    foreach (var i in keyExtBytes)
                        s.Append(i.ToString("X2"));
#else
                    s.Append(keyExt);
#endif
                }
            }
            return s.ToString();
        }

        private string ToBase64String()
        {
            return Convert.ToBase64String(ToByteArray());
        }

        public string ToString(string fmt)
        {
            if (fmt == "X")
                return ToHexString();
            else if (fmt == "B64")
                return ToBase64String();
            else
                throw new ArgumentException(
                    String.Format("unrecognized string format specification ({0})", fmt),
                    "fmt");
        }
    }
}
