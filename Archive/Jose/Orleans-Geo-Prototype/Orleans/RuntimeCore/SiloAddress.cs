using System;
using System.Diagnostics;
using System.Globalization;
using System.Net;

namespace Orleans
{
    /// <summary>
    /// Data class encapsulating the details of silo addresses.
    /// </summary>
    [Serializable]
    [DebuggerDisplay("SiloAddress {ToString()}")]
    public class SiloAddress : IEquatable<SiloAddress>
    {

        private readonly static IPEndPoint DummyEndpoint = new IPEndPoint(0, 0);
        // internal static readonly int SizeBytes = 24; // 16 for the address, 4 for the port, 4 for the generation

        /// <summary> Special constant value to indicate an empty SiloAddress. </summary>
        public static SiloAddress Zero { get; private set; }

        private static readonly int InternCacheInitialSize = InternerConstants.Size_Medium;
        private static readonly TimeSpan InternCacheCleanupInterval = TimeSpan.Zero;

        private int hashCode = 0;
        private bool hashCodeSet = false;

        public IPEndPoint Endpoint { get; private set; }
        public int Generation { get; private set; }

        // Jose: Value should be nonnegative in order to be meaningful.
        public int ClusterId { get; private set; }


        private const char Separator = '@';
        private const char DatacenterSeparator = '#';

        private static readonly DateTime epoch = new DateTime(2010, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        private static readonly Interner<SiloAddress, SiloAddress> siloAddressInterningCache;

        private static readonly IPEndPoint localEndpoint = new IPEndPoint(OrleansConfiguration.GetLocalIPAddress(), 0); // non loopback local ip.

        static SiloAddress()
        {
            siloAddressInterningCache = new Interner<SiloAddress, SiloAddress>(InternCacheInitialSize, InternCacheCleanupInterval);
            var sa = new SiloAddress(new IPEndPoint(0, 0), 0);
            Zero = siloAddressInterningCache.Intern(sa, sa);
        }

        /// <summary>
        /// Factory for creating new SiloAddresses for silo on this machine with specified generation number.
        /// </summary>
        /// <param name="gen">Generation number of the silo.</param>
        /// <returns>SiloAddress object initialized with the non-loopback local IP address and the specified silo generation.</returns>
        public static SiloAddress NewLocalAddress(int gen)
        {
            return New(localEndpoint, gen);
        }

        /// <summary>
        /// Factory for creating new SiloAddresses with specified IP endpoint address and silo generation number.
        /// </summary>
        /// <param name="ep">IP endpoint address of the silo.</param>
        /// <param name="gen">Generation number of the silo.</param>
        /// <returns>SiloAddress object initialized with specified address and silo generation.</returns>
        public static SiloAddress New(IPEndPoint ep, int gen)
        {
            var sa = new SiloAddress(ep, gen);
            return siloAddressInterningCache.Intern(sa, sa);
        }

        public static SiloAddress New(IPEndPoint ep, int gen, int datacenterId)
        {
            var sa = new SiloAddress(ep, gen, datacenterId);
            return siloAddressInterningCache.Intern(sa, sa);
        }

        private SiloAddress(IPEndPoint endpoint, int gen)
        {
            Endpoint = endpoint;
            Generation = gen;
            ClusterId = -1;
        }

        private SiloAddress(IPEndPoint endpoint, int gen, int datacenterId)
        {
            Endpoint = endpoint;
            Generation = gen;
            ClusterId = datacenterId;
        }

        //public static SiloAddress NewClusterRef(int datacenterid)
        //{
        //    return SiloAddress.New(DummyEndpoint, 1, datacenterid);
        //}

        public bool IsClient { get { return Generation < 0; } }

        public bool IsSameCluster(SiloAddress other)
        {
            return (other.ClusterId == -1 || ClusterId == -1 || other.ClusterId == ClusterId);
        }

        /// <summary> Allocate a new silo generation number. </summary>
        /// <returns>A new silo generation number.</returns>
        public static int AllocateNewGeneration()
        {
            long elapsed = (DateTime.UtcNow.Ticks - epoch.Ticks) / TimeSpan.TicksPerSecond;
            return unchecked((int)elapsed); // Unchecked to truncate any bits beyond the lower 32
        }

        /// <summary>
        /// Return this SiloAddress in a standard string form, suitable for later use with the <c>FromParsableString</c> method.
        /// </summary>
        /// <returns>SiloAddress in a standard string format.</returns>
        public string ToParsableString()
        {
            // This must be the "inverse" of FromParsableString, and must be the same across all silos in a deployment.
            // Basically, this should never change unless the data content of SiloAddress changes

            return String.Format("{0}:{1}@{2}#{3}", Endpoint.Address, Endpoint.Port, Generation, ClusterId);
        }

        /// <summary>
        /// Create a new SiloAddress object by parsing string in a standard form returned from <c>ToParsableString</c> method.
        /// </summary>
        /// <param name="addr">String containing the SiloAddress info to be parsed.</param>
        /// <returns>New SiloAddress object created from the input data.</returns>
        public static SiloAddress FromParsableString(string addr)
        {
            // This must be the "inverse" of ToParsableString, and must be the same across all silos in a deployment.
            // Basically, this should never change unless the data content of SiloAddress changes

            // First is the IPEndpoint; then '@'; then the generation; then '#'; then datacenter id.
            int atSign = addr.LastIndexOf(Separator);
            int hashSign = addr.LastIndexOf(DatacenterSeparator);
            if (hashSign < 0 || atSign < 0) 
            {
                throw new FormatException("Invalid string SiloAddress: " + addr);
            }
            string epString = addr.Substring(0, atSign);
            string genString = addr.Substring(atSign + 1, hashSign-atSign-1);
            string datacenterString = addr.Substring(hashSign + 1);
            // datacenterId must be an integer. 
            int datacenterID;
            if (!int.TryParse(datacenterString, out datacenterID))
            {
                throw new FormatException("Invalid string SiloAddress: " + addr);
            }
            // IPEndpoint is the host, then ':', then the port
            int lastColon = epString.LastIndexOf(':');
            if (lastColon < 0)
            {
                throw new FormatException("Invalid string SiloAddress: " + addr);
            }
            string hostString = epString.Substring(0, lastColon);
            string portString = epString.Substring(lastColon + 1);
            IPAddress host = IPAddress.Parse(hostString);
            int port = Int32.Parse(portString);
            return New(new IPEndPoint(host, port), Int32.Parse(genString));
        }

        /// <summary> Object.ToString method override. </summary>
        public override string ToString()
        {
            return String.Format("{0}{1}:{2}:{3}", (IsClient ? "C" : "S"), Endpoint, Generation, ClusterId);
        }

        /// <summary>
        /// Return a long string representation of this SiloAddress.
        /// </summary>
        /// <remarks>
        /// Note: This string value is not comparable with the <c>FromParsableString</c> method -- use the <c>ToParsableString</c> method for that purpose.
        /// </remarks>
        /// <returns>String representaiton of this SiloAddress.</returns>
        public string ToLongString()
        {
            //return String.Format("{0}{1} Gen={2} Hash={3:X}, CH={4:X}", (IsClient ? "C" : "S"), Endpoint.ToString(), Generation, GetHashCode(), GetConsistentHashCode());
            return ToString();
        }

        /// <summary>
        /// Return a long string representation of this SiloAddress, including it's consistent hash value.
        /// </summary>
        /// <remarks>
        /// Note: This string value is not comparable with the <c>FromParsableString</c> method -- use the <c>ToParsableString</c> method for that purpose.
        /// </remarks>
        /// <returns>String representaiton of this SiloAddress.</returns>
        public string ToStringWithHashCode()
        {
            return String.Format("{0}/x{1, 8:X8}", ToString(), GetConsistentHashCode());
        }

        /// <summary> Object.Equals method override. </summary>
        public override bool Equals(object obj)
        {
            return Equals(obj as SiloAddress);
        }

        /// <summary> Object.GetHashCode method override. </summary>
        public override int GetHashCode()
        {
            // Note that Port cannot be used because Port==0 matches any non-zero Port value for .Equals
            return Endpoint.GetHashCode() ^ Generation.GetHashCode();
        }

        /// <summary> Get a consistent hash value for this silo address. </summary>
        /// <returns>Consistent hash value for this silo address.</returns>
        public int GetConsistentHashCode()
        {
            if (!hashCodeSet)
            {
                // TODO: compute something clever from keyBytes
                // Note that Port cannot be used because Port==0 matches any non-zero Port value for .Equals
                string siloAddressInfoToHash = Endpoint + Generation.ToString(CultureInfo.InvariantCulture);

                hashCode = Utils.CalculateIdHash(siloAddressInfoToHash);
                hashCodeSet = true;
            }
            return hashCode;
        }

        /// <summary>
        /// Two silo addresses match if they are equal or if one generation or the other is 0
        /// </summary>
        /// <param name="other"> The other SiloAddress to compare this one with. </param>
        /// <returns> Returns <c>true</c> if the two SiloAddresses are considered to match -- if they are equal or if one generation or the other is 0. </returns>
        internal bool Matches(SiloAddress other)
        {
            return other != null && Endpoint.Address.Equals(other.Endpoint.Address) && (Endpoint.Port == other.Endpoint.Port) &&
                IsSameCluster(other) &&
                ((Generation == other.Generation) || (Generation == 0) || (other.Generation == 0));
        }

        #region IEquatable<SiloAddress> Members

        /// <summary> IEquatable.Equals method override. </summary>
        public bool Equals(SiloAddress other)
        {
            return other != null && Endpoint.Address.Equals(other.Endpoint.Address) && (Endpoint.Port == other.Endpoint.Port) &&
                ((Generation == other.Generation)) && (this.ClusterId == other.ClusterId);
        }

        #endregion
    }
}
