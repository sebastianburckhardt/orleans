using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Orleans
{
    [Serializable]
    internal class ActivationAddress
    {
        public GrainId Grain { get; private set; }
        public ActivationId Activation { get; private set; }
        public SiloAddress Silo { get; private set; }

#if USE_INTERNER
        private string key;

        private static readonly int InternCacheInitialSize = 143357;
        private static readonly int InternCacheConcurrencyLevel = InternerConstants.DefaultCacheConcurrencyLevel;
        private static readonly TimeSpan InternCacheCleanupInterval = InternerConstants.DefaultCacheCleanupFreq;

        private static readonly Interner<string, ActivationAddress> interner = new Interner<string, ActivationAddress>(InternCacheInitialSize, InternCacheConcurrencyLevel, InternCacheCleanupInterval);
#endif

        public bool IsComplete
        {
            get { return Grain != null && Activation != null && Silo != null; }
        }

        private const char Separator = '/';

        private ActivationAddress(SiloAddress silo, GrainId grain, ActivationId activation)
        {
            Silo = silo;
            Grain = grain;
            Activation = activation;
        }

        public static ActivationAddress NewActivationAddress(SiloAddress silo, GrainId grain = null)
        {
            if (grain == null)
            {
                grain = GrainId.NewId(); // GrainID part is mandatory
            }
            ActivationId activation = ActivationId.NewId();
            ActivationAddress address = GetAddress(silo, grain, activation);
            return address;
        }

        public static ActivationAddress GetAddress(SiloAddress silo, GrainId grain, ActivationId activation)
        {
            // Silo part is not mandatory: if (silo == null) throw new NullReferenceException("silo address cannot be null");
            if (grain == null) throw new ArgumentNullException("grain");

            ActivationAddress address;

#if USE_INTERNER
            var key = CalculateKey(silo, grain, activation);
            address = interner.FindOrCreate(key, () => new ActivationAddress(silo, grain, activation));
            if (address.key == null)
            {
                address.key = key;
            }
#else
            address = new ActivationAddress(silo, grain, activation);
#endif

            return address;
        }

#if USE_INTERNER
        private static string CalculateKey(SiloAddress silo, GrainId grain, ActivationId activation)
        {
            throw new NotImplementedException("If ActivationAddresses are interned, the CalculateKey method must be defined");
            // A reasonable approach for a good key might be to create an ActivationAddress, serialize it to a byte stream,
            // and then binhex it to a character string
        }
#endif

        public override bool Equals(object obj)
        {
            var other = obj as ActivationAddress;
            return other != null && Equals(Silo, other.Silo) && Equals(Grain, other.Grain) && Equals(Activation, other.Activation);
        }

        public override int GetHashCode()
        {
            return (Silo != null ? Silo.GetHashCode() : 0) ^
                (Grain != null ? Grain.GetHashCode() : 0) ^
                (Activation != null ? Activation.GetHashCode() : 0);
        }

        public override string ToString()
        {
            return String.Format("{0}{1}{2}", Silo, Grain, Activation);
        }

        public string ToFullString()
        {
            return
                String.Format(
                    "[ActivationAddress: {0}, Full GrainId: {1}, Full ActivationId: {2}]",
                    this.ToString(),                        // 0
                    this.Grain.ToFullString(),              // 1
                    this.Activation.ToFullString());        // 2
        }

        public bool Matches(ActivationAddress other)
        {
            return Equals(Grain, other.Grain) && Equals(Activation, other.Activation);
        }

        public GrainReference GrainReference { get { return GrainReference.FromGrainId(Grain); }}
    }
}
