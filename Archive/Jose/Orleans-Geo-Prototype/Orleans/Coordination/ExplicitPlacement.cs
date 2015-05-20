using System;
using System.Net;


namespace Orleans
{
    [Serializable]
    internal class ExplicitPlacement : PlacementStrategy
    {
        public IPEndPoint Endpoint { get; private set; }
        internal SiloAddress Silo { get { return SiloAddress.New(Endpoint, 0); } }

        internal static ExplicitPlacement Tbd { get; private set; }

        private ExplicitPlacement(IPEndPoint endpoint)
        {
            Endpoint = endpoint;
        }

        internal static void InitializeClass()
        {
            Tbd = new ExplicitPlacement(null);
        }

        internal static ExplicitPlacement NewObject(IPEndPoint endpoint)
        {
            if (endpoint == null)
                throw new ArgumentNullException(
                    "endpoint",
                    "Explicit placement requires an IPEndPoint argument specifying the target silo.");
            else
                return new ExplicitPlacement(endpoint);
        }

        public override string ToString()
        {
            return string.Format("ExplicitPlacement({0})", Endpoint.ToString());
        }

        public override bool Equals(object obj)
        {
            var other = obj as ExplicitPlacement;
            return other != null && Object.Equals(Endpoint, other.Endpoint);
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode() + Endpoint.GetHashCode();
        }
    }
}
