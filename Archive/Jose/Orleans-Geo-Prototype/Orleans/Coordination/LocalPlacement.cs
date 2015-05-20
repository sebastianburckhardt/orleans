using System;

namespace Orleans
{
    [Serializable]
    internal class LocalPlacement : PlacementStrategy
    {
        private static int DefaultMaxActivationBankSize = System.Environment.ProcessorCount;

        public int MinAvailable { get; private set; }

        public int MaxLocal { get; private set; }

        internal static void InitializeClass(int defaultMaxActivationBankSize)
        {
            if (defaultMaxActivationBankSize < 1)
                throw new ArgumentOutOfRangeException("defaultMaxActivationBankSize",
                    "defaultMaxActivationBankSize must contain a value greater than zero.");
            else
                DefaultMaxActivationBankSize = defaultMaxActivationBankSize;
        }

        internal LocalPlacement(int minAvailable, int defaultMaxLocal = -1)
        {
            MinAvailable = minAvailable;
            MaxLocal = defaultMaxLocal > 0 ? defaultMaxLocal : DefaultMaxActivationBankSize;
        }

        public override string ToString()
        {
            return String.Format("LocalPlacement(min={0}, max={1})", MinAvailable, MaxLocal);
        }

        public override bool Equals(object obj)
        {
            var other = obj as LocalPlacement;
            return other != null && MinAvailable == other.MinAvailable && MaxLocal == other.MaxLocal;
        }

        public override int GetHashCode()
        {
            return GetType().GetHashCode() + MinAvailable.GetHashCode() + MaxLocal.GetHashCode();
        }
    }

}
