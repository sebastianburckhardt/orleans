using System;

namespace Orleans
{
    /// <summary>
    /// Thread-safe random number generator.
    /// Has same API as System.Random but takes a lock.
    /// </summary>
    internal class SafeRandom
    {
        private readonly Random random;

        public SafeRandom()
        {
            random = new Random();
        }

        public SafeRandom(int seed)
        {
            random = new Random(seed);
        }

        public int Next()
        {
            lock (random)
            {
                return random.Next();
            }
        }

        public int Next(int maxValue)
        {
            lock (random)
            {
                return random.Next(maxValue);
            }
        }

        public int Next(int minValue, int maxValue)
        {
            lock (random)
            {
                return random.Next(minValue, maxValue);
            }
        }

        public void NextBytes(byte[] buffer)
        {
            lock (random)
            {
                random.NextBytes(buffer);
            }
        }

        public double NextDouble()
        {
            lock (random)
            {
                return random.NextDouble();
            }
        }
    }
}
