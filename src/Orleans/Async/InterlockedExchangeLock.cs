
using System.Threading;

namespace Orleans
{
    internal class InterlockedExchangeLock
    {
        private const int Locked = 1;
        private const int Unlocked = 0;
        private int lockState = Unlocked;

        public bool TryGetLock()
        {
            return Interlocked.Exchange(ref lockState, Locked) != Locked;
        }

        public void ReleaseLock()
        {
            Interlocked.Exchange(ref lockState, Unlocked);
        }
    }
}
