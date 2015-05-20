using System;
using System.Threading;
using Orleans;

namespace UnitTests
{
    internal class LocalErrorGrain
    {
        int m_a = 0;
        int m_b = 0;

        public LocalErrorGrain() { }

        public AsyncCompletion SetA(int a)
        {
            AsyncValue<Int32> A = new AsyncValue<Int32>(a);
            return A.ContinueWith(avalue =>
            {
                m_a = avalue;
            });
        }

        public AsyncCompletion SetB(int b)
        {
            AsyncValue<Int32> B = new AsyncValue<Int32>(b);
            return B.ContinueWith(bvalue =>
            {
                m_b = bvalue;
            });
        }

        public AsyncValue<Int32> GetAxB()
        {
            AsyncValue<Int32> AB = new AsyncValue<Int32>(m_a * m_b);
            return AB.ContinueWith(abvalue =>
            {
                return abvalue;
            });
        }

        public AsyncValue<Int32> GetAxBError()
        {
            AsyncValue<Int32> AB = new AsyncValue<Int32>(m_a * m_b);
            return AB.ContinueWith(new Func<int, int>(abvalue =>
            {
                throw new Exception("GetAxBError-Exception");
            }));
        }

        public AsyncCompletion LongMethod(int waitTime)
        {
            AsyncValue<Int32> A = new AsyncValue<Int32>(waitTime);
            return A.ContinueWith(_waitTime =>
            {
                Thread.Sleep(_waitTime);
            });
        }

        public AsyncCompletion LongMethodWithError(int waitTime)
        {
            AsyncValue<Int32> A = new AsyncValue<Int32>(waitTime);
            return A.ContinueWith(_waitTime =>
            {
                Thread.Sleep(_waitTime);
                throw new Exception("LongMethodWithError(" + waitTime + ")");
            });
        }
    }
}