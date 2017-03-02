
using System;
using System.Collections.Generic;

namespace Orleans.Factory
{
    internal interface IFactory<out TType>
        where TType : class
    {
        TType Create();
    }

    internal interface IFactory<TKey, out TType>
        where TKey : IComparable<TKey>
        where TType : class
    {
        IEnumerable<TKey> Keys { get; }
        TType Create(TKey key);
    }
}
