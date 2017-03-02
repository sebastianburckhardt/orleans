
using System;

namespace Orleans.Factory
{
    internal interface IFactoryBuilder<TKey, TType>
        where TKey : IComparable<TKey>
        where TType : class
    {
        void Add(TKey key, IFactory<TType> factory);
        IFactory<TKey, TType> Build();
    }
}
