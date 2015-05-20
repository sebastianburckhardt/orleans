using System;

namespace Orleans
{
    //[Serializable]
    //internal class RequestId : UniqueIdentifier
    //{
    //    private RequestId(UniqueKey key) : 
    //        base(key)
    //    {}

    //    internal static RequestId GetRequestId(byte[] bytes)
    //    {
    //        return new RequestId(UniqueKey.FromByteArray(bytes, 0));
    //    }

    //    public bool IsReadOnly { get { return (Key.BaseTypeCode & 1) != 0; } }

    //    public override string ToString()
    //    {
    //        return ":" + base.ToString().Substring(24, 8); //.Tail(4);
    //    }

    //    public override bool Equals(UniqueIdentifier obj)
    //    {
    //        var o = obj as RequestId;
    //        return o != null && Key.Equals(o.Key);
    //    }

    //    public override bool Equals(object obj)
    //    {
    //        var o = obj as RequestId;
    //        return o != null && Key.Equals(o.Key);
    //    }

    //    private static readonly int ClassHash = typeof(RequestId).GetHashCode();

    //    public override int GetHashCode()
    //    {
    //        return Key.GetHashCode() ^ ClassHash;
    //    }
    //}
}
