using System;
using Microsoft.FSharp.Collections;
using Orleans.Serialization;

namespace Orleans.FSharp
{
    /// <summary>
    /// Serialization utilities for F# data types which have no direct correspondence in C#.
    /// Currently this supports <c>Microsoft.FSharp.Collections.FSharpList{T}</c>.
    /// </summary>
    /// <remarks>
    /// If this assembly is deployed with Orleans silo or client, 
    /// then these serialization helper functions will be auto-detected 
    /// and registered for use by the Orleans runtime.
    /// </remarks>
    [RegisterSerializer]
    public class FSharpSerializers
    {
        /// <summary>
        /// Hook function to auto-register serialization helper functions for supported F# types.
        /// </summary>
        public static void Register()
        {
            SerializationManager.Register(typeof(FSharpList<>), CopyGenericFSharpList, SerializeGenericFSharpList, DeserializeGenericFSharpList);
        }

        /// <summary> Serializer function for generic FSharpList{} types. </summary>
        internal static void SerializeGenericFSharpList(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var generics = t.GetGenericArguments();
            var concretes = BuiltInTypes.RegisterConcreteMethods(t, typeof(FSharpSerializers), "SerializeFSharpList", "DeserializeFSharpList", "DeepCopyFSharpList", generics);

            concretes.Item1(original, stream, expected);
        }

        /// <summary> Deserializer function for generic FSharpList{} types. </summary>
        internal static object DeserializeGenericFSharpList(Type expected, BinaryTokenStreamReader stream)
        {
            var generics = expected.GetGenericArguments();
            var concretes = BuiltInTypes.RegisterConcreteMethods(expected, typeof(FSharpSerializers), "SerializeFSharpList", "DeserializeFSharpList", "DeepCopyFSharpList", generics);

            return concretes.Item2(expected, stream);
        }

        /// <summary> Copier function for generic FSharpList{} types. </summary>
        internal static object CopyGenericFSharpList(object original)
        {
            Type t = original.GetType();
            var generics = t.GetGenericArguments();
            var concretes = BuiltInTypes.RegisterConcreteMethods(t, typeof(FSharpSerializers), "SerializeFSharpList", "DeserializeFSharpList", "DeepCopyFSharpList", generics);

            return concretes.Item3(original);
        }

        /// <summary> Serializer function for concrete FSharpList{} types. </summary>
        internal static void SerializeFSharpList<T>(object obj, BinaryTokenStreamWriter stream, Type expected)
        {
            var list = (FSharpList<T>)obj;

            stream.Write(list.Length);

            foreach (var element in list)
            {
                SerializationManager.SerializeInner(element, stream, typeof(T));
            }
        }

        /// <summary> Deserializer function for concrete FSharpList{} types. </summary>
        internal static object DeserializeFSharpList<T>(Type expected, BinaryTokenStreamReader stream)
        {
            var count = stream.ReadInt();
            var list = Array.CreateInstance(expected, count);
            for (var i = 0; i < count; i++)
            {
                list.SetValue((T)SerializationManager.DeserializeInner(typeof(T), stream), i);
            }
            Func<int, T> generator = n => (T)list.GetValue(n);
            var fsharpGenerator = Microsoft.FSharp.Core.FSharpFunc<int, T>.FromConverter(new Converter<int, T>(generator));
            var result = ListModule.Initialize(count, fsharpGenerator);
            return result;
        }

        /// <summary> Copier function for concrete FSharpList{} types. </summary>
        internal static object DeepCopyFSharpList<T>(object original)
        {
            var list = (FSharpList<T>)original;
            Func<int, T> generator = n => (T)SerializationManager.DeepCopyInner(list[n]);
            var fsharpGenerator = Microsoft.FSharp.Core.FSharpFunc<int, T>.FromConverter(new Converter<int, T>(generator));
            var result = ListModule.Initialize(list.Length, fsharpGenerator);
            return result;
        }
    }
}
