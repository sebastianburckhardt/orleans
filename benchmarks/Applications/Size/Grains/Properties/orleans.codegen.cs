//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated by a tool.
//     Runtime Version:4.0.30319.34209
//
//     Changes to this file may cause incorrect behavior and will be lost if
//     the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------
#if !EXCLUDE_CODEGEN
#pragma warning disable 162
#pragma warning disable 219
#pragma warning disable 414
#pragma warning disable 649
#pragma warning disable 693
#pragma warning disable 1591
#pragma warning disable 1998

namespace Size.Grains
{
    using Orleans.CodeGeneration;
    using Orleans;
    using System;
    using System.Runtime.InteropServices;
    using System.Runtime.Serialization;
    using System.Collections;
    using System.Collections.Generic;
    using ReplicatedGrains;
    
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [SerializableAttribute()]
    [global::Orleans.CodeGeneration.GrainStateAttribute("Size.Grains.Size.Grains.SequencedSizeGrain")]
    public class SequencedSizeGrainState : global::Orleans.CodeGeneration.GrainState, IGlobalState
    {
        

            public Int64 @Version { get; set; }

            public Byte[] @Raw { get; set; }

            public override void SetAll(System.Collections.Generic.IDictionary<string,object> values)
            {   
                object value;
                if (values == null) { InitStateFields(); return; }
                if (values.TryGetValue("Version", out value)) @Version = value is Int32 ? (Int32)value : (Int64)value;
                if (values.TryGetValue("Raw", out value)) @Raw = (Byte[]) value;
            }

            public override System.String ToString()
            {
                return System.String.Format("SequencedSizeGrainState( Version={0} Raw={1} )", @Version, @Raw);
            }
        
        public SequencedSizeGrainState() : 
                base("Size.Grains.SequencedSizeGrain")
        {
            this.InitStateFields();
        }
        
        public override System.Collections.Generic.IDictionary<string, object> AsDictionary()
        {
            System.Collections.Generic.Dictionary<string, object> result = new System.Collections.Generic.Dictionary<string, object>();
            result["Version"] = this.Version;
            result["Raw"] = this.Raw;
            return result;
        }
        
        private void InitStateFields()
        {
            this.Version = default(Int64);
            this.Raw = default(Byte[]);
        }
        
        [global::Orleans.CodeGeneration.CopierMethodAttribute()]
        public static object _Copier(object original)
        {
            SequencedSizeGrainState input = ((SequencedSizeGrainState)(original));
            return input.DeepCopy();
        }
        
        [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
        public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
        {
            SequencedSizeGrainState input = ((SequencedSizeGrainState)(original));
            input.SerializeTo(stream);
        }
        
        [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
        public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
        {
            SequencedSizeGrainState result = new SequencedSizeGrainState();
            result.DeserializeFrom(stream);
            return result;
        }
    }
}
#pragma warning restore 162
#pragma warning restore 219
#pragma warning restore 414
#pragma warning restore 649
#pragma warning restore 693
#pragma warning restore 1591
#pragma warning restore 1998
#endif
