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

namespace GeoOrleans.Runtime.ClusterProtocol.Interfaces
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.IO;
    using System.Collections.Generic;
    using System.Reflection;
    using Orleans.Serialization;
    using GeoOrleans.Runtime.ClusterProtocol.Interfaces;
    using Orleans;
    using Orleans.Runtime;
    using System.Collections;
    
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    public class ClusterRepFactory
    {
        

                        public static GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep GetGrain(long primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep), -2017674181, primaryKey));
                        }

                        public static GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep GetGrain(long primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep), -2017674181, primaryKey, grainClassNamePrefix));
                        }

            public static GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return ClusterRepReference.Cast(grainRef);
            }
        
        [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
        [System.SerializableAttribute()]
        [global::Orleans.CodeGeneration.GrainReferenceAttribute("GeoOrleans.Runtime.ClusterProtocol.Interfaces.GeoOrleans.Runtime.ClusterProtocol." +
            "Interfaces.IClusterRep")]
        internal class ClusterRepReference : global::Orleans.Runtime.GrainReference, global::Orleans.Runtime.IAddressable, GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep
        {
            

            public static GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return (GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep) global::Orleans.Runtime.GrainReference.CastInternal(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep), (global::Orleans.Runtime.GrainReference gr) => { return new ClusterRepReference(gr);}, grainRef, -2017674181);
            }
            
            protected internal ClusterRepReference(global::Orleans.Runtime.GrainReference reference) : 
                    base(reference)
            {
            }
            
            protected internal ClusterRepReference(SerializationInfo info, StreamingContext context) : 
                    base(info, context)
            {
            }
            
            protected override int InterfaceId
            {
                get
                {
                    return -2017674181;
                }
            }
            
            public override string InterfaceName
            {
                get
                {
                    return "GeoOrleans.Runtime.ClusterProtocol.Interfaces.GeoOrleans.Runtime.ClusterProtocol." +
                        "Interfaces.IClusterRep";
                }
            }
            
            [global::Orleans.CodeGeneration.CopierMethodAttribute()]
            public static object _Copier(object original)
            {
                ClusterRepReference input = ((ClusterRepReference)(original));
                return ((ClusterRepReference)(global::Orleans.Runtime.GrainReference.CopyGrainReference(input)));
            }
            
            [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
            public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                ClusterRepReference input = ((ClusterRepReference)(original));
                global::Orleans.Runtime.GrainReference.SerializeGrainReference(input, stream, expected);
            }
            
            [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
            public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                return ClusterRepReference.Cast(((global::Orleans.Runtime.GrainReference)(global::Orleans.Runtime.GrainReference.DeserializeGrainReference(expected, stream))));
            }
            
            public override bool IsCompatible(int interfaceId)
            {
                return ((interfaceId == this.InterfaceId) 
                            || (interfaceId == 1928988877));
            }
            
            protected override string GetMethodName(int interfaceId, int methodId)
            {
                return ClusterRepMethodInvoker.GetMethodName(interfaceId, methodId);
            }
            
            System.Threading.Tasks.Task<System.Collections.Generic.Dictionary<string, GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo>> GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep.GetGlobalInfo()
            {

                return base.InvokeMethodAsync<System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo>>(-1181674556, null );
            }
            
            System.Threading.Tasks.Task<System.Collections.Generic.Dictionary<string, GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo>> GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep.PostInfo(Dictionary<String,DeploymentInfo> @globalinfo)
            {

                return base.InvokeMethodAsync<System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo>>(-680118242, new object[] {@globalinfo} );
            }
            
            System.Threading.Tasks.Task GeoOrleans.Runtime.ClusterProtocol.Interfaces.IClusterRep.ReportActivity(string @instance, GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo @instanceinfo, Dictionary<String,ActivityCounts> @counts)
            {

                return base.InvokeMethodAsync<object>(1131637684, new object[] {@instance, @instanceinfo, @counts} );
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.MethodInvokerAttribute("GeoOrleans.Runtime.ClusterProtocol.Interfaces.GeoOrleans.Runtime.ClusterProtocol." +
        "Interfaces.IClusterRep", -2017674181)]
    internal class ClusterRepMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        
        int global::Orleans.CodeGeneration.IGrainMethodInvoker.InterfaceId
        {
            get
            {
                return -2017674181;
            }
        }
        
        global::System.Threading.Tasks.Task<object> global::Orleans.CodeGeneration.IGrainMethodInvoker.Invoke(global::Orleans.Runtime.IAddressable grain, int interfaceId, int methodId, object[] arguments)
        {

            try
            {                    if (grain == null) throw new System.ArgumentNullException("grain");
                switch (interfaceId)
                {
                    case -2017674181:  // IClusterRep
                        switch (methodId)
                        {
                            case -1181674556: 
                                return ((IClusterRep)grain).GetGlobalInfo().ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
                            case -680118242: 
                                return ((IClusterRep)grain).PostInfo((System.Collections.Generic.Dictionary<String,DeploymentInfo>)arguments[0]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
                            case 1131637684: 
                                return ((IClusterRep)grain).ReportActivity((String)arguments[0], (InstanceInfo)arguments[1], (System.Collections.Generic.Dictionary<String,ActivityCounts>)arguments[2]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)null; });
                            default: 
                            throw new NotImplementedException("interfaceId="+interfaceId+",methodId="+methodId);
                        }case 1928988877:  // IGrainWithIntegerKey
                        switch (methodId)
                        {
                            default: 
                            throw new NotImplementedException("interfaceId="+interfaceId+",methodId="+methodId);
                        }
                    default:
                        throw new System.InvalidCastException("interfaceId="+interfaceId);
                }
            }
            catch(Exception ex)
            {
                var t = new System.Threading.Tasks.TaskCompletionSource<object>();
                t.SetException(ex);
                return t.Task;
            }
        }
        
        public static string GetMethodName(int interfaceId, int methodId)
        {

            switch (interfaceId)
            {
                
                case -2017674181:  // IClusterRep
                    switch (methodId)
                    {
                        case -1181674556:
                            return "GetGlobalInfo";
                    case -680118242:
                            return "PostInfo";
                    case 1131637684:
                            return "ReportActivity";
                    
                        default: 
                            throw new NotImplementedException("interfaceId="+interfaceId+",methodId="+methodId);
                    }
                case 1928988877:  // IGrainWithIntegerKey
                    switch (methodId)
                    {
                        
                        default: 
                            throw new NotImplementedException("interfaceId="+interfaceId+",methodId="+methodId);
                    }

                default:
                    throw new System.InvalidCastException("interfaceId="+interfaceId);
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.RegisterSerializerAttribute()]
    internal class GeoOrleans_Runtime_ClusterProtocol_Interfaces_DeploymentInfoSerialization
    {
        
        static GeoOrleans_Runtime_ClusterProtocol_Interfaces_DeploymentInfoSerialization()
        {
            Register();
        }
        
        public static object DeepCopier(object original)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo)(original));
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo();
            Orleans.Serialization.SerializationContext.Current.RecordObject(original, result);
            result.Deployment = input.Deployment;
            result.Instances = ((System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo>)(Orleans.Serialization.SerializationManager.DeepCopyInner(input.Instances)));
            result.ResourceAvailability = ((System.Collections.Generic.Dictionary<System.String,System.String>)(Orleans.Serialization.SerializationManager.DeepCopyInner(input.ResourceAvailability)));
            result.Timestamp = input.Timestamp;
            return result;
        }
        
        public static void Serializer(object untypedInput, Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo)(untypedInput));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Deployment, stream, typeof(string));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Instances, stream, typeof(System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo>));
            Orleans.Serialization.SerializationManager.SerializeInner(input.ResourceAvailability, stream, typeof(System.Collections.Generic.Dictionary<System.String,System.String>));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Timestamp, stream, typeof(System.DateTime));
        }
        
        public static object Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo();
            result.Deployment = ((string)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(string), stream)));
            result.Instances = ((System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo>)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(System.Collections.Generic.Dictionary<System.String,GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo>), stream)));
            result.ResourceAvailability = ((System.Collections.Generic.Dictionary<System.String,System.String>)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(System.Collections.Generic.Dictionary<System.String,System.String>), stream)));
            result.Timestamp = ((System.DateTime)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(System.DateTime), stream)));
            return result;
        }
        
        public static void Register()
        {
            global::Orleans.Serialization.SerializationManager.Register(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.DeploymentInfo), DeepCopier, Serializer, Deserializer);
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.RegisterSerializerAttribute()]
    internal class GeoOrleans_Runtime_ClusterProtocol_Interfaces_InstanceInfoSerialization
    {
        
        static GeoOrleans_Runtime_ClusterProtocol_Interfaces_InstanceInfoSerialization()
        {
            Register();
        }
        
        public static object DeepCopier(object original)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo)(original));
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo();
            Orleans.Serialization.SerializationContext.Current.RecordObject(original, result);
            result.Address = input.Address;
            result.Timestamp = input.Timestamp;
            return result;
        }
        
        public static void Serializer(object untypedInput, Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo)(untypedInput));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Address, stream, typeof(string));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Timestamp, stream, typeof(System.DateTime));
        }
        
        public static object Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo();
            result.Address = ((string)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(string), stream)));
            result.Timestamp = ((System.DateTime)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(System.DateTime), stream)));
            return result;
        }
        
        public static void Register()
        {
            global::Orleans.Serialization.SerializationManager.Register(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.InstanceInfo), DeepCopier, Serializer, Deserializer);
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.RegisterSerializerAttribute()]
    internal class GeoOrleans_Runtime_ClusterProtocol_Interfaces_ActivityCountsSerialization
    {
        
        static GeoOrleans_Runtime_ClusterProtocol_Interfaces_ActivityCountsSerialization()
        {
            Register();
        }
        
        public static object DeepCopier(object original)
        {
            return original;
        }
        
        public static void Serializer(object untypedInput, Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ActivityCounts input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.ActivityCounts)(untypedInput));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Fails, stream, typeof(int));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Uses, stream, typeof(int));
        }
        
        public static object Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ActivityCounts result = default(GeoOrleans.Runtime.ClusterProtocol.Interfaces.ActivityCounts);
            result.Fails = ((int)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(int), stream)));
            result.Uses = ((int)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(int), stream)));
            return result;
        }
        
        public static void Register()
        {
            global::Orleans.Serialization.SerializationManager.Register(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.ActivityCounts), DeepCopier, Serializer, Deserializer);
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.RegisterSerializerAttribute()]
    internal class GeoOrleans_Runtime_ClusterProtocol_Interfaces_ResourceInfoSerialization
    {
        
        static GeoOrleans_Runtime_ClusterProtocol_Interfaces_ResourceInfoSerialization()
        {
            Register();
        }
        
        public static object DeepCopier(object original)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo)(original));
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo();
            Orleans.Serialization.SerializationContext.Current.RecordObject(original, result);
            result.Dictionary = input.Dictionary;
            result.Join = input.Join;
            result.Name = input.Name;
            return result;
        }
        
        public static void Serializer(object untypedInput, Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo input = ((GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo)(untypedInput));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Dictionary, stream, typeof(string));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Join, stream, typeof(string));
            Orleans.Serialization.SerializationManager.SerializeInner(input.Name, stream, typeof(string));
        }
        
        public static object Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
        {
            GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo result = new GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo();
            result.Dictionary = ((string)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(string), stream)));
            result.Join = ((string)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(string), stream)));
            result.Name = ((string)(Orleans.Serialization.SerializationManager.DeserializeInner(typeof(string), stream)));
            return result;
        }
        
        public static void Register()
        {
            global::Orleans.Serialization.SerializationManager.Register(typeof(GeoOrleans.Runtime.ClusterProtocol.Interfaces.ResourceInfo), DeepCopier, Serializer, Deserializer);
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
