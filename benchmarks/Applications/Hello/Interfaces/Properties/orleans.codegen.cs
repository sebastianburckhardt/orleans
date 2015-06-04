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

namespace Hello.Interfaces
{
    using System;
    using System.Net;
    using System.Runtime.Serialization;
    using System.Runtime.Serialization.Formatters.Binary;
    using System.IO;
    using System.Collections.Generic;
    using Orleans;
    using Orleans.Runtime;
    using System.Collections;
    
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    public class TCPReceiverGrainFactory
    {
        

                        public static Hello.Interfaces.ITCPReceiverGrain GetGrain(long primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPReceiverGrain), 607480204, primaryKey));
                        }

                        public static Hello.Interfaces.ITCPReceiverGrain GetGrain(long primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPReceiverGrain), 607480204, primaryKey, grainClassNamePrefix));
                        }

                        public static Hello.Interfaces.ITCPReceiverGrain GetGrain(System.Guid primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPReceiverGrain), 607480204, primaryKey));
                        }

                        public static Hello.Interfaces.ITCPReceiverGrain GetGrain(System.Guid primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPReceiverGrain), 607480204, primaryKey, grainClassNamePrefix));
                        }

            public static Hello.Interfaces.ITCPReceiverGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return TCPReceiverGrainReference.Cast(grainRef);
            }
        
        [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
        [System.SerializableAttribute()]
        [global::Orleans.CodeGeneration.GrainReferenceAttribute("Hello.Interfaces.Hello.Interfaces.ITCPReceiverGrain")]
        internal class TCPReceiverGrainReference : global::Orleans.Runtime.GrainReference, global::Orleans.Runtime.IAddressable, Hello.Interfaces.ITCPReceiverGrain
        {
            

            public static Hello.Interfaces.ITCPReceiverGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return (Hello.Interfaces.ITCPReceiverGrain) global::Orleans.Runtime.GrainReference.CastInternal(typeof(Hello.Interfaces.ITCPReceiverGrain), (global::Orleans.Runtime.GrainReference gr) => { return new TCPReceiverGrainReference(gr);}, grainRef, 607480204);
            }
            
            protected internal TCPReceiverGrainReference(global::Orleans.Runtime.GrainReference reference) : 
                    base(reference)
            {
            }
            
            protected internal TCPReceiverGrainReference(SerializationInfo info, StreamingContext context) : 
                    base(info, context)
            {
            }
            
            protected override int InterfaceId
            {
                get
                {
                    return 607480204;
                }
            }
            
            public override string InterfaceName
            {
                get
                {
                    return "Hello.Interfaces.Hello.Interfaces.ITCPReceiverGrain";
                }
            }
            
            [global::Orleans.CodeGeneration.CopierMethodAttribute()]
            public static object _Copier(object original)
            {
                TCPReceiverGrainReference input = ((TCPReceiverGrainReference)(original));
                return ((TCPReceiverGrainReference)(global::Orleans.Runtime.GrainReference.CopyGrainReference(input)));
            }
            
            [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
            public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                TCPReceiverGrainReference input = ((TCPReceiverGrainReference)(original));
                global::Orleans.Runtime.GrainReference.SerializeGrainReference(input, stream, expected);
            }
            
            [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
            public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                return TCPReceiverGrainReference.Cast(((global::Orleans.Runtime.GrainReference)(global::Orleans.Runtime.GrainReference.DeserializeGrainReference(expected, stream))));
            }
            
            public override bool IsCompatible(int interfaceId)
            {
                return (interfaceId == this.InterfaceId);
            }
            
            protected override string GetMethodName(int interfaceId, int methodId)
            {
                return TCPReceiverGrainMethodInvoker.GetMethodName(interfaceId, methodId);
            }
            
            System.Threading.Tasks.Task<string> Hello.Interfaces.ITCPReceiverGrain.listenMessages()
            {

                return base.InvokeMethodAsync<System.String>(1179553192, null );
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.MethodInvokerAttribute("Hello.Interfaces.Hello.Interfaces.ITCPReceiverGrain", 607480204)]
    internal class TCPReceiverGrainMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        
        int global::Orleans.CodeGeneration.IGrainMethodInvoker.InterfaceId
        {
            get
            {
                return 607480204;
            }
        }
        
        global::System.Threading.Tasks.Task<object> global::Orleans.CodeGeneration.IGrainMethodInvoker.Invoke(global::Orleans.Runtime.IAddressable grain, int interfaceId, int methodId, object[] arguments)
        {

            try
            {                    if (grain == null) throw new System.ArgumentNullException("grain");
                switch (interfaceId)
                {
                    case 607480204:  // ITCPReceiverGrain
                        switch (methodId)
                        {
                            case 1179553192: 
                                return ((ITCPReceiverGrain)grain).listenMessages().ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
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
                
                case 607480204:  // ITCPReceiverGrain
                    switch (methodId)
                    {
                        case 1179553192:
                            return "listenMessages";
                    
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
    public class TCPSenderGrainFactory
    {
        

                        public static Hello.Interfaces.ITCPSenderGrain GetGrain(long primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPSenderGrain), -1687327068, primaryKey));
                        }

                        public static Hello.Interfaces.ITCPSenderGrain GetGrain(long primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPSenderGrain), -1687327068, primaryKey, grainClassNamePrefix));
                        }

                        public static Hello.Interfaces.ITCPSenderGrain GetGrain(System.Guid primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPSenderGrain), -1687327068, primaryKey));
                        }

                        public static Hello.Interfaces.ITCPSenderGrain GetGrain(System.Guid primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.ITCPSenderGrain), -1687327068, primaryKey, grainClassNamePrefix));
                        }

            public static Hello.Interfaces.ITCPSenderGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return TCPSenderGrainReference.Cast(grainRef);
            }
        
        [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
        [System.SerializableAttribute()]
        [global::Orleans.CodeGeneration.GrainReferenceAttribute("Hello.Interfaces.Hello.Interfaces.ITCPSenderGrain")]
        internal class TCPSenderGrainReference : global::Orleans.Runtime.GrainReference, global::Orleans.Runtime.IAddressable, Hello.Interfaces.ITCPSenderGrain
        {
            

            public static Hello.Interfaces.ITCPSenderGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return (Hello.Interfaces.ITCPSenderGrain) global::Orleans.Runtime.GrainReference.CastInternal(typeof(Hello.Interfaces.ITCPSenderGrain), (global::Orleans.Runtime.GrainReference gr) => { return new TCPSenderGrainReference(gr);}, grainRef, -1687327068);
            }
            
            protected internal TCPSenderGrainReference(global::Orleans.Runtime.GrainReference reference) : 
                    base(reference)
            {
            }
            
            protected internal TCPSenderGrainReference(SerializationInfo info, StreamingContext context) : 
                    base(info, context)
            {
            }
            
            protected override int InterfaceId
            {
                get
                {
                    return -1687327068;
                }
            }
            
            public override string InterfaceName
            {
                get
                {
                    return "Hello.Interfaces.Hello.Interfaces.ITCPSenderGrain";
                }
            }
            
            [global::Orleans.CodeGeneration.CopierMethodAttribute()]
            public static object _Copier(object original)
            {
                TCPSenderGrainReference input = ((TCPSenderGrainReference)(original));
                return ((TCPSenderGrainReference)(global::Orleans.Runtime.GrainReference.CopyGrainReference(input)));
            }
            
            [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
            public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                TCPSenderGrainReference input = ((TCPSenderGrainReference)(original));
                global::Orleans.Runtime.GrainReference.SerializeGrainReference(input, stream, expected);
            }
            
            [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
            public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                return TCPSenderGrainReference.Cast(((global::Orleans.Runtime.GrainReference)(global::Orleans.Runtime.GrainReference.DeserializeGrainReference(expected, stream))));
            }
            
            public override bool IsCompatible(int interfaceId)
            {
                return (interfaceId == this.InterfaceId);
            }
            
            protected override string GetMethodName(int interfaceId, int methodId)
            {
                return TCPSenderGrainMethodInvoker.GetMethodName(interfaceId, methodId);
            }
            
            System.Threading.Tasks.Task<string> Hello.Interfaces.ITCPSenderGrain.SayHello(string @s)
            {

                return base.InvokeMethodAsync<System.String>(-510773597, new object[] {@s} );
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.MethodInvokerAttribute("Hello.Interfaces.Hello.Interfaces.ITCPSenderGrain", -1687327068)]
    internal class TCPSenderGrainMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        
        int global::Orleans.CodeGeneration.IGrainMethodInvoker.InterfaceId
        {
            get
            {
                return -1687327068;
            }
        }
        
        global::System.Threading.Tasks.Task<object> global::Orleans.CodeGeneration.IGrainMethodInvoker.Invoke(global::Orleans.Runtime.IAddressable grain, int interfaceId, int methodId, object[] arguments)
        {

            try
            {                    if (grain == null) throw new System.ArgumentNullException("grain");
                switch (interfaceId)
                {
                    case -1687327068:  // ITCPSenderGrain
                        switch (methodId)
                        {
                            case -510773597: 
                                return ((ITCPSenderGrain)grain).SayHello((String)arguments[0]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
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
                
                case -1687327068:  // ITCPSenderGrain
                    switch (methodId)
                    {
                        case -510773597:
                            return "SayHello";
                    
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
    public class HelloGrainFactory
    {
        

                        public static Hello.Interfaces.IHelloGrain GetGrain(long primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IHelloGrain), 1640833830, primaryKey));
                        }

                        public static Hello.Interfaces.IHelloGrain GetGrain(long primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IHelloGrain), 1640833830, primaryKey, grainClassNamePrefix));
                        }

                        public static Hello.Interfaces.IHelloGrain GetGrain(System.Guid primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IHelloGrain), 1640833830, primaryKey));
                        }

                        public static Hello.Interfaces.IHelloGrain GetGrain(System.Guid primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IHelloGrain), 1640833830, primaryKey, grainClassNamePrefix));
                        }

            public static Hello.Interfaces.IHelloGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return HelloGrainReference.Cast(grainRef);
            }
        
        [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
        [System.SerializableAttribute()]
        [global::Orleans.CodeGeneration.GrainReferenceAttribute("Hello.Interfaces.Hello.Interfaces.IHelloGrain")]
        internal class HelloGrainReference : global::Orleans.Runtime.GrainReference, global::Orleans.Runtime.IAddressable, Hello.Interfaces.IHelloGrain
        {
            

            public static Hello.Interfaces.IHelloGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return (Hello.Interfaces.IHelloGrain) global::Orleans.Runtime.GrainReference.CastInternal(typeof(Hello.Interfaces.IHelloGrain), (global::Orleans.Runtime.GrainReference gr) => { return new HelloGrainReference(gr);}, grainRef, 1640833830);
            }
            
            protected internal HelloGrainReference(global::Orleans.Runtime.GrainReference reference) : 
                    base(reference)
            {
            }
            
            protected internal HelloGrainReference(SerializationInfo info, StreamingContext context) : 
                    base(info, context)
            {
            }
            
            protected override int InterfaceId
            {
                get
                {
                    return 1640833830;
                }
            }
            
            public override string InterfaceName
            {
                get
                {
                    return "Hello.Interfaces.Hello.Interfaces.IHelloGrain";
                }
            }
            
            [global::Orleans.CodeGeneration.CopierMethodAttribute()]
            public static object _Copier(object original)
            {
                HelloGrainReference input = ((HelloGrainReference)(original));
                return ((HelloGrainReference)(global::Orleans.Runtime.GrainReference.CopyGrainReference(input)));
            }
            
            [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
            public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                HelloGrainReference input = ((HelloGrainReference)(original));
                global::Orleans.Runtime.GrainReference.SerializeGrainReference(input, stream, expected);
            }
            
            [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
            public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                return HelloGrainReference.Cast(((global::Orleans.Runtime.GrainReference)(global::Orleans.Runtime.GrainReference.DeserializeGrainReference(expected, stream))));
            }
            
            public override bool IsCompatible(int interfaceId)
            {
                return (interfaceId == this.InterfaceId);
            }
            
            protected override string GetMethodName(int interfaceId, int methodId)
            {
                return HelloGrainMethodInvoker.GetMethodName(interfaceId, methodId);
            }
            
            System.Threading.Tasks.Task<string> Hello.Interfaces.IHelloGrain.Hello(string @s)
            {

                return base.InvokeMethodAsync<System.String>(532006314, new object[] {@s} );
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.MethodInvokerAttribute("Hello.Interfaces.Hello.Interfaces.IHelloGrain", 1640833830)]
    internal class HelloGrainMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        
        int global::Orleans.CodeGeneration.IGrainMethodInvoker.InterfaceId
        {
            get
            {
                return 1640833830;
            }
        }
        
        global::System.Threading.Tasks.Task<object> global::Orleans.CodeGeneration.IGrainMethodInvoker.Invoke(global::Orleans.Runtime.IAddressable grain, int interfaceId, int methodId, object[] arguments)
        {

            try
            {                    if (grain == null) throw new System.ArgumentNullException("grain");
                switch (interfaceId)
                {
                    case 1640833830:  // IHelloGrain
                        switch (methodId)
                        {
                            case 532006314: 
                                return ((IHelloGrain)grain).Hello((String)arguments[0]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
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
                
                case 1640833830:  // IHelloGrain
                    switch (methodId)
                    {
                        case 532006314:
                            return "Hello";
                    
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
    public class ReplicatedHelloGrainFactory
    {
        

                        public static Hello.Interfaces.IReplicatedHelloGrain GetGrain(long primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IReplicatedHelloGrain), 1844412741, primaryKey));
                        }

                        public static Hello.Interfaces.IReplicatedHelloGrain GetGrain(long primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IReplicatedHelloGrain), 1844412741, primaryKey, grainClassNamePrefix));
                        }

                        public static Hello.Interfaces.IReplicatedHelloGrain GetGrain(System.Guid primaryKey)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IReplicatedHelloGrain), 1844412741, primaryKey));
                        }

                        public static Hello.Interfaces.IReplicatedHelloGrain GetGrain(System.Guid primaryKey, string grainClassNamePrefix)
                        {
                            return Cast(global::Orleans.CodeGeneration.GrainFactoryBase.MakeGrainReferenceInternal(typeof(Hello.Interfaces.IReplicatedHelloGrain), 1844412741, primaryKey, grainClassNamePrefix));
                        }

            public static Hello.Interfaces.IReplicatedHelloGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return ReplicatedHelloGrainReference.Cast(grainRef);
            }
        
        [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
        [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
        [System.SerializableAttribute()]
        [global::Orleans.CodeGeneration.GrainReferenceAttribute("Hello.Interfaces.Hello.Interfaces.IReplicatedHelloGrain")]
        internal class ReplicatedHelloGrainReference : global::Orleans.Runtime.GrainReference, global::Orleans.Runtime.IAddressable, Hello.Interfaces.IReplicatedHelloGrain
        {
            

            public static Hello.Interfaces.IReplicatedHelloGrain Cast(global::Orleans.Runtime.IAddressable grainRef)
            {
                
                return (Hello.Interfaces.IReplicatedHelloGrain) global::Orleans.Runtime.GrainReference.CastInternal(typeof(Hello.Interfaces.IReplicatedHelloGrain), (global::Orleans.Runtime.GrainReference gr) => { return new ReplicatedHelloGrainReference(gr);}, grainRef, 1844412741);
            }
            
            protected internal ReplicatedHelloGrainReference(global::Orleans.Runtime.GrainReference reference) : 
                    base(reference)
            {
            }
            
            protected internal ReplicatedHelloGrainReference(SerializationInfo info, StreamingContext context) : 
                    base(info, context)
            {
            }
            
            protected override int InterfaceId
            {
                get
                {
                    return 1844412741;
                }
            }
            
            public override string InterfaceName
            {
                get
                {
                    return "Hello.Interfaces.Hello.Interfaces.IReplicatedHelloGrain";
                }
            }
            
            [global::Orleans.CodeGeneration.CopierMethodAttribute()]
            public static object _Copier(object original)
            {
                ReplicatedHelloGrainReference input = ((ReplicatedHelloGrainReference)(original));
                return ((ReplicatedHelloGrainReference)(global::Orleans.Runtime.GrainReference.CopyGrainReference(input)));
            }
            
            [global::Orleans.CodeGeneration.SerializerMethodAttribute()]
            public static void _Serializer(object original, global::Orleans.Serialization.BinaryTokenStreamWriter stream, System.Type expected)
            {
                ReplicatedHelloGrainReference input = ((ReplicatedHelloGrainReference)(original));
                global::Orleans.Runtime.GrainReference.SerializeGrainReference(input, stream, expected);
            }
            
            [global::Orleans.CodeGeneration.DeserializerMethodAttribute()]
            public static object _Deserializer(System.Type expected, global::Orleans.Serialization.BinaryTokenStreamReader stream)
            {
                return ReplicatedHelloGrainReference.Cast(((global::Orleans.Runtime.GrainReference)(global::Orleans.Runtime.GrainReference.DeserializeGrainReference(expected, stream))));
            }
            
            public override bool IsCompatible(int interfaceId)
            {
                return (interfaceId == this.InterfaceId);
            }
            
            protected override string GetMethodName(int interfaceId, int methodId)
            {
                return ReplicatedHelloGrainMethodInvoker.GetMethodName(interfaceId, methodId);
            }
            
            System.Threading.Tasks.Task Hello.Interfaces.IReplicatedHelloGrain.Hello(string @msg)
            {

                return base.InvokeMethodAsync<object>(532006314, new object[] {@msg} );
            }
            
            System.Threading.Tasks.Task<string> Hello.Interfaces.IReplicatedHelloGrain.GetTopMessagesAsync(bool @syncGlobal)
            {

                return base.InvokeMethodAsync<System.String>(-284572065, new object[] {@syncGlobal} );
            }
        }
    }
    
    [System.CodeDom.Compiler.GeneratedCodeAttribute("Orleans-CodeGenerator", "1.0.8.0")]
    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverageAttribute()]
    [global::Orleans.CodeGeneration.MethodInvokerAttribute("Hello.Interfaces.Hello.Interfaces.IReplicatedHelloGrain", 1844412741)]
    internal class ReplicatedHelloGrainMethodInvoker : global::Orleans.CodeGeneration.IGrainMethodInvoker
    {
        
        int global::Orleans.CodeGeneration.IGrainMethodInvoker.InterfaceId
        {
            get
            {
                return 1844412741;
            }
        }
        
        global::System.Threading.Tasks.Task<object> global::Orleans.CodeGeneration.IGrainMethodInvoker.Invoke(global::Orleans.Runtime.IAddressable grain, int interfaceId, int methodId, object[] arguments)
        {

            try
            {                    if (grain == null) throw new System.ArgumentNullException("grain");
                switch (interfaceId)
                {
                    case 1844412741:  // IReplicatedHelloGrain
                        switch (methodId)
                        {
                            case 532006314: 
                                return ((IReplicatedHelloGrain)grain).Hello((String)arguments[0]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)null; });
                            case -284572065: 
                                return ((IReplicatedHelloGrain)grain).GetTopMessagesAsync((Boolean)arguments[0]).ContinueWith(t => {if (t.Status == System.Threading.Tasks.TaskStatus.Faulted) throw t.Exception; return (object)t.Result; });
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
                
                case 1844412741:  // IReplicatedHelloGrain
                    switch (methodId)
                    {
                        case 532006314:
                            return "Hello";
                    case -284572065:
                            return "GetTopMessagesAsync";
                    
                        default: 
                            throw new NotImplementedException("interfaceId="+interfaceId+",methodId="+methodId);
                    }

                default:
                    throw new System.InvalidCastException("interfaceId="+interfaceId);
            }
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
