using System;
using System.Threading.Tasks;
using Orleans;
using System.Threading;
using SimpleGrain;
using UnitTestGrains;

namespace GrainContextTestGrain
{
    public class GrainContextTestGrain : GrainBase, IGrainContextTestGrain
    {

        public Task<string> GetRuntimeInstanceId()
        {
            return Task.FromResult(this.RuntimeIdentity);
        }

        public Task<string> GetGrainContext_Immediate()
        {
            return Task.FromResult(AsyncCompletion.Context.ToString());
        }

        public Task<string> GetGrainContext_ContinueWithVoid()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            ISimpleGrain dependency = SimpleGrainFactory.GetGrain((new Random()).Next());
            AsyncCompletion.FromTask(dependency.SetA(1)).ContinueWith(() =>
                {
                    resolver.Resolve(AsyncCompletion.Context.ToString());
                }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_DoubleContinueWith()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            ISimpleGrain dependency = SimpleGrainFactory.GetGrain((new Random()).Next());
            AsyncCompletion.FromTask(dependency.SetA(2)).ContinueWith(() =>
            {
                AsyncValue.FromTask(dependency.GetA()).ContinueWith((int a) =>
                {
                    try
                    {
                        resolver.Resolve(a.ToString() + AsyncCompletion.Context.ToString());
                    }
                    catch (Exception exc)
                    {
                        resolver.Break(exc);
                    }
                }).Ignore();
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }
        
        public Task<string> GetGrainContext_StartNew_Void()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            AsyncCompletion.StartNew(()=>
            {
                resolver.Resolve( AsyncCompletion.Context.ToString() );
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_StartNew_AsyncCompletion()
        {
            return AsyncValue<string>.StartNew(() =>
            {
                return AsyncCompletion.Context.ToString();
            }).AsTask();
        }

        public Task<string> GetGrainContext_StartNew_AsyncValue()
        {
            return AsyncValue<string>.StartNew(() =>
            {
                return new AsyncValue<string>(AsyncCompletion.Context.ToString());
            }).AsTask();
        }

        //public Task<string> GetGrainContext_StartNew_AsyncValueAndArgument(string prefixString)
        //{
        //    return AsyncValue<string>.StartNew(prefix =>
        //    {
        //        return prefix.ToString() + AsyncCompletion.Context.ToString();
        //    }, prefixString);
        //}

        public Task<string> GetGrainContext_ContinueWithException()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxBError(1, 2));
  
            promise.ContinueWith(() =>
            {
                resolver.Break(new ApplicationException("Exception block is expected to be called instead of the success continuation."));
            }, exc =>
            {
                try
                {
                    resolver.Resolve(AsyncCompletion.Context.ToString());
                }
                catch (Exception)
                {
                    resolver.Break( new ApplicationException("No grain context."));
                }
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueException()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxBError(1, 2));
            bool doThrow = true;
            promise.ContinueWith<string>(ivalue =>
            {
                if (doThrow)
                {
                    throw new ApplicationException("Exception block is expected to be called instead of the success continuation.");
                }
                return "";
            }, exc =>
            {
                try
                {
                    resolver.Resolve( AsyncCompletion.Context.ToString() );
                }
                catch (Exception)
                {
                    resolver.Break(new ApplicationException("No grain context."));
                }
                return "";
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueAction()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxB(1, 2));

            promise.ContinueWith(ivalue =>
            {
                try
                {
                    resolver.Resolve(AsyncCompletion.Context.ToString());
                }
                catch (Exception)
                {
                    resolver.Break(new ApplicationException("No grain context."));
                }
            }, exc =>
            {
                resolver.Break(new ApplicationException("Continuation is expected to be called instead of the exception block."));
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueActionException()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxBError(1, 2));

            promise.ContinueWith(ivalue =>
            {
                throw new ApplicationException("Exception block is expected to be called instead of the success continuation.");
            }, exc =>
            {
                try
                {
                    resolver.Resolve(AsyncCompletion.Context.ToString());
                }
                catch (Exception)
                {
                    resolver.Break(new ApplicationException("No grain context."));
                }
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueFunction()
        {
            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxB(1, 2));

            return promise.ContinueWith<string>(ivalue =>
            {
                try
                {
                    return AsyncCompletion.Context.ToString();
                }
                catch (Exception)
                {
                   throw new ApplicationException("No grain context.");
                }
            }).AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueFunctionException()
        {
            AsyncValueResolver<string> resolver = new AsyncValueResolver<string>();

            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue.FromTask(errorGrain.GetAxBError(1, 2));
            bool doThrow = true;
            promise.ContinueWith<string>((int ivalue) =>
            {
                if (doThrow)
                {
                    throw new ApplicationException("Exception block is expected to be called instead of the success continuation.");
                }
                return "";
            }, exc =>
            {
                try
                {
                    resolver.Resolve(AsyncCompletion.Context.ToString());
                }
                catch (Exception)
                {
                    resolver.Break(new ApplicationException("No grain context."));
                }
                return "";
            }).Ignore();

            return resolver.AsyncValue.AsTask();
        }

        public Task<string> GetGrainContext_ContinueWithValueFunctionPromise()
        {
            IErrorGrain errorGrain = ErrorGrainFactory.GetGrain((new Random()).Next());
            AsyncValue<int> promise = AsyncValue<int>.StartNew(() => { Thread.Sleep(500); return 1; }); // errorGrain.GetAxB(1, 2);

            return promise.ContinueWith<string>(ivalue =>
            {
                return AsyncValue<string>.StartNew(() =>
                    {
                        try
                        {
                            return AsyncCompletion.Context.ToString();
                        }
                        catch (Exception)
                        {
                            throw new ApplicationException("No grain context.");
                        }
                    });
            }).AsTask();
        }
    }
}
