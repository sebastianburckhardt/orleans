using System;

namespace Orleans
{
    /// <summary>
    /// This class provides useful utility functions for asynchronous programming.
    /// </summary>
    internal class AsyncLoop
    {
        /// <summary>
        /// Executes a function for the specified number of times.
        /// Each iteration runs asynchronously as a continuation of the previous iteration.
        /// The first iteration is started in-line.
        /// </summary>
        /// <param name="numIterations">The number of iterations to execute.
        /// If this is 0 or negative, then no iterations will be executed and the resulting promise
        /// will be resolved when thisd method returns.</param>
        /// <param name="function">The function to iterate.
        /// This function receives the current iteration count, starting at 0, as its argument.
        /// It must return an <c>AsyncCompletion</c>, but need not run in a new turn itself.
        /// If any execution of the function throws an exception or returns a promise that is broken,
        /// the iteration stops and the promise returned from this method is broken.
        /// </param>
        /// <returns>A promise that is resolved when all of the iterations have completed.</returns>
        public static AsyncCompletion For(int numIterations, Func<int, AsyncCompletion> function)
        {
            return While((int iteration) => { return iteration < numIterations; }, function);
        }

        /// <summary>
        /// Executes a function while a predicate is true.
        /// Each iteration runs asynchronously as a continuation of the previous iteration.
        /// The first iteration is started in-line.
        /// </summary>
        /// <param name="predicate">A function that takes the current iteration count, starting at 0, 
        /// and returns a (prompt) boolean indicating whether or not iteration should continue.
        /// If the predicate returns <c>false</c> for 0, then no iterations will be executed and the resulting
        /// promise will be resolved when thisd method returns.
        /// If the predicate throws an exception, then the iteration stops and the promise returned from this 
        /// method is broken.</param>
        /// <param name="function">The function to iterate.
        /// This function receives the current iteration count, starting at 0, as its argument.
        /// It must return an <c>AsyncCompletion</c>, but need not run in a new turn itself.
        /// If any execution of the function throws an exception or returns a promise that is broken,
        /// the iteration stops and the promise returned from this method is broken.
        /// </param>
        /// <returns>A promise that is resolved when all of the iterations have completed.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncCompletion While(Func<int, bool> predicate, Func<int, AsyncCompletion> function)
        {
            AsyncCompletionResolver loopResolver = new AsyncCompletionResolver();
            Action<Exception> breaker = (Exception ex) => loopResolver.Break(ex);
            int iteration = 0;
            Action loop = null;

            loop = () =>
            {
                bool doAnotherLoop = false;
                try
                {
                    doAnotherLoop = predicate(iteration);
                }
                catch (Exception ex)
                {
                    loopResolver.Break(ex);
                    return;
                }
                if (doAnotherLoop)
                {
                    try
                    {
                        function(iteration++).ContinueWith(loop, breaker).Ignore();
                    }
                    catch (Exception ex)
                    {
                        // function can actually throw, since we do not assume that it runs in a new Task.
                        loopResolver.Break(ex);
                    }
                }
                else
                {
                    loopResolver.Resolve();
                }
            };
            loop();
            return loopResolver.AsyncCompletion;
        }

        public static AsyncValue<T> While<T>(Func<Tuple<int, T>, bool> predicate, Func<int, AsyncValue<T>> function)
        {
            return 
                Reduce(
                    predicate, 
                    tup =>
                        {
                            var index = tup.Item1;
                            return function(index);
                        });
        }

        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static AsyncValue<T> Reduce<T>(Func<Tuple<int, T>, bool> predicate, Func<Tuple<int, T>, AsyncValue<T>> function)
        {
            int iteration = 0;
            // [mlr] separating the initialization from the assignment here quiets an unecessary warning when
            // we pass `loop` into Func().ContinueWith.
            Func<T, AsyncValue<T>> loop = null;
            loop =
                (T accum) =>
                {
                    try
                    {
                        var tup = Tuple.Create(iteration++, accum);
                        bool recurse = predicate(tup);
                        if (recurse)
                            return function(tup).ContinueWith(loop);
                        else
                            return accum;
                    }
                    catch (Exception ex)
                    {
                        return AsyncValue<T>.GenerateFromException(ex);
                    }
                };
            return loop(default(T));
        }

        // This is Midori promises implementaion, which I used as a basis for our implementation.
        // Notice a small bug in the below implementation:
        // If the predicate function throws an exception in the first time it is invoked, the main Pvoid Loop function throws instead of returning a broken promise.

        // /// <summary>
        // /// This method implements the asynchronous equivalent of a while loop.
        // /// The method takes a single parameter, which is a delegate that returns a PVoid.
        // /// The method immediately calls the delegate, and retains the promise returned
        // /// from it.  When that promise resolves, then the delegate will be invoked again,
        // /// causing the "loop" to continue.
        // /// </summary>
        // ///
        // /// <param name="predicate">
        // /// A predicate that controls whether or not the loop should continue to run.
        // /// Before the 'action' delegate is invoked, this 'predicate' is invoked.
        // /// If its return value is false, then the loop terminates.
        // /// </param>
        // /// <param name="action">
        // /// The delegate to invoke.  This will be invoked many times.
        // /// </param>
        // /// <returns>
        // /// A promise that will be resolved when the loop terminates.  The loop terminates
        // /// when the promise returned by any invocation of the delegate breaks, or when any
        // /// invocation of the continuePredicate delegate returns false. If the loop terminates
        // /// because of the delegate invocations breaks, then the return promise is also broken.
        // /// </returns>
        // /// <remarks>The int parameter to <paramref name="predicate"/> and <paramref name="action"/> contains an iteration counter that is incremented from zero.</remarks>
        //public static PVoid Loop(Func<int, bool> predicate, Func<int, PVoid> action)
        //{
        //    PVoid loopPromise;
        //    var loopResolver = new PVoid.Resolver(out loopPromise);
        //    Action<Exception> breaker = (Exception ex) => loopResolver.Break(ex);
        //    int iteration = 0;
        //    Action loop = null;

        //    loop = () => {
        //        if (predicate(iteration)) {
        //            try {
        //                PVoid.WhenOnly(action(iteration++), loop, breaker);
        //            }
        //            catch (Exception ex) {
        //                loopResolver.Break(ex);
        //            }
        //        }
        //        else {
        //            loopResolver.Resolve();
        //        }
        //    };

        //    loop();

        //    return loopPromise;
        //}

        // This implementation uses 1 Task per loop iteration, plus one task for the whole Loop as AsyncCompletionResolver (which is not a real thread).
        // AG -- This implementation leaves a bunch of unobserved promises dangling, and so will cause the UnobservedException event to be raised
        // if any execution of action throws an exception. For this reason, it should not be used.
        //public static AsyncCompletion While2(Func<int, bool> predicate, Action<int> action) 
        //{
        //    AsyncCompletionResolver loopResolver = new AsyncCompletionResolver();
        //    int iteration = 0;
        //    Action internalLoop = null;

        //    // Both internalAction and internalLoop never throw.
        //    Action<object> internalAction = (object obj) =>
        //    {
        //        try
        //        {
        //            action((int)obj);
        //        }
        //        catch (Exception ex)
        //        {
        //            loopResolver.Break(ex);
        //            return;
        //        }
        //        internalLoop();
        //    };

        //    internalLoop = () =>
        //    {
        //        bool doAnotherLoop = false;
        //        try
        //        {
        //            doAnotherLoop = predicate(iteration);
        //        }
        //        catch (Exception ex)
        //        {
        //            loopResolver.Break(ex);
        //            return;
        //        }
        //        if (doAnotherLoop)
        //        {
        //            AsyncCompletion oneIteration = AsyncCompletion.StartNew(internalAction, (object)(iteration++));
        //            // We do not need to Wait explicitely on oneIteration. However, it may be considered a bad practice since we do not explicitely garbage collect Tasks.
        //            // In theory it means we can finish executing the While loop and still have multiple orphan AsyncCompletions that are still consideded not finished.
        //            // oneIteration.Wait();
        //        }
        //        else
        //        {
        //            loopResolver.Resolve();
        //        }
        //    };

        //    internalLoop();
        //    return loopResolver.AsyncCompletion;
        //}


        // An altenative implementation of the While using ContinueWith(internalLoop);
        // This implementation is less efficient, since it uses 2 Tasks per loop iteration: one task for every interation action and one task to schedule a next iteration.
        // AG -- This implementation leaves no unobserved promises around, which is good, but uses an explicit Wait, which is bad -- very bad, in fact, because each
        // Wait will wind up waiting on the next iteration, so that iteration 0 waits on iteration 1 which waits on iteration 2 which waits on iteration 3, etc., and each
        // waiting iteration uses up a thread.
        //public static AsyncCompletion While3(Func<int, bool> predicate, Func<int, AsyncCompletion> function)
        //{
        //    AsyncCompletionResolver loopResolver = new AsyncCompletionResolver();
        //    int iteration = 0;
        //    Action internalLoop = null;

        //    Func<object, AsyncCompletion> internalFunction = (object obj) =>
        //    {
        //        return function((int)obj);
        //    };

        //    internalLoop = () =>
        //    {
        //        bool doAnotherLoop = false;
        //        try
        //        {
        //            doAnotherLoop = predicate(iteration);
        //        }
        //        catch (Exception ex)
        //        {
        //            loopResolver.Break(ex);
        //            return;
        //        }
        //        if (doAnotherLoop)
        //        {
        //            AsyncValue<AsyncCompletion> oneIteration = AsyncValue<AsyncCompletion>.StartNew(internalFunction, (object)(iteration++));
        //            AsyncCompletion oneIterationResult = oneIteration.ContinueWith<AsyncCompletion>( (AsyncCompletion ac) => { return ac; });
        //            AsyncCompletion nextIteration = oneIterationResult.ContinueWith(internalLoop);

        //            // An altenative implementation of the While does not call internalLoop from inside internalAction and instead does oneIteration.ContinueWith(internalLoop);
        //            //AsyncCompletion nextIteration = oneIteration.ContinueWith(internalLoop);
        //            // Since currently we do not allow to pass exception handling code to ContinueWith (like Midori promises do), we need to explicitely Wait on it.
        //            try
        //            {
        //                nextIteration.Wait();
        //            }
        //            catch (Exception ex)
        //            {
        //                loopResolver.Break(ex);
        //                return;
        //            }                   
        //        }
        //        else
        //        {
        //            loopResolver.Resolve();
        //        }
        //    };
        //    internalLoop();
        //    return loopResolver.AsyncCompletion;
        //}
    }
}



