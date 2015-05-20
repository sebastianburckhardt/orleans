using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrains
{
    public class BaseGrain : GrainBase, IBase
    {
        public Task<bool> Foo()
        {
            return Task.FromResult(true);
        }
    }

    public class DerivedFromBaseGrain : GrainBase, IDerivedFromBase
    {
        public Task<bool> Bar()
        {
            return Task.FromResult(true);
        }

        public Task<bool> Foo()
        {
            return Task.FromResult(false);
        }
    }

    public class BaseGrain1 : GrainBase, IBase1
    {
        public Task<bool> Foo()
        {
            return Task.FromResult(false);
        }
    }

    public class BaseGrain1And2 : GrainBase, IBase3, IBase2
    {
        public Task<bool> Foo()
        {
            return Task.FromResult(false);
        }

        public Task<bool> Bar()
        {
            return Task.FromResult(true);
        }
    }

    public class Base4 : GrainBase, IBase4
    {
        public Task<bool> Foo()
        {
            return Task.FromResult(false);
        }
    }

    public class Base4_ : GrainBase, IBase4
    {
        public Task<bool> Foo()
        {
            return Task.FromResult(true);
        }
    }
}
