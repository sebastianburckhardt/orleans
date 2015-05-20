using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace UnitTestGrains
{
    public interface IBase : IGrain
    {
        Task<bool> Foo();
    }

    public interface IDerivedFromBase : IBase
    {
        Task<bool> Bar();
    }

    public interface IBase1 : IGrain
    {
        Task<bool> Foo();
    }

    public interface IBase2 : IGrain
    {
        Task<bool> Bar();
    }

    public interface IBase3 : IGrain
    {
        Task<bool> Foo();
    }

    public interface IBase4 : IGrain
    {
        Task<bool> Foo();
    }
}
