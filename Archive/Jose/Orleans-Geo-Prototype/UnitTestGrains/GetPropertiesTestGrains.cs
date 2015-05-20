using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTestGrainInterfaces;
using Orleans;

namespace UnitTestGrains
{
    public interface IComplexGrainState : IGrainState
    {
        ComplicatedTestType<int> FldInt { get; set; }
        ComplicatedTestType<string> FldStr { get; set; }
    }
    
    public class ComplexGrain : GrainBase<IComplexGrainState>, IComplexGrain
    {
        public Task SeedFldInt(int i)
        {
            State.FldInt.InitWithSeed(i);
            return TaskDone.Done;
        }
        public Task SeedFldStr(string s)
        {
            State.FldStr.InitWithSeed(s);
            return TaskDone.Done;
        }
        public override Task ActivateAsync()
        {
            State.FldInt = new ComplicatedTestType<int>();
            State.FldStr = new ComplicatedTestType<string>();
            return TaskDone.Done;
        }

        public Task<ComplicatedTestType<int>> FldInt
        {
            get { return Task.FromResult(State.FldInt); }
        }

        public Task<ComplicatedTestType<string>> FldStr
        {
            get { return Task.FromResult(State.FldStr); }
        }
    }
    public interface ILinkedListGrainState : IGrainState
    {
        ILinkedListGrain Next { get; set; }
        int Value { get; set; }
    }
    public class LinkedListGrain : GrainBase<ILinkedListGrainState>, ILinkedListGrain
    {
        public Task SetValue(int v)
        {
            State.Value = v;
            return TaskDone.Done;
        }
        public Task SetNext(ILinkedListGrain next)
        {
            State.Next = next;
            return TaskDone.Done;
        }

        public Task<ILinkedListGrain> Next
        {
            get { return Task.FromResult(State.Next); }
        }

        public Task<int> Value
        {
            get { return Task.FromResult(State.Value); }
        }
    }

}
