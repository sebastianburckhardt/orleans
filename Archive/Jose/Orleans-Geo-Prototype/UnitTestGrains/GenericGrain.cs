using System.Collections.Generic;
using System.Threading.Tasks;
using Orleans;
using UnitTestGrainInterfaces.Generic;

namespace GenericTestGrains
{
    public interface ISimpleGenericGrainState<T> : IGrainState
    {
        T A { get; set; }
        T B { get; set; }
    }

    public class SimpleGenericGrain<T> : GrainBase<ISimpleGenericGrainState<T>>, ISimpleGenericGrain<T>
    {
        public Task<T> GetA()
        {
            return A;
        }

        public Task SetA(T a)
        {
            State.A = a;
            return TaskDone.Done;
        }

        public Task SetB(T b)
        {
            State.B = b;
            return TaskDone.Done;
        }

        public Task<string> GetAxB()
        {
            string retValue = State.A.ToString() + "x" + State.B.ToString();
            return Task.FromResult(retValue);
        }

        public Task<string> GetAxB(T a, T b)
        {
            return Task.FromResult(a.ToString() + "x" + b.ToString());
        }

        public Task<T> A
        {
            get { return Task.FromResult(State.A); }
        }

        public Task<T> B
        {
            get { return Task.FromResult(State.B); }
        }
    }

    public interface ISimpleGenericGrainUState<U> : IGrainState
    {
        U A { get; set; }
        U B { get; set; }
    }

    public class SimpleGenericGrainU<U> : GrainBase<ISimpleGenericGrainUState<U>>, ISimpleGenericGrainU<U>
    {
        public Task<U> GetA()
        {
            return Task.FromResult(State.A);
        }

        public Task SetA(U a)
        {
            State.A = a;
            return TaskDone.Done;
        }

        public Task SetB(U b)
        {
            State.B = b;
            return TaskDone.Done;
        }

        public Task<string> GetAxB()
        {
            return Task.FromResult(State.A.ToString() + "x" + State.B.ToString());
        }

        public Task<string> GetAxB(U a, U b)
        {
            return Task.FromResult(a.ToString() + "x" + b.ToString());
        }

        public Task<U> A
        {
            get { return Task.FromResult( State.A); }
        }
    }

    public interface ISimpleGenericGrain2State<T, U> : IGrainState
    {
        T A { get; set; }
        U B { get; set; }
    }
    public class SimpleGenericGrain2<T, U> : GrainBase<ISimpleGenericGrain2State<T, U>>, ISimpleGenericGrain2<T, U>
    {
        public Task<T> GetA()
        {
            return Task.FromResult(State.A);
        }

        public Task SetA(T a)
        {
            State.A = a;
            return TaskDone.Done;
        }

        public Task SetB(U b)
        {
            State.B = b;
            return TaskDone.Done;
        }

        public Task<string> GetAxB()
        {
            return Task.FromResult(State.A.ToString() + "x" + State.B.ToString());
        }

        public Task<string> GetAxB(T a, U b)
        {
            return Task.FromResult(a.ToString() + "x" + b.ToString());
        }

        public Task<T> A
        {
            get { return Task.FromResult(State.A); }
        }
    }

    //public class GenericGrainWithNoProperties<T> : GenericGrainWithNoPropertiesBase<T>
    public class GenericGrainWithNoProperties<T> : GrainBase, IGenericGrainWithNoProperties<T>
    {
        public Task<string> GetAxB(T a, T b)
        {
            return Task.FromResult(a + "x" + b);
        }
    }
    public class GrainWithNoProperties : GrainBase, IGrainWithNoProperties
    {
        public Task<string> GetAxB(int a, int b)
        {
            return Task.FromResult(a + "x" + b);
        }
    }
    public interface IGrainWithListFieldsState : IGrainState
    {
        IList<string> Items { get; set; }
    }

    public class GrainWithListFields : GrainBase<IGrainWithListFieldsState>, IGrainWithListFields
    {
        public override Task ActivateAsync()
        {
            if (State.Items == null)
                State.Items = new List<string>();
            return base.ActivateAsync();
        }

        public Task AddItem(string item)
        {
            State.Items.Add(item);
            return TaskDone.Done;
        }

        public Task<IList<string>> GetItems()
        {
            return Task.FromResult((State.Items));
        }

        public Task<IList<string>> Items
        {
            get { return GetItems(); }
        }
    }

    public interface IGenericGrainWithListFieldsState<T> : IGrainState
    {
        IList<T> Items { get; set; }
    }

    public class GenericGrainWithListFields<T> : GrainBase<IGenericGrainWithListFieldsState<T>>, IGenericGrainWithListFields<T>
    {
        public override Task ActivateAsync()
        {
            if (State.Items == null)
                State.Items = new List<T>();

            return base.ActivateAsync();
        }

        public Task AddItem(T item)
        {
            State.Items.Add(item);
            return TaskDone.Done;
        }

        public Task<IList<T>> GetItems()
        {
            return Task.FromResult(State.Items);
        }

        public Task<IList<T>> Items
        {
            get { return GetItems(); }
        }
    }

    public interface IGenericReaderWriterState<T> : IGrainState
    {
        T Value { get; set; }
    }
    

    public interface IGenericReader2State<TOne, TTwo> : IGrainState
    {
        TOne Value1 { get; set; }
        TTwo Value2 { get; set; }
    }
    public interface IGenericReaderWriterGrain2State<TOne, TTwo> : IGrainState
    {
        TOne Value1 { get; set; }
        TTwo Value2 { get; set; }
    }

    public interface IGenericReader3State<TOne, TTwo, TThree> : IGrainState
    {
        TOne Value1 { get; set; }
        TTwo Value2 { get; set; }
        TThree Value3 { get; set; }
    }


    public class GenericReaderWriterGrain1<T> : GrainBase<IGenericReaderWriterState<T>>, IGenericReaderWriterGrain1<T>
    {
        public Task SetValue(T value)
        {
            State.Value = value;
            return TaskDone.Done;
        }

        public Task<T> Value
        {
            get { return Task.FromResult(State.Value); }
        }
    }

    public class GenericReaderWriterGrain2<TOne, TTwo> : GrainBase<IGenericReaderWriterGrain2State<TOne, TTwo>>, IGenericReaderWriterGrain2<TOne,TTwo>
    {
        public Task SetValue1(TOne value)
        {
            State.Value1 = value;
            return TaskDone.Done;
        }
        public Task SetValue2(TTwo value)
        {
            State.Value2 = value;
            return TaskDone.Done;
        }

        public Task<TOne> Value1
        {
            get { return Task.FromResult(State.Value1); }
        }

        public Task<TTwo> Value2
        {
            get { return Task.FromResult(State.Value2); }
        }
    }

    public class GenericReaderWriterGrain3<TOne, TTwo, TThree> : GrainBase<IGenericReader3State<TOne, TTwo, TThree>>, IGenericReaderWriterGrain3<TOne, TTwo, TThree>
    {
        public Task SetValue1(TOne value)
        {
            State.Value1 = value;
            return TaskDone.Done;
        }
        public Task SetValue2(TTwo value)
        {
            State.Value2 = value;
            return TaskDone.Done;
        }
        public Task SetValue3(TThree value)
        {
            State.Value3 = value;
            return TaskDone.Done;
        }

        public Task<TThree> Value3
        {
            get { return Task.FromResult(State.Value3); }
        }

        public Task<TOne> Value1
        {
            get { return Task.FromResult(State.Value1); }
        }

        public Task<TTwo> Value2
        {
            get { return Task.FromResult(State.Value2); }
        }
    }

    public class GenericSelfManagedGrain<T, U> : GrainBase, IGenericSelfManagedGrain<T, U>
    {
        private T a;
        private U b;

        public Task<T> A
        {
            get { return Task.FromResult(a); }
        }

        public Task<T> GetA()
        {
            return Task.FromResult(a);
        }

        public Task<string> GetAxB()
        {
            return Task.FromResult(a.ToString() + "x" + b.ToString());
        }

        public Task<string> GetAxB(T a, U b)
        {
            return Task.FromResult(a.ToString() + "x" + b.ToString());
        }

        public Task SetA(T a)
        {
            this.a = a;
            return TaskDone.Done;
        }

        public Task SetB(U b)
        {
            this.b = b;
            return TaskDone.Done;
        }
    }

    public class HubGrain<TKey, T1, T2> : GrainBase, IHubGrain<TKey, T1, T2>
    {
        public virtual Task Bar(TKey key, T1 message1, T2 message2)
        {
            throw new System.NotImplementedException();
        }
    }

    public class EchoHubGrain<TKey, TMessage> : HubGrain<TKey, TMessage, TMessage>, IEchoHubGrain<TKey, TMessage>
    {
        private int _x;

        public Task Foo(TKey key, TMessage message, int x)
        {
            _x = x;
            return TaskDone.Done;
        }

        public override Task Bar(TKey key, TMessage message1, TMessage message2)
        {
            return TaskDone.Done;
        }

        public Task<int> X
        {
            get { return Task.FromResult(_x); }
        }
    }

    public class EchoGenericChainGrain<T> : GrainBase, IEchoGenericChainGrain<T>
    {
        public async Task<T> Echo(T item)
        {
            long pk = this.GetPrimaryKeyLong();
            ISimpleGenericGrain<T> otherGrain = SimpleGenericGrainFactory<T>.GetGrain(pk);
            await otherGrain.SetA(item);
            return await otherGrain.GetA();
        }

        public async Task<T> Echo2(T item)
        {
            long pk = this.GetPrimaryKeyLong() + 1;
            IEchoGenericChainGrain<T> otherGrain = EchoGenericChainGrainFactory<T>.GetGrain(pk);
            return await otherGrain.Echo(item);
        }

        public async Task<T> Echo3(T item)
        {
            long pk = this.GetPrimaryKeyLong() + 1;
            IEchoGenericChainGrain<T> otherGrain = EchoGenericChainGrainFactory<T>.GetGrain(pk);
            return await otherGrain.Echo2(item);
        }
    }
}
