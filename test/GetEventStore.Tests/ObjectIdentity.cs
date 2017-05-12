﻿using EventSourcing.Tests;
using Orleans.EventSourcing;
using Orleans.Storage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestExtensions;
using Xunit;
using Xunit.Abstractions;

namespace GetEventStore.Tests
{
    [Collection(TestEnvironmentFixture.DefaultCollection)]
    [TestCategory("Functional")]
    public class ObjectIdentity : OrleansTestingBase, IClassFixture<ProvidersFixture>
    {

        private readonly ITestOutputHelper output;
        private readonly ProvidersFixture fixture;

        public ObjectIdentity(ITestOutputHelper output, ProvidersFixture fixture)
        {
            this.output = output;
            this.fixture = fixture;
        }

        private Task Store<E>(IEventStreamHandle stream, E obj)
        {
            return stream.Append<E>(new KeyValuePair<Guid, E>[] { new KeyValuePair<Guid, E>(Guid.NewGuid(), obj) });
        }

        private async Task<E> Load<E>(IEventStreamHandle stream)
        {
            var rsp = await stream.Load<E>(0, 1);
            return rsp.Events[0].Value;
        }


        class N { public string val;  public N left; public N right; }
        


        [Fact]
        public async Task StoreTree()
        {
            using (var stream = fixture.EventStoreDefault.GetEventStreamHandle(Guid.NewGuid().ToString()))
            {
                var left = new N() { val = "l" };
                var right = new N() { val = "r" };
         
                await Store<N>(stream, new N() { left = left, right = right });

                var n =  await Load<N>(stream);

                Assert.Equal("l", n.left.val);
                Assert.Equal("r", n.right.val);
            }
        }


        [Fact]
        public async Task StoreDag()
        {
            using (var stream = fixture.EventStoreDefault.GetEventStreamHandle(Guid.NewGuid().ToString()))
            {
                var child = new N() { val = "c" };

                await Store<N>(stream, new N() { left = child, right = child });

                var n = await Load<N>(stream);

                Assert.Equal("c", n.left.val);
                Assert.Equal("c", n.right.val);

                // shared node is not recognized because object identity was not stored
                Assert.NotEqual(n.left, n.right);
            }
        }

        [Fact]
        public async Task StoreDagWithObjectIdentity()
        {
            using (var stream = fixture.EventStoreObjectIdentity.GetEventStreamHandle(Guid.NewGuid().ToString()))
            {
                var child = new N() { val = "c" };

                await Store<N>(stream, new N() { left = child, right = child });

                var n = await Load<N>(stream);

                Assert.Equal("c", n.left.val);
                Assert.Equal("c", n.right.val);

                // shared node is recognized because object identity was stored
                Assert.Equal(n.left, n.right);
            }
        }
    }
}
