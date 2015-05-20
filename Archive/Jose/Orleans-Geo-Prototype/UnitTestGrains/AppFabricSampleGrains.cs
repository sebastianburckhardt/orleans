using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Orleans;

namespace AppFabricSample
{
    public interface ICustomerState : IGrainState
    {
        Guid CustomerId { get; set;  }
        Dictionary<Guid, Product> Products { get; set; }
        IOrderProcessing Processing { get; set; }
    }
    public class Customer : GrainBase<ICustomerState>, ICustomer
    {
        public override Task ActivateAsync()
        {
            State.CustomerId = this.GetPrimaryKey();
            State.Processing = OrderProcessingFactory.GetGrain((new Random()).Next());
            return TaskDone.Done;
        }

        public Task AddProductToCart(Product item)
        {
            State.Products.Add(item.SKU, item);
            return TaskDone.Done;
        }

        public Task RemoveProductFromCart(Product item)
        {
            State.Products.Remove(item.SKU);
            return TaskDone.Done;
        }

        public Task UpdateProductInCart(Product item)
        {
            State.Products[item.SKU] = item;
            return TaskDone.Done;
        }

        public async Task<Order> Checkout()
        {
            Order order = new Order(State.CustomerId, DateTime.UtcNow, State.Products.Values.ToList<Product>());

            await State.Processing.EnqueueOrder(order);
            State.Products.Clear();
            return order;
        }

        public Task<Guid> CustomerId
        {
            get { return Task.FromResult(State.CustomerId); }
        }
    }


    public class OrderProcessing : GrainBase, IOrderProcessing
    {
        public Task EnqueueOrder(Order order)
        {
            return TaskDone.Done;
        }
    }
}
