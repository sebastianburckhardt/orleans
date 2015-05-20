using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orleans;

namespace AppFabricSample
{

    [Serializable]
    public sealed class Product
    {
        public Guid SKU { get; internal set; }
        public string Name { get; set; }
        public string Information { get; set; }
        public int Quantity { get; set; }
        public float UnitPrice { get; set; }

        public Product()
        {
            this.SKU = Guid.NewGuid();
        }
    }

    [Serializable]
    public sealed class Order
    {
        public Guid OrderId { get; internal set; }
        public Guid CustomerId { get; internal set; }
        public DateTime OrderDate { get; set; }
        public List<Product> OrderItems { get; set; }
        public float Total { get; set; }

        public Order(Guid customerId, DateTime orderDate, List<Product> orderItems)
        {
            this.OrderItems = new List<Product>();

            foreach (Product product in OrderItems)
                this.Total += product.UnitPrice * product.Quantity;
        }
    }


    public interface ICustomer : IGrain
    {
        Task AddProductToCart(Product item);
        Task RemoveProductFromCart(Product item);
        Task UpdateProductInCart(Product item);
        Task<Order> Checkout();
    }

    public interface IOrderProcessing : IGrain
    {
        Task EnqueueOrder(Order order);
    }
}
