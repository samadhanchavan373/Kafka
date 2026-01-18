using CustomerAPI.Models;

namespace CustomerAPI.Interfaces
{
    public interface IKafkaProducerService
    {
        Task SendCustomerCreatedEventAsync(Customer customer);
    }
}