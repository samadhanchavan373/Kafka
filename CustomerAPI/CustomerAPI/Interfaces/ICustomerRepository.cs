using CustomerAPI.Models;

namespace CustomerAPI.Interfaces
{
    public interface ICustomerRepository
    {
        Task<int> CreateCustomerAsync(Customer customer);
    }
}