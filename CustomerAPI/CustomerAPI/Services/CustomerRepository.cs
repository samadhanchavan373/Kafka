using Confluent.Kafka;
using CustomerAPI.Interfaces;
using CustomerAPI.Models;
using Microsoft.Data.SqlClient;

namespace CustomerAPI.Services
{
    public class CustomerRepository : ICustomerRepository
    {
        private readonly string _connectionString;

        public CustomerRepository(IConfiguration configuration)
        {
            _connectionString = configuration.GetConnectionString("SQLServerConnectionString");
        }

        public async Task<int> CreateCustomerAsync(Customer customer)
        {
            using var conn = new SqlConnection(_connectionString);
            await conn.OpenAsync();

            string query = @"INSERT INTO Customers (Name, Email, Gender)
                             VALUES (@Name, @Email, @Gender);
                             SELECT SCOPE_IDENTITY();";

            using var cmd = new SqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@Name", customer.Name);
            cmd.Parameters.AddWithValue("@Email", customer.Email);
            cmd.Parameters.AddWithValue("@Gender", customer.Gender);

            object result = await cmd.ExecuteScalarAsync();
            return Convert.ToInt32(result);
        }
    }
}
