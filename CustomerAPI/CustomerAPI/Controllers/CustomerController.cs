using CustomerAPI.Interfaces;
using CustomerAPI.Models;
using CustomerAPI.Services;
using Microsoft.AspNetCore.Mvc;

namespace CustomerAPI.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class CustomerController : ControllerBase
    {
        private readonly IKafkaProducerService kafkaProducerService;

        public CustomerController(IConfiguration config, IKafkaProducerService kafkaProducerService )
        {
            this.kafkaProducerService = kafkaProducerService;
        }

        [HttpPost("create")]
        public async Task<IActionResult> CreateCustomer([FromBody] Customer customer)
        {
            await kafkaProducerService.SendCustomerCreatedEventAsync(customer);

            return Ok(new { message = "Customer created successfully", customer });
        }
    }
}