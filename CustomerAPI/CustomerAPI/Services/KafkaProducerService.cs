using Confluent.Kafka;
using CustomerAPI.Config;
using CustomerAPI.Interfaces;
using CustomerAPI.Models;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Partitioner = Confluent.Kafka.Partitioner;

namespace CustomerAPI.Services
{
    public class KafkaProducerService : IKafkaProducerService
    {
        private readonly IProducer<string, string> _producer;
        private readonly ICustomerRepository customerRepository;
        private KafkaConfig kafkaConfig;

        public KafkaProducerService(ICustomerRepository customerRepository, IOptions<KafkaConfig> kafkaConfig)
        {
            this.kafkaConfig = kafkaConfig.Value;

            var config = new ProducerConfig
            {
                BootstrapServers = this.kafkaConfig.BootstrapServers,
                Partitioner = Partitioner.Murmur2Random
            };
            this.customerRepository = customerRepository;


            _producer =
            //new ProducerBuilder<string, string>(config)
            //    .Build();
            new ProducerBuilder<string, string>(config)
                .SetPartitioner(this.kafkaConfig.Topic, GenderPartitioner)
                .Build();
        }

        public async Task SendCustomerCreatedEventAsync(Customer customer)
        {
            if (string.IsNullOrEmpty(customer.Name) || string.IsNullOrEmpty(customer.Email))
                return ;

            int id = await this.customerRepository.CreateCustomerAsync(customer);
            customer.Id = id;

            string topic = this.kafkaConfig.Topic;

            // Partition 0 for Male, Partition 1 for Female
            int partition = customer.Gender.Equals("Male", StringComparison.OrdinalIgnoreCase) ? 0 : 1;

            var msg = new Message<string, string>
            {
                Key = customer.Gender,
                Value = JsonSerializer.Serialize(customer)
            };

            await _producer.ProduceAsync(topic, msg );

            Console.WriteLine($"✅ Sent message to {topic} partition {partition}");
        }

        private Partition GenderPartitioner(string topic, int partitionCount,
                                          ReadOnlySpan<byte> keyData, bool keyIsNull)
        {
            if (keyIsNull || keyData.Length == 0)
                return new Partition(0); // Default to partition 0 if no key

            // Convert key bytes to string
            string gender = Encoding.UTF8.GetString(keyData);

            // Male -> Partition 0, Female -> Partition 1
            if (gender.Equals("Male", StringComparison.OrdinalIgnoreCase))
                return new Partition(0);
            else if (gender.Equals("Female", StringComparison.OrdinalIgnoreCase))
                return new Partition(1);
            else
                return new Partition(0); // Default partition for unknown gender
        }
    }

    
}
