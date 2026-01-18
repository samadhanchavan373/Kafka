//See https://aka.ms/new-console-template for more information
//using Confluent.Kafka;
//using NotificationConsumer2.Repository;
//using NotificationConsumer2.Services;
//using System.Text.Json;
//using static System.Runtime.InteropServices.JavaScript.JSType;

//using Confluent.Kafka;
//using System.Text.Json;
//using NotificationConsumer2.Models;
//using Microsoft.Data.SqlClient;
//using NotificationConsumer2.Models;
//using NotificationConsumer2.Repository;
//using NotificationConsumer2.Services;

//Console.WriteLine("🚀 Broker1 started");

//const int partitionNumber = 1;
//var connectionString = "Data Source = DESKTOP-116A5VG; Initial Catalog = kafkaOffset; Integrated Security = True; Encrypt = False; Trust Server Certificate=True;\r\n";
//var offsetRepo = new OffsetRepository(connectionString);
//var emailService = new EmailService(
//    smtpHost: "smtp.gmail.com",
//    smtpPort: 587,
//    fromEmail: "samadhanchavan373@gmail.com",
//    fromPassword: "wcoh tugr zjlt yzkt"  // App-specific password!
//);

//var config = new ConsumerConfig
//{
//    BootstrapServers = "localhost:9092",
//    GroupId = "cg-email-notification",
//    ClientId = "consumer-topic-new-customers-",
//    AutoOffsetReset = AutoOffsetReset.Earliest,
//    EnableAutoCommit = false,
//    SessionTimeoutMs = 30000,
//    MaxPollIntervalMs = 300000

//};

//using var consumer = new ConsumerBuilder<string, string>(config).Build();

//Event to signal partitions assigned
// Track if we need to seek
//var partitionsToSeek = new Dictionary<TopicPartition, long>();

//using var consumer = new ConsumerBuilder<string, string>(config)
//             .SetPartitionsAssignedHandler((c, partitions) =>
//             {
//                 Console.WriteLine("✅ Partitions assigned:");
//                 foreach (var p in partitions)
//                     Console.WriteLine($"   - Partition: {p.Partition}");
//                 c.Assign(partitions); // assign automaticall

//                 foreach (var p in partitions)
//                 {
//                     Console.WriteLine($"Partition: {p.Partition}");
//                     long lastOffset = offsetRepo.GetLastOffsetFromSQL(config.GroupId, p.Topic, p.Partition);
//                     if (lastOffset >= 0)
//                     {
//                         TopicPartitionOffset tpo = new TopicPartitionOffset(
//                        new TopicPartition(p.Topic, p.Partition),       // TopicPartition
//                        new Offset(lastOffset + 1));

//                         c.Seek(tpo);
//                         if (lastOffset >= 0)
//                         {
//                             partitionsToSeek[p] = lastOffset + 1;
//                             Console.WriteLine($"   📌 Will seek to offset {lastOffset + 1} for partition {p.Partition}");
//                         }
//                         else
//                         {
//                             Console.WriteLine($"   ℹ️ No previous offset found, starting from beginning");
//                         }
//                     }
//                 }
//             })
//            .SetPartitionsRevokedHandler((c, partitions) =>
//            {
//                Console.WriteLine("⚠️ Partitions revoked:");
//                foreach (var p in partitions)
//                {
//                    Console.WriteLine($"   - Partition: {p.Partition}");
//                }

//            })
//            .Build();



//consumer.Subscribe("topic-new-customers");


//var cts = new CancellationTokenSource();
//Console.CancelKeyPress += (_, e) =>
//{
//    e.Cancel = true;
//    cts.Cancel();

//};

//bool initialSeekDone = false;
//try
//{
//    while (!cts.Token.IsCancellationRequested)
//    {
//        try
//        {
//            Perform seek after first consume attempt
//            if (!initialSeekDone && partitionsToSeek.Count > 0)
//            {
//                Thread.Sleep(1000); // Give Kafka a moment to fully assign partitions

//                foreach (var kvp in partitionsToSeek)
//                {
//                    try
//                    {
//                        var tpo = new TopicPartitionOffset(kvp.Key, new Offset(kvp.Value));
//                        consumer.Seek(tpo);
//                        Console.WriteLine($"⏩ Seeked to offset {kvp.Value} for partition {kvp.Key.Partition}");
//                    }
//                    catch (Exception ex)
//                    {
//                        Console.WriteLine($"⚠️ Could not seek partition {kvp.Key.Partition}: {ex.Message}");
//                    }
//                }
//                initialSeekDone = true;
//                partitionsToSeek.Clear();
//            }

//            var cr = consumer.Consume(cts.Token);

//            if (cr == null || cr.IsPartitionEOF)
//            {
//                Console.WriteLine("📭 Reached end of partition, waiting for new messages...");
//                Task.Delay(1000).Wait();
//                continue;
//            }


//            var isSuccess = await SaveOffsetAndProcessMessage(cr, emailService, offsetRepo, config);

//            if (isSuccess)
//            {

//                Commit to Kafka
//                Console.WriteLine($"Consumed message: {cr.Message.Value} | Partition: {cr.Partition.Value} | Offset: {cr.Offset.Value}");
//                Task.Delay(4000).Wait();


//                consumer.Commit(cr);

//                Console.WriteLine($"COMMITED MESSAGE: {cr.Message.Value} | PARTITION: {cr.Partition.Value} | OFFSET: {cr.Offset.Value}");
//                Console.WriteLine();
//                Console.WriteLine();
//                Console.WriteLine();
//                Task.Delay(2000).Wait();
//            }
//        }
//        catch (ConsumeException e)
//        {
//            Console.WriteLine($"Consume error: {e.Error.Reason}");
//        }
//    }
//}
//catch (OperationCanceledException)
//{
//    Console.WriteLine("Closing consumer...");
//}
//finally
//{
//    consumer.Close();
//}

//static async Task<bool> SaveOffsetAndProcessMessage(ConsumeResult<string, string> cr, EmailService emailService, OffsetRepository offsetRepo, ConsumerConfig config)
//{

//    try
//    {
//        var customer = JsonSerializer.Deserialize<Customer>(cr.Message.Value);

//        if (customer != null)
//        {
//            string subject = $"Welcome, {customer.Name}!";
//            string body = $"Hello {customer.Name},<br>Thank you for registering.<br>- Kafka Notification Service";
//            var isSuccess = await emailService.SendEmail(customer.Email, subject, body);

//            if (true)
//            {
//                await offsetRepo.SaveOffset(cr, config.GroupId);
//                return true;
//            }
//        }

//        return false;
//    }
//    catch (Exception ex)
//    {
//        return false;
//    }
//}





using Confluent.Kafka;
using NotificationConsumer2.Repository;
using NotificationConsumer2.Services;
using System.Text.Json;
using NotificationConsumer2.Models;

Console.WriteLine("🚀 Broker 2 started");

var connectionString = "Data Source = DESKTOP-116A5VG; Initial Catalog = kafkaOffset; Integrated Security = True; Encrypt = False; Trust Server Certificate=True;\r\n";
var offsetRepo = new OffsetRepository(connectionString);
var emailService = new EmailService(
    smtpHost: "smtp.gmail.com",
    smtpPort: 587,
    fromEmail: "samadhanchavan373@gmail.com",
    fromPassword: "wcoh tugr zjlt yzkt"
);

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "cg-email-notification",
    ClientId = "consumer-topic-new-customers-",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false,
    SessionTimeoutMs = 30000,
    MaxPollIntervalMs = 300000
};

using var consumer = new ConsumerBuilder<string, string>(config)
    .SetPartitionsAssignedHandler((c, partitions) =>
    {
        Console.WriteLine("✅ Partitions assigned:");
        foreach (var partition in partitions)
        {
            Console.WriteLine($"   - Partition: {partition.Partition}");

            long dbOffset = offsetRepo.GetLastOffsetFromSQL(config.GroupId, partition.Topic, partition.Partition);

            if (dbOffset >= 0)
            {
                Console.WriteLine($"           -  DB shows next offset should be: {dbOffset + 1}");
            }
            else
            {
                Console.WriteLine($"No previous offset in DB");
            }
        }
    })
    .SetPartitionsRevokedHandler((c, partitions) =>
    {
        Console.WriteLine("Partitions revoked:");
        foreach (var p in partitions)
        {
            Console.WriteLine($"   - Partition: {p.Partition}");
        }
    })
    .Build();

consumer.Subscribe("topic-new-customers");

var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    cts.Cancel();
};

try
{
    Console.WriteLine("Starting to consume messages...");
    Task.Delay(5000).Wait();

    while (!cts.Token.IsCancellationRequested)
    {
        try
        {
            var cr = consumer.Consume(cts.Token);

            if (cr == null || cr.IsPartitionEOF)
            {
                Console.WriteLine("Reached end of partition, waiting for new messages...");
                Task.Delay(1000).Wait();
                continue;
            }

            Console.WriteLine($"Received message at offset {cr.Offset.Value} from partition {cr.Partition.Value}");

            // CRITICAL: Check if this message was already processed
            var dbOffset = offsetRepo.GetLastOffsetFromSQL(
                 config.GroupId,
                 cr.Topic,
                 cr.Partition.Value
             );

            if (cr.Offset.Value <= dbOffset)
            {
                Console.WriteLine($"SKIPPING already processed message at offset {cr.Offset.Value} for partition {cr.Partition.Value}");

                // Commit to move Kafka's offset forward
                try
                {
                    consumer.Commit(cr);
                    Console.WriteLine($"Committed skip of offset {cr.Offset.Value} for partition {cr.Partition.Value}");
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"Commit failed: {ex.Message}");
                }

                Console.WriteLine();
                continue;
            }

            // Process the message
            var isSuccess = await SaveOffsetAndProcessMessage(cr, emailService, offsetRepo, config);

            if (isSuccess)
            {
                Console.WriteLine($"Processed NEW message: {cr.Message.Value} | Partition: {cr.Partition.Value} | Offset: {cr.Offset.Value}");

                // Commit to Kafka after successful processing
                try
                {
                    Task.Delay(4000).Wait();
                    consumer.Commit(cr);
                    Console.WriteLine($"COMMITTED to Kafka: Partition {cr.Partition.Value} | Offset: {cr.Offset.Value}");
                }
                catch (KafkaException ex)
                {
                    Console.WriteLine($"Kafka commit failed (DB is still safe): {ex.Message}");
                }

                Task.Delay(4000).Wait();
                Console.WriteLine();
            }
            else
            {
                Console.WriteLine($"Failed to process offset {cr.Offset.Value} for partiton {cr.Offset.Value}, will retry on next run");
                // Don't commit - will reprocess on next run
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
        }
        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine();
    }
}
catch (OperationCanceledException)
{
    Console.WriteLine("Closing consumer...");
}
finally
{
    consumer.Close();
}

static async Task<bool> SaveOffsetAndProcessMessage(ConsumeResult<string, string> cr, EmailService emailService, OffsetRepository offsetRepo, ConsumerConfig config)
{
    try
    {
        var customer = JsonSerializer.Deserialize<Customer>(cr.Message.Value);

        if (customer != null)
        {
            string subject = $"Welcome, {customer.Name}!";
            string body = $"Hello {customer.Name},<br>Thank you for registering.<br>- Kafka Notification Service";

            // Uncomment when ready to send actual emails
            // var isSuccess = await emailService.SendEmail(customer.Email, subject, body);

            bool emailSent = true; // Replace with actual email sending result

            if (emailSent)
            {
                // Save that we processed this specific offset
                await offsetRepo.SaveOffset(cr, config.GroupId);
                Console.WriteLine($"💾 Saved processed offset {cr.Offset.Value} to database");
                return true;
            }
            else
            {
                return false;
            }
        }

        return true;
    }
    catch (Exception ex)
    {
        Console.WriteLine($"❌ Error processing message: {ex.Message}");
        return false;
    }
}