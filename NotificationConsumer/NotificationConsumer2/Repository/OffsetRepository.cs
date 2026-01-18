using Confluent.Kafka;
using Microsoft.Data.SqlClient;
namespace NotificationConsumer2.Repository
{
    public class OffsetRepository
    {
        private readonly string _connectionString;

        public OffsetRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public long GetLastOffset(string topic, int partition)
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();

            var cmd = new SqlCommand(
                "SELECT LastOffset FROM KafkaOffsets WHERE Topic=@topic AND PartitionId=@pid", conn);
            cmd.Parameters.AddWithValue("@topic", topic);
            cmd.Parameters.AddWithValue("@pid", partition);

            var result = cmd.ExecuteScalar();
            return result == null ? -1 : Convert.ToInt64(result);
        }

        public void SaveOffset(string topic, int partition, long offset)
        {
            using var conn = new SqlConnection(_connectionString);
            conn.Open();

            var cmd = new SqlCommand(@"
                IF EXISTS (SELECT 1 FROM KafkaOffsets WHERE Topic=@topic AND PartitionId=@pid)
                    UPDATE KafkaOffsets SET LastOffset=@offset WHERE Topic=@topic AND PartitionId=@pid
                ELSE
                    INSERT INTO KafkaOffsets (Topic, PartitionId, LastOffset)
                    VALUES (@topic, @pid, @offset)", conn);

            cmd.Parameters.AddWithValue("@topic", topic);
            cmd.Parameters.AddWithValue("@pid", partition);
            cmd.Parameters.AddWithValue("@offset", offset);

            cmd.ExecuteNonQuery();
        }

        public long GetLastOffsetFromSQL(string groupId, string topic, int partition)
        {
            using var connection = new SqlConnection(_connectionString);
            connection.Open();

            string selectQuery = @"
            SELECT LastOffset FROM KafkaConsumerOffsets
            WHERE ConsumerGroupId = @ConsumerGroupId
            AND Topic = @Topic AND PartitionId = @PartitionId";

            using var cmd = new SqlCommand(selectQuery, connection);
            cmd.Parameters.AddWithValue("@ConsumerGroupId", groupId);
            cmd.Parameters.AddWithValue("@Topic", topic);
            cmd.Parameters.AddWithValue("@PartitionId", partition);

            var result = cmd.ExecuteScalar();
            return result != null && result != DBNull.Value ? (long)result : -1;
        }

        public async Task SaveOffset(ConsumeResult<string, string> cr, string consumerGroup)
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync();
            using var transaction = connection.BeginTransaction();

            try
            {
                // Upsert offset
                string upsertQuery = @"
                MERGE KafkaConsumerOffsets AS target
                USING (SELECT @ConsumerGroupId AS ConsumerGroupId, @Topic AS Topic, @PartitionId AS PartitionId) AS source
                ON target.ConsumerGroupId = source.ConsumerGroupId AND target.Topic = source.Topic AND target.PartitionId = source.PartitionId
                WHEN MATCHED THEN
                    UPDATE SET LastOffset = @LastOffset
                WHEN NOT MATCHED THEN
                    INSERT (ConsumerGroupId, Topic, PartitionId, LastOffset)
                    VALUES (@ConsumerGroupId, @Topic, @PartitionId, @LastOffset);";

                using var upsertCmd = new SqlCommand(upsertQuery, connection, transaction);
                upsertCmd.Parameters.AddWithValue("@ConsumerGroupId", consumerGroup);
                upsertCmd.Parameters.AddWithValue("@Topic", cr.Topic);
                upsertCmd.Parameters.AddWithValue("@PartitionId", cr.Partition.Value);
                upsertCmd.Parameters.AddWithValue("@LastOffset", cr.Offset.Value);
                await upsertCmd.ExecuteNonQueryAsync();

                transaction.Commit();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ERROR] {ex.Message}");
                transaction.Rollback();
            }
        }
    }
}
