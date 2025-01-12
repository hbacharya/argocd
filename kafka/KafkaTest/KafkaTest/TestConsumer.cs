using Confluent.Kafka;

namespace KafkaTest
{
    public class TestConsumer
    {
        public ILogger<TestConsumer> Logger { get; }

        public TestConsumer(ILogger<TestConsumer> logger)
        {
            Logger = logger;
        }

        public void Consume()
        {
            Logger.LogInformation("Consuming messages...");

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("topic1");
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();
                        Logger.LogInformation(
                            $"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.TopicPartitionOffset}'");
                    }
                    catch (ConsumeException e)
                    {
                        Logger.LogError($"Error occurred: {e.Error.Reason}");
                    }
                }
            }
        }
    }
}
