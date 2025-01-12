using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace KafkaTest.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class KafkaTestController : ControllerBase
    {
        private readonly ILogger<KafkaTestController> _logger;

        public KafkaTestController(ILogger<KafkaTestController> logger)
        {
            _logger = logger;
        }

        [HttpGet(Name = "GetMessages")]
        public IEnumerable<string> Get()
        {
            return null;
        }

        public async Task<IActionResult> Post([FromBody] string message)
        {
            //create a kafka producer and send message
            var config = new ProducerConfig { BootstrapServers = "localhost:9092" };
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    var deliveryReport = await producer.ProduceAsync("topic1", new Message<Null, string>
                    {
                        Value = message
                    });
                    _logger.LogInformation($"Delivered message to {deliveryReport.TopicPartitionOffset}");
                    return Ok(deliveryReport.TopicPartitionOffset);
                }
                catch (ProduceException<Null, string> e)
                {
                    _logger.LogError($"Delivery failed: {e.Error.Reason}");
                }
            }

            return Ok("Error");
        }
    }
}
