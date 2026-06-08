using Confluent.Kafka;
using System.Text.Json;
using AuthService.Application.Events;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;

namespace AuthService.Infrastructure.Kafka;

public interface IKafkaProducerService
{
    Task PublishUserRegisteredAsync(UserRegisteredEvent evt, CancellationToken ct = default);
}

public class KafkaProducerService : IKafkaProducerService, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly string _topic;
    private readonly ILogger<KafkaProducerService> _logger;

    public KafkaProducerService(IConfiguration config, ILogger<KafkaProducerService> logger)
    {
        _logger = logger;
        _topic = config["Kafka:Topic"]!;

        var producerConfig = new ProducerConfig
        {
            BootstrapServers = config["Kafka:BootstrapServers"],
            Acks = Acks.All,                    // espera confirmación de todas las réplicas
            EnableIdempotence = true,            // evita duplicados en retry
            MessageTimeoutMs = 5000
        };

        _producer = new ProducerBuilder<string, string>(producerConfig).Build();
    }

    public async Task PublishUserRegisteredAsync(UserRegisteredEvent evt, CancellationToken ct = default)
    {
        var message = new Message<string, string>
        {
            Key = evt.UserId.ToString(),         // la key determina la partición
            Value = JsonSerializer.Serialize(evt)
        };

        var result = await _producer.ProduceAsync(_topic, message, ct);

        _logger.LogInformation(
            "Evento publicado → Topic: {Topic} | Partición: {Partition} | Offset: {Offset}",
            result.Topic, result.Partition.Value, result.Offset.Value);
    }

    public void Dispose() => _producer?.Dispose();
}