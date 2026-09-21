using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
using System.Text.Json;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var factory = new ConnectionFactory { HostName = "localhost" };
        using var connection = await factory.CreateConnectionAsync(stoppingToken);
        using var channel = await connection.CreateChannelAsync(cancellationToken: stoppingToken);

        await channel.QueueDeclareAsync(
            queue: "pedidos-criados",
            durable: false,
            exclusive: false,
            autoDelete: false,
            cancellationToken: stoppingToken);

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var json = Encoding.UTF8.GetString(body);
            var evento = JsonSerializer.Deserialize<PedidoCriadoEvent>(json);

            if (evento is not null)
            {
                var total = (evento.PrecoUnitario * evento.Quantidade) * (1 - evento.Desconto);
                _logger.LogInformation(
                    "Pedido {PedidoId} processado. Total: {Total:C}",
                    evento.PedidoId, total);
            }

            await channel.BasicAckAsync(ea.DeliveryTag, false, stoppingToken);
        };

        await channel.BasicConsumeAsync(
            queue: "pedidos-criados",
            autoAck: false,
            consumer: consumer,
            cancellationToken: stoppingToken);

        await Task.Delay(Timeout.Infinite, stoppingToken);
    }
}

record PedidoCriadoEvent(Guid PedidoId, Guid ProdutoId, int Quantidade, decimal PrecoUnitario, decimal Desconto);