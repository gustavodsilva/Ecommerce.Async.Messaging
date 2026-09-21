using RabbitMQ.Client;
using System.Text;
using System.Text.Json;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.MapPost("/pedido", async (CriarPedidoRequest request) =>
{
    var evento = new PedidoCriadoEvent 
    (
        Guid.NewGuid(),
        request.ProdutoId,
        request.Quantidade,
        request.PrecoUnitario,
        request.Desconto
    );

    var factory = new ConnectionFactory { HostName = "localhost" };
    using var connection = await factory.CreateConnectionAsync();
    using var channel = await connection.CreateChannelAsync();

    await channel.QueueDeclareAsync(
        queue: "pedidos-criados",
        durable: false,
        exclusive: false,
        autoDelete: false);

    var json = JsonSerializer.Serialize(evento);
    var body = Encoding.UTF8.GetBytes(json);

    await channel.BasicPublishAsync(
        exchange: string.Empty,
        routingKey: "pedidos-criados",
        body: body);

    return Results.Accepted(value: evento);
});

app.Run();

record CriarPedidoRequest(Guid ProdutoId, int Quantidade, decimal PrecoUnitario, decimal Desconto);
record PedidoCriadoEvent(Guid PedidoId, Guid ProdutoId, int Quantidade, decimal PrecoUnitario, decimal Desconto);