namespace RabbitPublisher.Services
{
    public interface IRabbitMqService
    {
        Task PublishAsync(string message,string routingKey);
    }
}
