using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RabbitMQ;
using RabbitPublisher.Services;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace RabbitPublisher.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class PaymentController : ControllerBase
    {
        private readonly IRabbitMqService _rabbitMqService;
        public PaymentController(IRabbitMqService rabbitMqService) 
        {
            _rabbitMqService = rabbitMqService;
        }
        [HttpPost]
        [Route("ipn")]
        public IActionResult Webhook()
        {
            var data = new
            {
                orderId = Guid.NewGuid().ToString(),
                orderInfo = "Thanh toan dang ky kham benh",
                amount = 150000,
                createdDate = DateTime.Now,
            };
            var jsonData = JsonSerializer.Serialize(data);
            _rabbitMqService.PublishAsync(jsonData, "test");
            return Ok();
        }
    }
}
