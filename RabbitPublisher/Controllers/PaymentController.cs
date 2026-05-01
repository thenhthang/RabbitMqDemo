using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using RabbitMQ;
using RabbitPublisher.Services;
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
            _rabbitMqService.PublishAsync("Xin chao "+DateTime.Now.ToString(), "test");
            return Ok();
        }
    }
}
