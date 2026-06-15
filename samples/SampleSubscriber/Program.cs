// See https://aka.ms/new-console-template for more information
using Flumewright.Client;
using Grpc.Net.Client;
using System;
using System.Text;



using var subscriber = new FlumewrightSubscriber("http://localhost:5050");

Console.WriteLine("Subscribing to 'demo'... (Ctrl+C to stop)");

await foreach(var msg in subscriber.SubscribeAsync("demo"))
{
    var text = Encoding.UTF8.GetString(msg.Payload);
    Console.WriteLine($"received: topic={msg.Topic} offset={msg.Offset} payload={text}");
}
