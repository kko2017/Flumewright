// See https://aka.ms/new-console-template for more information
using Flumewright.Client;
using Grpc.Net.Client;



using var publisher = new FlumewrightPublisher("http://localhost:5050");
var offset = await publisher.PublishAsync("demo", System.Text.Encoding.UTF8.GetBytes("hello"));
Console.WriteLine($"published at offset {offset}");
