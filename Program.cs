using DefectDataAudio;
using Microsoft.Extensions.Hosting;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddWindowsService(options =>
{
    options.ServiceName = "DefectDataAudio";
});

builder.Services.AddHostedService<Worker>();

var host = builder.Build();
host.Run();