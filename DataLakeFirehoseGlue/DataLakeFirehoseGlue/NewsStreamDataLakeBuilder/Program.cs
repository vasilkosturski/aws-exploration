using System.Text;
using System.Text.Json;
using Amazon;
using Amazon.KinesisFirehose;
using Amazon.KinesisFirehose.Model;
using NewsAPI;
using NewsAPI.Constants;
using NewsAPI.Models;

const string apiKey = "your_api_key";
const string firehoseStream = "NEWS-API-STREAM";

var articles = await FetchNewsAsync();

foreach (var article in articles)
    Console.WriteLine($"{article.PublishedAt}: {article.Title}");

using var firehoseClient = new AmazonKinesisFirehoseClient(RegionEndpoint.USEast1);
await SendToFirehoseAsync(firehoseClient, articles);

Console.ReadKey();

static async Task<List<Article>> FetchNewsAsync()
{
    var newsApiClient = new NewsApiClient(apiKey);
    var articlesResponse = await newsApiClient.GetEverythingAsync(new EverythingRequest
    {
        Sources = new List<string>
        {
            "bbc-news", "abc-news", "associated-press", "bbc-sport", "business-insider", "cbs-news", "cnn",
            "google-news", "techradar"
        },
        SortBy = SortBys.PublishedAt,
        Language = Languages.EN,
        From = DateTime.UtcNow.Subtract(TimeSpan.FromDays(10))
    });
    
    return articlesResponse.Articles;
}

static async Task SendToFirehoseAsync(AmazonKinesisFirehoseClient firehoseClient, List<Article> articles)
{
    foreach (var article in articles)
    {
        var articleJson = JsonSerializer.Serialize(article); 
        
        var record = new Record
        {
            Data = new MemoryStream(Encoding.UTF8.GetBytes($"{articleJson}\n"))
        };

        var request = new PutRecordRequest
        {
            DeliveryStreamName = firehoseStream,
            Record = record
        };

        var response = await firehoseClient.PutRecordAsync(request);
        Console.WriteLine($"Sent to Firehose, Record ID: {response.RecordId}");
    }
}