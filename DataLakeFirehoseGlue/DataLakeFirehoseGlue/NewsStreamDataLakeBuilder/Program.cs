using NewsAPI;
using NewsAPI.Constants;
using NewsAPI.Models;

const string apiKey = "fd294ab551504c18bed86254e42bb280";

await FetchNewsAsync();

static async Task FetchNewsAsync()
{
    var newsApiClient = new NewsApiClient(apiKey);
    var articlesResponse = await newsApiClient.GetEverythingAsync(new EverythingRequest
    {
        Sources = new List<string>
        {
            "bbc-news", "abc-news", "associated-press", "bbc-sport", "business-insidert", "cbs-news", "cnn",
            "google-news", "techradar"
        },
        SortBy = SortBys.PublishedAt,
        Language = Languages.EN,
        From = DateTime.UtcNow.Subtract(TimeSpan.FromDays(10))
    });
    if (articlesResponse.Status == Statuses.Ok)
    {
        foreach (var article in articlesResponse.Articles)
        {
            Console.WriteLine(article.PublishedAt);
        }
    }
    Console.ReadLine();
}