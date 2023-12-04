using NewsAPI;
using NewsAPI.Constants;
using NewsAPI.Models;

const string apiKey = "fd294ab551504c18bed86254e42bb280";

await FetchNewsAsync();

static async Task FetchNewsAsync()
{
    // init with your API key
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
        // total results found
        Console.WriteLine(articlesResponse.TotalResults);
        // here's the first 20
        foreach (var article in articlesResponse.Articles)
        {
            // title
            Console.WriteLine(article.Title);
            // author
            Console.WriteLine(article.Author);
            // description
            Console.WriteLine(article.Description);
            // url
            Console.WriteLine(article.Url);
            // published at
            Console.WriteLine(article.PublishedAt);
        }
    }
    Console.ReadLine();
}