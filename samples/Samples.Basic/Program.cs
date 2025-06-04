using GraphQL;
using Chat = GraphQL.Samples.Schemas.Chat;
using GraphQL.Server.Samples.Basic;
using Microsoft.Azure.WebPubSub.AspNetCore;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<Chat.IChat, Chat.Chat>();
builder.Services.AddGraphQL(b => b
    .AddAutoSchema<Chat.Query>(s => s
        .WithMutation<Chat.Mutation>()
        .WithSubscription<Chat.Subscription>())
    .AddSystemTextJson());

builder.Services.AddWebPubSubTransport();
var app = builder.Build();
app.UseDeveloperExceptionPage();

app.UseWebPubSubTransport();
var serviceClient = app.Services.GetRequiredService<WebPubSubServiceClient<GraphQLHub>>();

var url = serviceClient.GetClientAccessUri();
// configure the graphql endpoint at "/graphql"
app.UseGraphQL("/graphql", config =>
{
    config.CsrfProtectionEnabled = false;
    config.ValidationErrorsReturnBadRequest = false;
});

// TODO: Looks like GraphiQL does not provide a way to dynamically refresh the SubscriptionsEndPoint value, url contains access_token which will be expired, need a way to refresh it in a real frontend product
app.UseGraphQLGraphiQL(
    "/",
    new GraphQL.Server.Ui.GraphiQL.GraphiQLOptions
    {
        GraphQLEndPoint = "/graphql",
        SubscriptionsEndPoint = url.AbsoluteUri,
    });

await app.RunAsync();

