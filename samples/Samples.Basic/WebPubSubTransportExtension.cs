using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;
using Azure.Core;
using GraphQL.Server.Transports.AspNetCore;
using GraphQL.Server.Transports.AspNetCore.Errors;
using GraphQL.Server.Transports.AspNetCore.WebSockets;
using GraphQL.Transport;
using GraphQL.Types;
using Microsoft.Azure.WebPubSub.AspNetCore;
using Microsoft.Azure.WebPubSub.Common;

namespace GraphQL.Server.Samples.Basic;

public static class WebPubSubTransportExtension
{
    public static IServiceCollection AddWebPubSubTransport(this IServiceCollection services, Action<GraphQLHttpMiddlewareOptions>? configureOptions = null)
    {
        services.AddWebPubSub(o =>
        {
            o.ServiceEndpoint = new WebPubSubServiceEndpoint(Environment.GetEnvironmentVariable("WebPubSubConnectionString"));
        }).AddWebPubSubServiceClient<GraphQLHub>();
        services.AddSingleton<ClientConnectionManager>();
        services.AddSingleton<GraphQLWebPubSubMiddleware>();

        var opts = new GraphQLHttpMiddlewareOptions();
        configureOptions?.Invoke(opts);
        services.AddSingleton<GraphQLHttpMiddlewareOptions>(opts);
        return services;
    }

    public static IApplicationBuilder UseWebPubSubTransport(this IApplicationBuilder app)
    {
        app.UseRouting();
        var applicationBuilder = app.UseEndpoints(e => e.MapWebPubSubHub<GraphQLHub>("/wps/{**path}"));
        return applicationBuilder;
    }
}

public sealed record WebPubSubConnection : IWebSocketConnection
{
    private readonly WebPubSubServiceClient<GraphQLHub> _serviceClient;
    private readonly IGraphQLSerializer _graphQLSerializer;
    private readonly CancellationTokenSource _abortRequest = new CancellationTokenSource();
    public DateTime LastMessageSentAt { get; private set; }
    public CancellationToken RequestAborted { get; init; }
    public HttpContext HttpContext { get; }
    public string ConnectionId { get; }

    public WebPubSubConnection(WebPubSubServiceClient<GraphQLHub> serviceClient, string connectionId, HttpContext httpContext, IGraphQLSerializer graphQLSerializer)
    {
        _serviceClient = serviceClient;
        ConnectionId = connectionId;
        _graphQLSerializer = graphQLSerializer;
        HttpContext = httpContext;
        RequestAborted = _abortRequest.Token;
    }
    public Task CloseAsync() => CloseAsync(1000, null);
    public Task CloseAsync(int eventId, string? description) => _serviceClient.CloseConnectionAsync(ConnectionId, description);
    public void Dispose() { _abortRequest.Cancel(); }
    public Task ExecuteAsync(IOperationMessageProcessor operationMessageProcessor) => throw new NotImplementedException();
    public async Task SendMessageAsync(OperationMessage message)
    {
        using var stream = new MemoryStream();
        await _graphQLSerializer.WriteAsync(stream, message, RequestAborted);
        stream.Position = 0;
        await _serviceClient.SendToConnectionAsync(ConnectionId, RequestContent.Create(BinaryData.FromStream(stream)), ContentType.ApplicationJson);
        LastMessageSentAt = DateTime.UtcNow;
    }
}

public sealed record ClientConnectionContext(HttpContext HttpContext, string ConnectionId, WebPubSubConnection Connection, IOperationMessageProcessor OperationMessageProcessor) : IDisposable
{
    public void Dispose()
    {
        Connection.Dispose();
        OperationMessageProcessor.Dispose();
    }
}

/// <summary>
/// Single Server Connection Manager
/// </summary>
public class ClientConnectionManager
{
    public ConcurrentDictionary<string, ClientConnectionContext> ClientConnections { get; } = new ConcurrentDictionary<string, ClientConnectionContext>();
}

public class GraphQLHub : WebPubSubHub
{

    private readonly WebPubSubServiceClient<GraphQLHub> _serviceClient;
    private readonly ClientConnectionManager _connectionManager;
    private readonly GraphQLWebPubSubMiddleware _webPubSubMiddleware;
    private readonly IGraphQLSerializer _graphQLSerializer;
    private readonly GraphQLHttpMiddlewareOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger<GraphQLHub> _logger;

    // Need to ensure service client is injected by call `AddServiceHub<SampleHub>` in ConfigureServices.
    public GraphQLHub(WebPubSubServiceClient<GraphQLHub> serviceClient, ClientConnectionManager connectionManager, GraphQLWebPubSubMiddleware webPubSubMiddleware, IGraphQLSerializer graphQLSerializer, GraphQLHttpMiddlewareOptions options, IServiceProvider serviceProvider, ILogger<GraphQLHub> logger)
    {
        _serviceClient = serviceClient;
        _connectionManager = connectionManager;
        _webPubSubMiddleware = webPubSubMiddleware;
        _graphQLSerializer = graphQLSerializer;
        _options = options;
        _serviceProvider = serviceProvider;
        _logger = logger;
    }

    public override async ValueTask<ConnectEventResponse> OnConnectAsync(ConnectEventRequest request, CancellationToken cancellationToken)
    {
        // reestablish the httpContext, with claims, queries
        var httpContext = request.RebuildHttpContext(_serviceProvider);

        if (await _webPubSubMiddleware.AuthorizeWebSocketConnectionAsync(httpContext))
        {
            throw new UnauthorizedAccessException();
        }

        var subprotocol = request.Subprotocols.Count > 0 ? request.Subprotocols[0] : null;
#pragma warning disable CA2254 // Template should be a static expression
        _logger.LogInformation($"Connection: {request.ConnectionContext.ConnectionId}, Subprotocol: {subprotocol}");
#pragma warning restore CA2254 // Template should be a static expression

        // follow the same rules as how GraphQLHttpMiddleware handles the WebSocket requests
        string? subProtocol = null;
        // select a sub-protocol, preferring the first sub-protocol requested by the client
        foreach (var protocol in request.Subprotocols)
        {
            if (_options.WebSockets.SupportedWebSocketSubProtocols.Contains(protocol))
            {
                subProtocol = protocol;
                break;
            }
        }

        if (subProtocol == null)
        {
            throw new WebSocketSubProtocolNotSupportedError(request.Subprotocols);
        }

        var response = new ConnectEventResponse
        {
            Subprotocol = subprotocol
        };

        // in this POC we suppose the requests comes into the same upstream server, if there are multiple servers, make sure to setup sticky policy so that requests belongs to the same connectionId goes to the same server instance
        var connectionId = request.ConnectionContext.ConnectionId;
        var connection = new WebPubSubConnection(_serviceClient, connectionId, httpContext, _graphQLSerializer);
        var messageProcessor = _webPubSubMiddleware.CreateMessageProcessorWrapper(connection, subprotocol!);
        _connectionManager.ClientConnections.TryAdd(connectionId, new ClientConnectionContext(httpContext, connectionId, connection, messageProcessor));
        await messageProcessor.InitializeConnectionAsync();
        return response;
    }

    public override Task OnConnectedAsync(ConnectedEventRequest request)
    {
#pragma warning disable CA2254 // Template should be a static expression
        _logger.LogInformation($"{request.ConnectionContext.ConnectionId} connected.");
#pragma warning restore CA2254 // Template should be a static expression
        return Task.CompletedTask;
    }

    public override async ValueTask<UserEventResponse> OnMessageReceivedAsync(UserEventRequest request, CancellationToken cancellationToken)
    {
        if (!_connectionManager.ClientConnections.TryGetValue(request.ConnectionContext.ConnectionId, out var connectionContext))
        {
            throw new InvalidOperationException($"{request.ConnectionContext.ConnectionId} is not found, make sure the requests from one connectionId is routed to the same upstream server.");
        }


        var message = await _graphQLSerializer.ReadAsync<OperationMessage>(request.Data.ToStream(), cancellationToken);
        if (message != null)
        {
            await connectionContext.OperationMessageProcessor.OnMessageReceivedAsync(message);
        }
        return new UserEventResponse();
    }

    public override Task OnDisconnectedAsync(DisconnectedEventRequest request)
    {
        if (!_connectionManager.ClientConnections.TryGetValue(request.ConnectionContext.ConnectionId, out var connectionContext))
        {
            throw new InvalidOperationException($"{request.ConnectionContext.ConnectionId} is not found, make sure the requests from one connectionId is routed to the same upstream server.");
        }

        connectionContext.Dispose();

        return Task.CompletedTask;
    }
}

public class GraphQLWebPubSubMiddleware : GraphQLHttpMiddleware<ISchema>
{
    private static readonly RequestDelegate EmptyRequestDelegate = (context) => Task.CompletedTask;
    public GraphQLWebPubSubMiddleware(IGraphQLTextSerializer serializer, IDocumentExecuter<ISchema> documentExecuter, IServiceScopeFactory serviceScopeFactory, GraphQLHttpMiddlewareOptions options, IHostApplicationLifetime hostApplicationLifetime) : base(EmptyRequestDelegate, serializer, documentExecuter, serviceScopeFactory, options, hostApplicationLifetime)
    {
    }

    public ValueTask<bool> AuthorizeWebSocketConnectionAsync(HttpContext context) => HandleAuthorizeAsync(context, EmptyRequestDelegate);

    public IOperationMessageProcessor CreateMessageProcessorWrapper(IWebSocketConnection webSocketConnection, string subProtocol) => CreateMessageProcessor(webSocketConnection, subProtocol);

    protected override ValueTask<IDictionary<string, object?>?> BuildUserContextAsync(HttpContext context, object? payload) => base.BuildUserContextAsync(context, payload);

    protected override IWebSocketConnection CreateWebSocketConnection(HttpContext httpContext, WebSocket webSocket, CancellationToken cancellationToken) => base.CreateWebSocketConnection(httpContext, webSocket, cancellationToken);
}

public static class HttpContextRebuilder
{
    public static HttpContext RebuildHttpContext(this ConnectEventRequest request, IServiceProvider serviceProvider)
    {
        var context = new DefaultHttpContext
        {
            RequestServices = serviceProvider
        };
        // Set query parameters
        var query = new QueryCollection(request.Query.ToDictionary(
            kvp => kvp.Key,
            kvp => new Microsoft.Extensions.Primitives.StringValues(kvp.Value)
        ));
        context.Request.Query = query;

        // Set headers
        foreach (var header in request.Headers)
        {
            context.Request.Headers[header.Key] = header.Value;
        }

        // Set user claims
        var claims = request.Claims
            .SelectMany(kvp => kvp.Value.Select(v => new Claim(kvp.Key, v)))
            .ToList();
        context.User = new ClaimsPrincipal(new ClaimsIdentity(claims));

        // Add subprotocols to Items (or handle via a custom feature if needed)
        context.Items["WebSocketSubprotocols"] = request.Subprotocols;

        // TODO: Client certificates (if needed in TLS-related logic)

        return context;
    }
}
