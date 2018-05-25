-module(emq_amqp_client).

-behaviour(gen_server).

%% API
-export([start_link/0, publish/3]).

%% GenServer API
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-define(SERVER, ?MODULE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("emq_amqp.hrl").
-include("emq_amqp_cli.hrl").

-record(state, {connection, channel}).


%%====================================================================
%% API
%%====================================================================

-spec(start_link() ->
  {ok, Pid :: pid()} |
  ignore |
  {error, Reason :: term()}
).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec(publish(Exchange:: binary(), RoutingKey:: binary(), Msg:: iodata() | binary()) -> ok).
publish(Exchange, RoutingKey, Msg) when is_binary(Msg) ->
  gen_server:cast(?MODULE, {publish, Exchange, RoutingKey, Msg});
publish(Exchange, RoutingKey, Msg) ->
  publish(Exchange, RoutingKey, jiffy:encode(Msg)).


%%====================================================================
%% GenServer
%%====================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #state{}} |
  {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore
).
init([]) ->
  {ok, Connection} = amqp_connection:start(application:get_env(?APP, amqp)),
  {ok, Channel} = amqp_connection:open_channel(Connection),

  %% Limit unacknowledged messages to 10
  amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 10}),

  {ok, #state{
    connection = Connection,
    channel    = Channel
  }}.

%%--------------------------------------------------------------------

-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()), State :: #state{}) -> term()).
terminate(_Reason, #state{connection = Connection, channel = Channel}) ->
  amqp_channel:close(Channel),
  amqp_connection:close(Connection),
  ok.

%%--------------------------------------------------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_call({declare_exchange, ExchangeName, Type}, _From, State) ->
  amqp_channel:call(State#state.channel, #'exchange.declare'{
    exchange = ExchangeName,
    durable  = true,
    type     = Type
  }),
  {noreply, State};

handle_call({declare_queue, Exchange, Queue}, _From, State) ->
  #state{connection = _, channel = Channel} = State,
  #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, #'queue.declare'{queue = Queue, auto_delete = false}),
  amqp_channel:call(Channel, #'queue.bind'{queue = Q, exchange = Exchange, routing_key = <<"dn.key.resource.delete.*.5">>}), %TODO: Check the routing key
  #'basic.consume_ok'{} = amqp_channel:subscribe(Channel, #'basic.consume'{no_ack = false, queue = Q}, self()),
  {noreply, State};

handle_call(_Request, _From, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_cast({publish, Exchange, RKey, Msg}, State) ->
  Publish = #'basic.publish'{exchange = Exchange, routing_key = RKey},
  amqp_channel:cast(State#state.channel, Publish, #amqp_msg{payload = Msg, props = #'P_basic'{
    content_type = <<"application/json">>
  }}),
  {noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).

handle_info(#'basic.consume_ok'{}, State) ->
  {noreply, State};

handle_info(#'basic.cancel_ok'{}, State) ->
  {noreply, State};

handle_info({#'basic.deliver'{exchange = Exchange, routing_key = RoutingKey, delivery_tag = Tag}, Msg}, State) ->
  #amqp_msg{payload = Payload} = Msg,
  ?DEBUG("message received from RabbitMQ ~s, ~s,\n ~p", [Exchange, RoutingKey, Payload]),
  case RoutingKey of
    <<"reply.", Key/binary>> ->
      Topic = binary:replace(Key, <<".">>, <<"/">>, [global]),
      emqttd:publish(emqttd_message:make(undefined, 2, <<"v1/", Topic/binary>>, Payload));
    _ ->
      ok
  end,
  amqp_channel:cast(State#state.channel, #'basic.ack'{delivery_tag = Tag}),
  {noreply, State};

handle_info(_, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.
