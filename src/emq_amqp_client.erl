-module(emq_amqp_client).

-behaviour(gen_server).

%% API
-export([start_link/0, publish/3, declare_exchange/2]).

%% GenServer API
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.


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

-spec(declare_exchange(Exchange:: (binary() | list() | atom()), Type:: (binary() | list() | atom())) -> ok).
declare_exchange(Exchange, Type) when is_binary(Exchange), is_binary(Type) ->
  gen_server:call(?MODULE, {declare_exchange, Exchange, Type});
declare_exchange(Exchange, Type) when is_atom(Exchange) -> declare_exchange(atom_to_binary(Exchange, utf8), Type);
declare_exchange(Exchange, Type) when is_list(Exchange) -> declare_exchange(list_to_binary(Exchange), Type);
declare_exchange(Exchange, Type) when is_atom(Type) -> declare_exchange(Exchange, atom_to_binary(Type, utf8));
declare_exchange(Exchange, Type) when is_list(Type) -> declare_exchange(Exchange, list_to_binary(Type)).

-spec(publish(Exchange:: binary() | bitstring(), RoutingKey:: binary(), Msg:: term() | iodata() | binary()) -> ok).
publish(Exchange, RoutingKey, Msg) when is_binary(Msg) ->
  gen_server:cast(?MODULE, {publish, Exchange, RoutingKey, Msg}),
  ok;
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

handle_call({declare_exchange, Exchange, Type}, _From, State) ->
  ?DEBUG("declare_exchanges ~p:=~p~n", [Type, Exchange]),
  amqp_channel:call(State#state.channel, #'exchange.declare'{
    exchange = Exchange,
    durable  = true,
    type     = Type
  }),
  {ok, State};

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
