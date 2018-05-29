-module(emq_amqp_plugin).

-behaviour(gen_server).

-include_lib("emqttd/include/emqttd.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-include("emq_amqp.hrl").
-include("emq_amqp_cli.hrl").

%% API
-export([start_link/1, load/0,unload/0]).

%% GenServer API
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3]).

% EMQ Hooks
-export([on_client_connected/3,on_client_disconnected/3]).
-export([on_client_subscribe/4,on_client_unsubscribe/4]).
-export([on_message_publish/2,on_message_delivered/4,on_message_acked/4]).

-export([load_client_routes/1, load_message_routes/1]).

%% For eunit tests
-export([start/0, stop/0]).

-record(routes, {
  client_connect :: list(emq_amqp_client_route),
  client_disconnect :: list(emq_amqp_client_route),
  client_subscribe :: list(emq_amqp_client_route),
  client_unsubscribe :: list(emq_amqp_client_route),
  message_publish :: list(emq_amqp_message_route)
}).

-define(ROUTER, ?MODULE).

%%====================================================================
%% API
%%====================================================================

-spec(start_link(Routes::term()) ->
  {ok, Pid :: pid()} |
  ignore |
  {error, Reason :: term()}
).
start_link(Routes) ->
  gen_server:start_link({local, ?ROUTER}, ?MODULE, Routes, []).

-spec(load() -> ok).
load() ->
  install_hooks(),
  ok.

-spec(unload() -> ok).
unload() ->
  remove_hooks(),
  ok.


%%====================================================================
%% GenServer
%%====================================================================

-spec(init(Args :: term()) ->
  {ok, State :: #routes{}} |
  {ok, State :: #routes{}, timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore
).
init(Routes) ->
  ClientRoutes  = proplists:get_value(client, Routes, []),
  MessageRoutes = proplists:get_value(message, Routes, []),
  {ok, #routes{
    client_connect      = load_client_routes(proplists:get_value(connect, ClientRoutes, [])),
    client_disconnect   = load_client_routes(proplists:get_value(disconnect, ClientRoutes, [])),
    client_subscribe    = load_client_routes(proplists:get_value(subscribe, ClientRoutes, [])),
    client_unsubscribe  = load_client_routes(proplists:get_value(unsubscribe, ClientRoutes, [])),
    message_publish     = load_message_routes(proplists:get_value(publish, MessageRoutes, []))
  }}.

%%--------------------------------------------------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: #routes{}) ->
  {reply, Reply :: term(), NewState :: #routes{}} |
  {reply, Reply :: term(), NewState :: #routes{}, timeout() | hibernate} |
  {noreply, NewState :: #routes{}} |
  {noreply, NewState :: #routes{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #routes{}} |
  {stop, Reason :: term(), NewState :: #routes{}}).

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_cast(Request :: term(), State :: #routes{}) ->
  {noreply, NewState :: #routes{}} |
  {noreply, NewState :: #routes{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #routes{}}).

handle_cast({on_client_connected, _Client}, #routes{client_connect = []} = State) ->
  {noreply, State};
handle_cast({on_client_connected, Client}, #routes{client_connect = Routes} = State) ->
  #mqtt_client{
    client_id    = ClientId,
    connected_at = Timestamp,
    peername     = {Ip, Port}
  } = Client,
  ?DEBUG("client ~p, ~p:~p connected", [ClientId, Ip, Port]),
  lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
    emq_amqp_client:publish(<<Exchange/binary>>, <<"client.", ClientId/binary, ".connected">>, #{
      <<"deviceId">> => ClientId,
      <<"protocol">> => <<"mqtt">>,
      <<"login">> => timestamp_to_milliseconds(Timestamp),
      <<"ip">> => list_to_binary(inet:ntoa(Ip)),
      <<"port">> => Port
    })
  end, Routes),
  {noreply, State};

handle_cast({on_client_disconnected, _Reason, _Client}, #routes{client_disconnect = []} = State) ->
  {noreply, State};
handle_cast({on_client_disconnected, Reason, Client}, #routes{client_disconnect = Routes} = State) ->
  #mqtt_client{
    client_id    = ClientId,
    connected_at = Timestamp,
    peername     = {Ip, Port}
  } = Client,
  ?DEBUG("client ~p, ~p:~p disconnected, reason ~p", [ClientId, Ip, Port, Reason]),
  lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
    emq_amqp_client:publish(<<Exchange/binary>>, <<"client.", ClientId/binary, ".disconnected">>, #{
      <<"deviceId">> => ClientId,
      <<"protocol">> => <<"mqtt">>,
      <<"login">> => timestamp_to_milliseconds(Timestamp),
      <<"ip">> => list_to_binary(inet:ntoa(Ip)),
      <<"port">> => Port
    })
  end, Routes),
  {noreply, State};

handle_cast({on_client_subscribe, _ClientId, _Username, _TopicTable}, #routes{client_subscribe = []} = State) ->
  {noreply, State};
handle_cast({on_client_subscribe, ClientId, Username, TopicTable}, #routes{client_subscribe = Routes} = State) ->
  ?DEBUG("client(~s/~s) subscribes: ~p~n", [Username, ClientId, TopicTable]),
  lists:foreach(fun({Topic, _}) ->
    lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
      emq_amqp_client:publish(<<Exchange/binary>>, <<"client.", ClientId/binary, ".subscribed">>, #{
        <<"deviceId">> => ClientId,
        <<"protocol">> => <<"mqtt">>,
        <<"topic">> => Topic,
        <<"timestamp">> => erlang:system_time(1000)
      })
    end, Routes)
  end, TopicTable),
  {noreply, State};

handle_cast({on_client_unsubscribe, _ClientId, _Username, _TopicTable}, #routes{client_unsubscribe = []} = State) ->
  {noreply, State};
handle_cast({on_client_unsubscribe, ClientId, Username, TopicTable}, #routes{client_unsubscribe = Routes} = State) ->
  ?DEBUG("client(~s/~s) unsubscribe ~p~n", [ClientId, Username, TopicTable]),
  lists:foreach(fun({Topic, _}) ->
    lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
      emq_amqp_client:publish(<<Exchange/binary>>, <<"client.", ClientId/binary, ".unsubscribe">>, #{
        <<"deviceId">> => ClientId,
        <<"protocol">> => <<"mqtt">>,
        <<"topic">> => Topic,
        <<"timestamp">> => erlang:system_time(1000)
      })
    end, Routes)
  end, TopicTable),
  {noreply, State};

handle_cast({on_message_publish, _Message}, #routes{message_publish = []} = State) ->
  {noreply, State};
handle_cast({on_message_publish, Message}, #routes{message_publish = Routes} = State) ->
  #mqtt_message{
    from = {ClientId, _Username},
    payload = Payload,
    topic = Topic
  } = Message,
  case (lists:filter(fun({filter = Filter}) -> emqttd_topic:match(Topic, Filter) end, Routes)) of
    [] -> {noreply, State};
    MatchedRoutes ->
      ?DEBUG("publish ~s~n", [emqttd_message:format(Message)]),
      Msg = #{
        <<"deviceId">> => ClientId,
        <<"info">> => jiffy:decode(Payload),
        <<"timestamp">> => erlang:system_time(1000)
      },
      RoutingKey = binary:replace(Topic, <<"/">>, <<".">>, [global]),
      lists:foreach(fun(#emq_amqp_message_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
        emq_amqp_client:publish(<<Exchange/binary>>, RoutingKey, Msg)
      end, MatchedRoutes),

      {noreply, State}
  end;

handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_info(Info :: timeout() | term(), State :: #routes{}) ->
  {noreply, NewState :: #routes{}} |
  {noreply, NewState :: #routes{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #routes{}}).

handle_info(_, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%====================================================================
% EMQ Hooks
%%====================================================================

-spec(on_client_connected(ConnAck:: non_neg_integer(), Client:: mqtt_client(), Env:: term()) -> {ok, mqtt_client()}).
on_client_connected(?CONNACK_ACCEPT, Client, _) ->
  gen_server:cast(?ROUTER, {on_client_connected, Client}),
  {ok, Client};
on_client_connected(_ConnAck, Client, _) ->
  {ok, Client#mqtt_client{client_id = undefined}}.

on_client_disconnected(Reason, Client, _) ->
  gen_server:cast(?ROUTER, {on_client_disconnected, Reason, Client}),
  ok.

on_client_subscribe(ClientId, Username, TopicTable, _) ->
  gen_server:cast(?ROUTER, {on_client_subscribe, ClientId, Username, TopicTable}),
  ok.

on_client_unsubscribe(ClientId, Username, TopicTable, _Routes) ->
  gen_server:cast(?ROUTER, {on_client_subscribe, ClientId, Username, TopicTable}),
  {ok, TopicTable}.

-spec(on_message_publish(Message:: mqtt_message(), Env:: term()) -> {ok, mqtt_message()}).
on_message_publish(Message = #mqtt_message{topic = <<"$SYS/", _/binary>>}, _) ->
  {ok, Message};
on_message_publish(Message = #mqtt_message{payload = <<>>}, _) ->
  {ok, Message};
on_message_publish(Message = #mqtt_message{}, _) ->
  gen_server:cast(?ROUTER, {on_message_publish, Message}),
  {ok, Message};
on_message_publish(Message, _) ->
  {ok, Message}.

on_message_delivered(ClientId, Username, Message, _) ->
  ?DEBUG("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
  {ok, Message}.

on_message_acked(ClientId, Username, Message, _Routes) ->
  ?DEBUG("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
  {ok, Message}.


start() ->
  gen_server:start({local, ?ROUTER}, ?MODULE, [], []).

stop() ->
  gen_server:call(?ROUTER, stop).


%%====================================================================
%% Internal functions
%%====================================================================

install_hooks() ->
  emqttd:hook('client.connected',       fun ?MODULE:on_client_connected/3,    []),
  emqttd:hook('client.disconnected',    fun ?MODULE:on_client_disconnected/3, []),
  emqttd:hook('client.subscribe',       fun ?MODULE:on_client_subscribe/4,    []),
  emqttd:hook('client.unsubscribe',     fun ?MODULE:on_client_unsubscribe/4,  []),
  emqttd:hook('message.publish',        fun ?MODULE:on_message_publish/2,     []),
  emqttd:hook('message.delivered',      fun ?MODULE:on_message_delivered/4,   []),
  emqttd:hook('message.acked',          fun ?MODULE:on_message_acked/4,       []).

remove_hooks() ->
  emqttd:unhook('client.connected',     fun ?MODULE:on_client_connected/3),
  emqttd:unhook('client.disconnected',  fun ?MODULE:on_client_disconnected/3),
  emqttd:unhook('client.subscribe',     fun ?MODULE:on_client_subscribe/4),
  emqttd:unhook('client.unsubscribe',   fun ?MODULE:on_client_unsubscribe/4),
  emqttd:unhook('message.publish',      fun ?MODULE:on_message_publish/2),
  emqttd:unhook('message.delivered',    fun ?MODULE:on_message_delivered/4),
  emqttd:unhook('message.acked',        fun ?MODULE:on_message_acked/4).

%%timestamp_to_seconds({Mega, Sec, Micro}) ->
%%  (Mega * 1000000 + Sec).

timestamp_to_milliseconds({Mega, Sec, Micro}) ->
  (Mega * 1000000 + Sec) * 1000 + round(Micro / 1000).

-spec(load_client_routes(RoutesSpecs :: list()) -> Routes :: emq_amqp_client_routes()).
load_client_routes(RoutesSpecs) ->
  lists:reverse(load_client_routes(RoutesSpecs, [])).
load_client_routes([], Routes) ->
  Routes;
load_client_routes([{Id, [{exchange, Destination}]} | T], Routes) ->
  case string:tokens(Destination, ":") of
    [Type, Exchange] ->
      load_client_routes(T, [#emq_amqp_client_route{
        id = Id,
        destination = #emq_amqp_exchange{
          type  = list_to_binary(Type),
          exchange  = list_to_binary(Exchange)
        }
      } | Routes]);
    _ ->
      ?ERROR("Invalid client route destination: ~p", [Destination]),
      load_client_routes(T, Routes)
  end.

-spec(load_message_routes(RoutesSpecs :: list()) -> Routes :: emq_amqp_message_routes()).
load_message_routes(RoutesSpecs) ->
  lists:reverse(load_message_routes(RoutesSpecs, [])).
load_message_routes([], Routes) ->
  Routes;
load_message_routes([{Id, [{topic, Topic}, {exchange, Destination}]} | T], Routes) ->
  case string:tokens(Destination, ":") of
    [Type, Exchange] ->
      load_message_routes(T, [#emq_amqp_message_route{
        id = Id,
        filter = Topic,
        destination = #emq_amqp_exchange{
          type     = list_to_binary(Type),
          exchange = list_to_binary(Exchange)
        }
      } | Routes]);
    _ ->
      ?ERROR("Invalid message route destination: ~p", [Destination]),
      load_message_routes(T, Routes)
  end.
