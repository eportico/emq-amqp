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

-ifdef(TEST).
-compile(export_all).
-endif.

-type routes() :: map().

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
  {ok, State :: routes()} |
  {ok, State :: routes(), timeout() | hibernate} |
  {stop, Reason :: term()} |
  ignore
).
init(Routes) ->
  ClientRoutes  = proplists:get_value(client, Routes, []),
  MessageRoutes = proplists:get_value(message, Routes, []),
  {ok, #{
    client_connect      => load_client_routes(proplists:get_value(connect, ClientRoutes, [])),
    client_disconnect   => load_client_routes(proplists:get_value(disconnect, ClientRoutes, [])),
    client_subscribe    => load_client_routes(proplists:get_value(subscribe, ClientRoutes, [])),
    client_unsubscribe  => load_client_routes(proplists:get_value(unsubscribe, ClientRoutes, [])),
    message_publish     => load_message_routes(proplists:get_value(publish, MessageRoutes, []))
  }}.

%%--------------------------------------------------------------------

-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()}, State :: routes()) ->
  {reply, Reply :: term(), NewState :: routes()} |
  {reply, Reply :: term(), NewState :: routes(), timeout() | hibernate} |
  {noreply, NewState :: routes()} |
  {noreply, NewState :: routes(), timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: routes()} |
  {stop, Reason :: term(), NewState :: routes()}).

handle_call(stop, _From, State) ->
  {stop, normal, ok, State};

handle_call(_Request, _From, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_cast(Request :: term(), State :: routes()) ->
  {noreply, NewState :: routes()} |
  {noreply, NewState :: routes(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: routes()}).

handle_cast({on_client_connected, _Client}, #{client_connect := []} = State) ->
  {noreply, State};
handle_cast({on_client_connected, Client}, #{client_connect := Routes} = State) ->
  #mqtt_client{
    client_id    = ClientId,
    username     = Username,
    connected_at = Timestamp,
    peername     = {Ip, Port}
  } = Client,
  ?DEBUG("client ~p, ~p:~p connected", [ClientId, Ip, Port]),
  lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
    emq_amqp_client:publish(
      Exchange,
      <<"client.", ClientId/binary, ".connected">>,
      #{
        <<"deviceId">> => ClientId,
        <<"userId">>   => Username,
        <<"protocol">> => <<"mqtt">>,
        <<"login">>    => timestamp_to_milliseconds(Timestamp),
        <<"ip">>       => list_to_binary(inet:ntoa(Ip)),
        <<"port">>     => Port
      }
    )
  end, Routes),
  {noreply, State};
handle_cast({on_client_connected, _Client}, State) ->
  ?DEBUG("Routes not configured for on_client_connected"),
  {noreply, State};

handle_cast({on_client_disconnected, _Reason, _Client}, #{client_disconnect := []} = State) ->
  {noreply, State};
handle_cast({on_client_disconnected, auth_failure, _Client}, State) ->
  {noreply, State};
handle_cast({on_client_disconnected, {shutdown, Reason}, Client}, State) ->
  gen_server:cast(?ROUTER, {on_client_disconnected, Reason, Client}),
  {noreply, State};
handle_cast({on_client_disconnected, Reason, Client}, #{client_disconnect := Routes} = State) when is_atom(Reason) ->
  #mqtt_client{
    client_id    = ClientId,
    username     = Username,
    connected_at = Timestamp,
    peername     = {Ip, Port}
  } = Client,
  ?DEBUG("client ~p:~p, ~p:~p disconnected, reason ~p", [ClientId, Username, Ip, Port, Reason]),
  lists:foreach(fun(#emq_amqp_client_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
    emq_amqp_client:publish(
      <<Exchange/binary>>,
      <<"client.", ClientId/binary, ".disconnected">>,
      #{
        <<"deviceId">> => ClientId,
        <<"userId">>   => Username,
        <<"protocol">> => <<"mqtt">>,
        <<"login">>    => timestamp_to_milliseconds(Timestamp),
        <<"ip">>       => list_to_binary(inet:ntoa(Ip)),
        <<"port">>     => Port,
        <<"reason">>   => atom_to_binary(Reason, utf8)
      }
    )
  end, Routes),
  {noreply, State};
handle_cast({on_client_disconnected, Reason, _Client}, #{client_disconnect := _Routes} = State) ->
  ?ERROR("Client disconnected, cannot encode reason: ~p", [Reason]),
  {noreply, State};
handle_cast({on_client_disconnected, _Reason, _Client}, State) ->
  ?DEBUG("Routes not configured for on_client_disconnected"),
  {noreply, State};

handle_cast({on_client_subscribe, _ClientId, _Username, _TopicTable}, #{client_subscribe := []} = State) ->
  {noreply, State};
handle_cast({on_client_subscribe, ClientId, Username, TopicTable}, #{client_subscribe := Routes} = State) ->
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
handle_cast({on_client_subscribe, _ClientId, _Username, _TopicTable}, State) ->
  ?DEBUG("Routes not configured for on_client_subscribe"),
  {noreply, State};

handle_cast({on_client_unsubscribe, _ClientId, _Username, _TopicTable}, #{client_unsubscribe := []} = State) ->
  {noreply, State};
handle_cast({on_client_unsubscribe, ClientId, Username, TopicTable}, #{client_unsubscribe := Routes} = State) ->
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
handle_cast({on_client_unsubscribe, _ClientId, _Username, _TopicTable}, State) ->
  ?DEBUG("Routes not configured for on_client_unsubscribe"),
  {noreply, State};

handle_cast({on_message_publish, _Message}, #{message_publish := []} = State) ->
  {noreply, State};
handle_cast({on_message_publish, #mqtt_message{payload = <<>>}}, State) ->
  {noreply, State};
handle_cast({on_message_publish, Message}, #{message_publish := Routes} = State) ->
  #mqtt_message{
    from = {ClientId, _Username},
    payload = Payload,
    topic = Topic
  } = Message,
  case (lists:filter(fun(#emq_amqp_message_route{filter = Filter}) -> emqttd_topic:match(Topic, Filter) end, Routes)) of
    [] -> {noreply, State};
    MatchedRoutes ->
      ?DEBUG("publish ~s~n", [emqttd_message:format(Message)]),
      Msg = #{
        <<"deviceId">> => ClientId,
        <<"timestamp">> => erlang:system_time(1000),
        <<"info">> => try jiffy:decode(Payload) of
                        Res -> Res
                      catch
                        throw:{error, {_, invalid_json}} -> Payload
                      end
      },
      RoutingKey = binary:replace(Topic, <<"/">>, <<".">>, [global]),
      lists:foreach(fun(#emq_amqp_message_route{destination = #emq_amqp_exchange{exchange = Exchange}}) ->
        emq_amqp_client:publish(<<Exchange/binary>>, RoutingKey, Msg)
      end, MatchedRoutes),

      {noreply, State}
  end;
handle_cast({on_message_publish, _Message}, State) ->
  ?DEBUG("Routes not configured for on_message_publish"),
  {noreply, State};

handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

-spec(handle_info(Info :: timeout() | term(), State :: routes()) ->
  {noreply, NewState :: routes()} |
  {noreply, NewState :: routes(), timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: routes()}).

handle_info(_, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.


%%====================================================================
% EMQ Hooks
%%====================================================================

-spec on_client_connected(ConnAck:: non_neg_integer(), Client:: mqtt_client(), Env:: term()) ->
  {ok, mqtt_client()}.
on_client_connected(?CONNACK_ACCEPT, Client, _) ->
  gen_server:cast(?ROUTER, {on_client_connected, Client}),
  {ok, Client};
on_client_connected(_ConnAck, Client, _) ->
  {ok, Client#mqtt_client{client_id = undefined}}.

-spec on_client_disconnected(Reason:: term(), Client:: mqtt_client(), Env:: term()) ->
  ok.
on_client_disconnected(Reason, Client, _) ->
  gen_server:cast(?ROUTER, {on_client_disconnected, Reason, Client}),
  ok.

-spec on_client_subscribe(ClientId:: string(), Username:: string(), TopicTable:: [{binary(), term()}], Env:: term()) ->
  ok.
on_client_subscribe(ClientId, Username, TopicTable, _Env) ->
  gen_server:cast(?ROUTER, {on_client_subscribe, ClientId, Username, TopicTable}),
  ok.

-spec on_client_unsubscribe(ClientId:: string(), Username:: string(), TopicTable:: [{binary(), term()}], Env:: term()) ->
  ok.
on_client_unsubscribe(ClientId, Username, TopicTable, _Env) ->
  gen_server:cast(?ROUTER, {on_client_subscribe, ClientId, Username, TopicTable}),
  {ok, TopicTable}.

-spec on_message_publish(Message:: mqtt_message(), Env:: term()) ->
  {ok, mqtt_message()}.
on_message_publish(Message = #mqtt_message{}, _Env) ->
  gen_server:cast(?ROUTER, {on_message_publish, Message}),
  {ok, Message};
on_message_publish(Message, _) ->
  {ok, Message}.

-spec on_message_delivered(ClientId:: string(), Username:: string(), Message:: mqtt_message(), Env:: term()) ->
  {ok, mqtt_message()}.
on_message_delivered(ClientId, Username, Message, _Env) ->
  ?DEBUG("delivered to client(~s/~s): ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
  {ok, Message}.

-spec on_message_acked(ClientId:: string(), Username:: string(), Message:: mqtt_message(), Env:: term()) ->
  {ok, mqtt_message()}.
on_message_acked(ClientId, Username, Message, _Env) ->
  ?DEBUG("client(~s/~s) acked: ~s~n", [Username, ClientId, emqttd_message:format(Message)]),
  {ok, Message}.


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

-spec timestamp_to_milliseconds({integer(), integer(), integer()}) ->
  integer().
timestamp_to_milliseconds({Mega, Sec, Micro}) ->
  (Mega * 1000000 + Sec) * 1000 + round(Micro / 1000).


-spec load_client_routes(RoutesSpecs :: list()) ->
  [emq_amqp_client_route()].
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

-spec(load_message_routes(RoutesSpecs :: list()) ->
  [emq_amqp_message_route()]).
load_message_routes(RoutesSpecs) ->
  lists:reverse(load_message_routes(RoutesSpecs, [])).
load_message_routes([], Routes) ->
  Routes;
load_message_routes([{Id, [{topic, Topic}, {exchange, Destination}]} | T], Routes) ->
  case string:tokens(Destination, ":") of
    [Type, Exchange] ->
      load_message_routes(T, [#emq_amqp_message_route{
        id = Id,
        filter = list_to_binary(Topic),
        destination = #emq_amqp_exchange{
          type     = list_to_binary(Type),
          exchange = list_to_binary(Exchange)
        }
      } | Routes]);
    _ ->
      ?ERROR("Invalid message route destination: ~p", [Destination]),
      load_message_routes(T, Routes)
  end.
