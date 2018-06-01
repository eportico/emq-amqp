-module(emq_amqp_plugin_SUITE).

-include_lib("emqttd/include/emqttd.hrl").
-include_lib("emqttd/include/emqttd_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emq_amqp.hrl").

%% Test server callbacks
-export([suite/0, all/0, groups/0,
  init_per_suite/1, end_per_suite/1, init_per_group/2, end_per_group/2, init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([load_client_routes/1, load_message_routes/1]).
-export([on_client_connected/1, on_client_disconnected/1, on_client_subscribe/1, on_client_unsubscribe/1, on_client_disconnected_auth_failure/1]).
-export([on_message_publish_match/1, on_message_publish_mismatch/1, on_message_publish_mixed/1, on_message_publish_several_matches/1]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() -> [
  {timetrap,{minutes,1}}
].

all() -> [
  {group, api},
  {group, gen_server_client},
  {group, gen_server_message}
].

groups() -> [
  {api, [sequence], [
    load_client_routes,
    load_message_routes
  ]},
  {gen_server_client, [sequence], [
    on_client_connected,
    on_client_disconnected,
    on_client_disconnected_auth_failure,
    on_client_subscribe,
    on_client_unsubscribe
  ]},
  {gen_server_message, [sequence], [
    on_message_publish_match,
    on_message_publish_mismatch,
    on_message_publish_mixed,
    on_message_publish_several_matches
  ]}
].

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.


init_per_group(gen_server_client, Config) ->
  Client = #mqtt_client{
    client_id    = <<"ClientId">>,
    username     = <<"Username">>,
    connected_at = os:timestamp(),
    peername     = {{127,0,0,1}, 80}
  },
  [{mqtt_client, Client } | Config];
init_per_group(gen_server_message, Config) ->
  Message = #mqtt_message{
    from = {<<"ClientId">>, <<"Username">>},
    payload = <<"{\"foo\": \"bar\"}">>, %TODO Jiffy
    topic = <<"v1/topic">>
  },
  [{mqtt_message, Message } | Config];
init_per_group(_GroupName, Config) ->
  Config.

end_per_group(_GroupName, Config) ->
  Config.


init_per_testcase(_TestCase, Config) ->
  meck:new(emq_amqp_client),
  Config.

end_per_testcase(_TestCase, Config) ->
  meck:unload(emq_amqp_client),
  Config.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

load_client_routes(_Config) ->
  Res = emq_amqp_plugin:load_client_routes([
    {1, [{exchange, "direct:emq.direct"}]},
    {2, [{exchange, "topic:emq.topic"}]},
    {3, [{exchange, "fanout:emq.fanout"}]}
  ]),
  ?assertMatch([
    #emq_amqp_client_route{
      id = 1,
      destination = #emq_amqp_exchange{
        type = <<"direct">>,
        exchange = <<"emq.direct">>
      }
    }, #emq_amqp_client_route{
      id = 2,
      destination = #emq_amqp_exchange{
        type = <<"topic">>,
        exchange = <<"emq.topic">>
      }
    }, #emq_amqp_client_route{
      id = 3,
      destination = #emq_amqp_exchange{
        type = <<"fanout">>,
        exchange = <<"emq.topic">>
      }
    }
  ], Res).

load_message_routes(_Config) ->
  Res = emq_amqp_plugin:load_message_routes([
    {1, [{topic, "$SYS/#"}, {exchange, "direct:emq.$sys"}]},
    {2, [{topic, "#"}, {exchange, "topic:emq.pub"}]}
  ]),
  ?assertMatch([
    #emq_amqp_message_route{
      id = 1,
      filter = <<"$SYS/#">>,
      destination = #emq_amqp_exchange{
        type = <<"direct">>,
        exchange = <<"emq.$sys">>
      }
    }, #emq_amqp_message_route{
      id = 2,
      filter = <<"#">>,
      destination = #emq_amqp_exchange{
        type = <<"topic">>,
        exchange = <<"emq.pub">>
      }
    }
  ], Res).

%%--------------------------------------------------------------------

on_client_connected(Config) ->
  Routes = [#emq_amqp_client_route{id = 1, destination = #emq_amqp_exchange{type = <<"direct">>, exchange = <<"emq.connection">>}}],
  Client = ?config(mqtt_client, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_client_connected -> ~p:~p~nMsg:~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_client_connected, Client}, #{client_connect => Routes}),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.connection">>, <<"client.ClientId.connected">>, '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_client_disconnected(Config) ->
  Routes = [#emq_amqp_client_route{id = 1, destination = #emq_amqp_exchange{type = <<"direct">>, exchange = <<"emq.connection">>}}],
  Client = ?config(mqtt_client, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_client_disconnected -> ~p:~p~nMsg:~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_client_disconnected, goodbye, Client}, #{client_disconnect => Routes}),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.connection">>, <<"client.ClientId.disconnected">>, '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_client_disconnected_auth_failure(Config) ->
  Routes = [#emq_amqp_client_route{id = 1, destination = #emq_amqp_exchange{type = <<"direct">>, exchange = <<"emq.connection">>}}],
  Client = ?config(mqtt_client, Config),
  emq_amqp_plugin:handle_cast({on_client_disconnected, auth_failure, Client}, #{client_disconnect => Routes}),
  ?assertNot(meck:called(emq_amqp_client, publish, [<<"emq.connection">>, <<"client.ClientId.disconnected">>, '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_client_subscribe(_Config) ->
  Routes = [#emq_amqp_client_route{id = 1, destination = #emq_amqp_exchange{type = <<"direct">>, exchange = <<"emq.subscription">>}}],
  TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_client_subscribe -> ~p:~p~nMsg:~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_client_subscribe, <<"ClientId">>, <<"Username">>, TopicTable}, #{client_subscribe => Routes}),
  ?assertEqual(3, meck:num_calls(emq_amqp_client, publish, [<<"emq.subscription">>, <<"client.ClientId.subscribed">>, '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_client_unsubscribe(_Config) ->
  Routes = [#emq_amqp_client_route{id = 1, destination = #emq_amqp_exchange{type = <<"direct">>, exchange = <<"emq.subscription">>}}],
  TopicTable = [{<<"TopicQos0">>, ?QOS_0}, {<<"TopicQos1">>, ?QOS_1}, {<<"TopicQos2">>, ?QOS_2}],
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_client_unsubscribe -> ~p:~p~nMsg:~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_client_unsubscribe, <<"ClientId">>, <<"Username">>, TopicTable}, #{client_unsubscribe => Routes}),
  ?assertEqual(3, meck:num_calls(emq_amqp_client, publish, [<<"emq.subscription">>, <<"client.ClientId.unsubscribe">>, '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

%%--------------------------------------------------------------------

on_message_publish_match(Config) ->
  Routes = [#emq_amqp_message_route{id = 1, filter = <<"#">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.pub">>}}],
  Message = ?config(mqtt_message, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_message_publish -> ~p:~p~n~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_message_publish, Message}, #{message_publish => Routes}),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.pub">>, '_', '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_message_publish_mismatch(Config) ->
  Routes = [#emq_amqp_message_route{id = 1, filter = <<"v2/#">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.pub">>}}],
  Message = ?config(mqtt_message, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_message_publish -> ~p:~p~n~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_message_publish, Message}, #{message_publish => Routes}),
  ?assertNot(meck:called(emq_amqp_client, publish, [<<"emq.pub">>, '_', '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_message_publish_mixed(Config) ->
  Routes = [
    #emq_amqp_message_route{id = 1, filter = <<"v2/#">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.v2">>}},
    #emq_amqp_message_route{id = 1, filter = <<"#">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.pub">>}}
  ],
  Message = ?config(mqtt_message, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_message_publish -> ~p:~p~n~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_message_publish, Message}, #{message_publish => Routes}),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.pub">>, '_', '_'])),
  ?assertNot(meck:called(emq_amqp_client, publish, [<<"emq.v2">>, '_', '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

on_message_publish_several_matches(Config) ->
  Routes = [
    #emq_amqp_message_route{id = 1, filter = <<"v1/topic">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.v1">>}},
    #emq_amqp_message_route{id = 1, filter = <<"#">>, destination = #emq_amqp_exchange{type = <<"topic">>, exchange = <<"emq.pub">>}}
  ],
  Message = ?config(mqtt_message, Config),
  meck:expect(emq_amqp_client, publish, fun(Exchange, RoutingKey, Msg) -> ct:print("on_message_publish -> ~p:~p~n~p", [Exchange, RoutingKey, Msg]), ok end),
  emq_amqp_plugin:handle_cast({on_message_publish, Message}, #{message_publish => Routes}),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.pub">>, '_', '_'])),
  ?assert(meck:called(emq_amqp_client, publish, [<<"emq.v1">>, '_', '_'])),
  ?assert(meck:validate(emq_amqp_client)),
  ok.

