-module(emq_amqp_app_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%% Test server callbacks
%%-export([suite/0, all/0, groups/0,
-export([suite/0, all/0, init_per_suite/1, end_per_suite/1]).

%% Test cases
-export([extract/1, convert_exchange/1, declare_exchange/1]).

%%--------------------------------------------------------------------
%% COMMON TEST CALLBACK FUNCTIONS
%%--------------------------------------------------------------------

suite() -> [
  {timetrap,{minutes,1}}
].

all() -> [
  extract,
  convert_exchange,
  declare_exchange
].

init_per_suite(Config) ->
  Config.

end_per_suite(Config) ->
  Config.

%%--------------------------------------------------------------------
%% TEST CASES
%%--------------------------------------------------------------------

extract(_Config) ->
  Args = [
    {client, [
      {connect, [{1, [{exchange, "direct:emq.connection"}]}]},
      {disconnect, [{1, [{exchange, "direct:emq.connection"}]}]},
      {subscribe, [{1, [{exchange, "direct:emq.subscription"}]}]},
      {unsubscribe, [{1, [{exchange, "direct:emq.subscription"}]}]}
    ]},
    {message, [
      {publish, [
        {1, [{topic, "$SYS/#"}, {exchange, "topic:emq.$sys"}]},
        {2, [{topic, "#"}, {exchange, "topic:emq.pub"}]}
      ]}
    ]}
  ],
  Res = emq_amqp_app:extract(exchange, Args),
  ?assertMatch(["direct:emq.connection", "direct:emq.subscription", "topic:emq.$sys", "topic:emq.pub"], Res),
  ?assertNotMatch(["direct:emq.connection", "direct:emq.connection", "direct:emq.subscription", "direct:emq.subscription", "topic:emq.$sys", "topic:emq.pub"], Res).

convert_exchange(_Config) ->
  ?assertMatch([<<"topic">>,<<"emq.pub">>], emq_amqp_app:convert_exchange("topic:emq.pub")).

declare_exchange(_Config) ->
  meck:new(emq_amqp_client),
  meck:expect(emq_amqp_client, declare_exchange, fun(<<"emq.pub">>, <<"topic">>) -> ok end),
  ?assertEqual(ok, emq_amqp_app:declare_exchanges(["topic:emq.pub"])),
  ?assert(meck:validate(emq_amqp_client)),
  meck:unload(emq_amqp_client).
