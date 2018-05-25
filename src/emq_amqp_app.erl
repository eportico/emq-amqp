%%%-------------------------------------------------------------------
%% @doc EMQ-AMQP public API
%% @end
%%%-------------------------------------------------------------------

-module(emq_amqp_app).

-behaviour(application).

-export([start/2, stop/1]).

-include("emq_amqp.hrl").
-include("emq_amqp_cli.hrl").

%%--------------------------------------------------------------------
%% Application Callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
  print_banner(),
  {ok, Sup} = emq_amqp_sup:start_link(),
  start_servers(Sup),
%%  TODO: Load routes?
  emq_amqp_plugin:load(),
  print_vsn(),
  {ok, Sup}.

stop(_State) ->
  ok = emq_amqp_plugin:unload(),
  ok.

%%--------------------------------------------------------------------
%% Print Banner
%%--------------------------------------------------------------------

print_banner() ->
  ?INFO("starting ~s on node '~s'~n", [?APP, node()]).

print_vsn() ->
  {ok, Vsn} = application:get_key(vsn),
  ?INFO("~s ~s is running now~n", [?APP, Vsn]).

%%--------------------------------------------------------------------
%% Start Servers
%%--------------------------------------------------------------------

start_servers(Sup) ->
  Servers = [{"amqp client", emq_amqp_client},
             {"emqtt-amqp router", emq_amqp_router}],
  [start_server(Sup, Server) || Server <- Servers].


start_server(Sup, {Name, Server}) ->
  ?INFO("~s is starting...", [Name]),
  start_child(Sup, Server),
  ?INFO("[ok]~n").

start_child(Sup, Module) when is_atom(Module) ->
  {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Module)).

worker_spec(Module) when is_atom(Module) ->
  worker_spec(Module, start_link, []).

worker_spec(M, F, A) ->
  {M, {M, F, A}, permanent, 10000, worker, [M]}.
