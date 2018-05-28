%%%-------------------------------------------------------------------
%% @doc EMQ-AMQP public API
%% @end
%%%-------------------------------------------------------------------

-module(emq_amqp_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([extract/2, declare_exchanges/1]). %%TODO: Remove

-include("emq_amqp.hrl").
-include("emq_amqp_cli.hrl").

%%--------------------------------------------------------------------
%% Application Callbacks
%%--------------------------------------------------------------------

start(_StartType, _StartArgs) ->
  lager:start(),
  print_banner(),
  {ok, Sup} = emq_amqp_sup:start_link(),

  {ok, Routes} = application:get_env(?APP, events),

  start_server(Sup, {"amqp client", emq_amqp_client}),
  start_server(Sup, {"emqtt-amqp router", emq_amqp_plugin, {Routes}}),

  declare_exchanges(emq_amqp_app:extract(exchange, Routes)),

  emq_amqp_plugin:load(),
  print_vsn(),
  {ok, Sup}.

stop(_State) ->
  ok = emq_amqp_plugin:unload(),
  ok.

%%--------------------------------------------------------------------

print_banner() ->
  ?INFO("starting ~s on node '~s'~n", [?APP, node()]).

print_vsn() ->
  {ok, Vsn} = application:get_key(vsn),
  ?INFO("~s ~s is running now~n", [?APP, Vsn]).

%%--------------------------------------------------------------------

declare_exchanges([]) -> ok;
declare_exchanges([H|T]) when is_list(H) ->
  declare_exchanges(lists:map(fun(V) -> list_to_binary(V) end, string:tokens(H, ":")), T);
declare_exchanges([H|T]) ->
  ?ERROR("unrecognized exchange declaration: ~p~n", [H]),
  declare_exchanges(T).

declare_exchanges([Type, Exchange], T) when is_binary(Type), is_binary(Exchange) ->
  emq_amqp_client:declare_exchange(Exchange, Type),
  declare_exchanges(T);
declare_exchanges(H, T) ->
  ?ERROR("unrecognized exchange declaration: ~p~n", [H]),
  declare_exchanges(T).

%%--------------------------------------------------------------------
%% Start Servers
%%--------------------------------------------------------------------

start_server(Sup, {Name, Server, Args}) ->
  ?INFO("~s is starting...", [Name]),
  start_child(Sup, Server, Args),
  ?INFO("[ok]~n");

start_server(Sup, {Name, Server}) ->
  ?INFO("~s is starting...", [Name]),
  start_child(Sup, Server),
  ?INFO("[ok]~n").

start_child(Sup, Module, Args) when is_atom(Module) ->
  {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Module, Args)).
start_child(Sup, Module) when is_atom(Module) ->
  {ok, _ChiId} = supervisor:start_child(Sup, worker_spec(Module)).

worker_spec(Module, Args) when is_atom(Module) ->
  worker_spec(Module, start_link, Args).
worker_spec(Module) when is_atom(Module) ->
  worker_spec(Module, start_link, []).
worker_spec(M, F, A) ->
  {M, {M, F, A}, permanent, 10000, worker, [M]}.


%%--------------------------------------------------------------------
%% Extract values from configuration hierarchies
%%--------------------------------------------------------------------

extract(Key, Data) ->
  remove_duplicates(extract(Key, Data, [])).
extract(_Key, [], Acc) ->
  Acc;
extract(Key, {Key,V}, Acc) ->
  [V|Acc];
extract(Key, [{Key,V}|Fields], Acc) ->
  extract(Key, Fields, [V|Acc]);
extract(Key, [{_,V}|Fields], Acc) when is_tuple(V); is_list(V) ->
  extract(Key, Fields, extract(Key, V, Acc));
extract(Key, Data, Acc) when is_list(Data) ->
  lists:foldl(fun(V, FoldAcc) when is_tuple(V); is_list(V) -> extract(Key, V, FoldAcc);
                 (_, FoldAcc) -> FoldAcc
              end, Acc, Data);
extract(Key, Data, Acc) when is_tuple(Data) ->
  extract(Key, tuple_to_list(Data), Acc).

remove_duplicates([])    -> [];
remove_duplicates([H|T]) -> [H | [X || X <- remove_duplicates(T), X /= H]].
