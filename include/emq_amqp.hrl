-define(APP, emq_amqp).


-type amqp_exchange_type() :: binary().

-record(emq_amqp_exchange, {
  type :: amqp_exchange_type(),
  exchange :: binary()
}).
-type emq_amqp_exchange() :: #emq_amqp_exchange{}.


-record(emq_amqp_client_route, {
  id :: term(),
  destination :: emq_amqp_exchange()
}).
-type emq_amqp_client_route() :: #emq_amqp_client_route{}.


-record(emq_amqp_message_route, {
  id :: term(),
  filter :: binary(),
  destination :: emq_amqp_exchange()
}).
-type emq_amqp_message_route() :: #emq_amqp_message_route{}.

-define(proplist_to_record(Record), fun(Proplist) ->
  Fields = record_info(fields, Record),
  [Tag| Values] = tuple_to_list(#Record{}),
  Defaults = lists:zip(Fields, Values),
  L = lists:map(fun ({K,V}) -> proplists:get_value(K, Proplist, V) end, Defaults),
  list_to_tuple([Tag|L])
end).
