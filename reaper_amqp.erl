%% @author Shailen Karur
%% @copyright 2014 Shailen Karur
%% @doc Translates AMQP messages from JSON into more concise Erlang terms and vice versa.  Also handles AMQP routing logic.
%% Note that Mochijson2 turns JSON strings into Erlang binaries; for consistency's sake, we transform these into
%% standard Erlang strings (lists) during parsing.  If this is changed, mod_reaper.erl will need to be updated to
%% call binary_to_list on these before passing them to various ejabberd internals.
%% @reference See <a href="http://github.com/careo/rabbitmq-erlang-client-examples">these examples</a> for some more examples
%%   of working with the RabbitMQ Erlang client.

%% @author Christophe Romain <cromain@process-one.net>
%% @doc This is a rewrite by Shailen Karur of original module from Shailen Karur to work on eBE

-module(reaper_amqp).

-include("logger.hrl").
-include("reaper.hrl").

-include("amqp_client.hrl").

-behavior(gen_server).

-export([start_link/4, publish_property_values/2,
	 publish_to_reaper_manager/1, publish_online_gateways/1,
	 publish_to_sod/1, write_async_message/1, turn_msg_sink/1, dump_iq_on_error/1, show/1]).

%%% API: gen_server
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2, code_change/3]).

-record(state,
	{amqp_host, user, pass, connection, channel,
     mconnref, mchanref,
	 consumer_tag, mod_reaper_host, iod}).

-define(reaper_DATA_EXCHANGE, <<"reaper.data">>).
-define(reaper_STAGING_DATA_EXCHANGE, <<"reaper.staging.data">>).
-define(reaper_ONLINE_EXCHANGE, <<"reaper.online_gateways">>).
-define(reaper_GATEWAY_CRASH_EXCHANGE, <<"reaper.gateway.crash">>).
-define(reaper_DATA_ALARM_EXCHANGE, <<"reaper.data.alarms">>).
-define(reaper_MANAGER_QUEUE, <<"reaper_manager">>).
-define(reaper_MESSAGING_QUEUE, <<"reaper_messaging">>).
-define(reaper_ALERT_CONFIRMATION_QUEUE, <<"reaper.alert.confirmation">>).
-define(JSON_CONTENT_TYPE, <<"application/json">>).
-define(XML_CONTENT_TYPE, <<"application/xml">>).

-define(RECONNECT_TIME, 1000).

%%--------------------------------------------------------------------
%% External API
%%--------------------------------------------------------------------

start_link(AmqpHost, User, Pass, ModreaperHost) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE,
			  [AmqpHost, User, Pass, ModreaperHost], []).

publish_property_values(PropertyValues = #property_values{},
			GatewayInfo = #gateway{}) ->
    gen_server:cast(?MODULE, {publish_property_values, PropertyValues, GatewayInfo}).

publish_to_reaper_manager(Message) ->
    gen_server:cast(?MODULE, {publish_to_reaper_manager, Message}).

publish_to_sod(Message) ->
    gen_server:cast(?MODULE, {publish_to_sod, Message}).

publish_online_gateways(Gateways) ->
    gen_server:cast(?MODULE, {publish_online_gateways, Gateways}).

%% Write received messages in an asynchronous manner ...
write_async_message(Message) ->
    gen_server:cast(?MODULE, {async_write, Message}).

%% turn 'on' or 'off' msg_sink feature (copy messages)
%% Valid inputs are on | off
%% messages get logged to the Messages file only if the value is 'on'
turn_msg_sink(Flag) ->
    ets:update_element(mod_reaper_config, msg_sink, {2, Flag}).

%% read current settings of mod_reaper_config
%% input parameter is name of key
%% output is the value of the key
show(Param) ->
   case ets:lookup(mod_reaper_config, Param) of
       [{Param, Value}] -> io:format(user, "~p has value of ~p~n", [Param, Value]);
          _ ->             io:format(user, "Undefined parameter: ~p~n", [Param])
    end.

%% turn 'on' or 'off' dump_iq_on_error
%% Valid inputs are on | off
%% Error messages get logged to the ejabberd.log file only if the value is 'on'
dump_iq_on_error(Flag) ->
    ets:update_element(mod_reaper_config, dump_iq_on_error, {2, Flag}).

%%----------------------------------
%% gen_server implementation
%%----------------------------------

init([AmqpHost, User, Pass, ModreaperHost]) ->
    process_flag(trap_exit, true),
    ets:new(mod_reaper_config, [set, public, named_table]),
    ets:insert(mod_reaper_config, {msg_sink, off}),
    ets:insert(mod_reaper_config, {dump_iq_on_error, off}),
    {ok, IOD} = file:open("Messages", [append]),
    gen_server:cast(self(), init),
    {ok,
     #state{amqp_host = AmqpHost, user = User, pass = Pass,
	    mod_reaper_host = ModreaperHost, iod = IOD}}.

handle_call(_Msg, _From, State) -> {noreply, State}.

handle_cast(init, State) ->
    {MConnRef, Connection, MChanRef, Channel} = amqp_connect_loop(State),
    {noreply, State#state{mconnref= MConnRef, connection = Connection, mchanref= MChanRef, channel = Channel}};
handle_cast({publish_property_values,
	     PropertyValues = #property_values{type = Type},
	     Gateway},
	    State) ->
    Devices = [format_device(Device) || Device <- PropertyValues#property_values.devices],
    Payload = reaper_util:set_json({struct,
		[{machine_id, Gateway#gateway.machine_id},
		 {soil_id, Gateway#gateway.soil_id},
		 {utc_offset, Gateway#gateway.utc_offset},
		 {devices, Devices}]}),
    publish(Payload, State#state.channel,
	    property_values_exchange(Gateway#gateway.production_soil),
	    property_values_routing_key(Type, (Gateway#gateway.soil_id)),
	    amqp_properties(is_property_values_persistent(Type))),
    {noreply, State};
handle_cast({publish_to_reaper_manager, Message}, State) ->
    handle_publish_to_reaper_manager(Message, State#state.channel),
    {noreply, State};
handle_cast({publish_to_sod, Message}, State) ->
    handle_publish_to_sod(Message, State#state.channel),
    {noreply, State};
handle_cast({publish_online_gateways, Gateways}, State) ->
    Payload = reaper_util:set_json(Gateways),
    publish(Payload, State#state.channel,
	    ?reaper_ONLINE_EXCHANGE, <<"online_gateways">>,
	    amqp_properties(false)),
    {noreply, State};
handle_cast({async_write, Msg}, State) ->
     case ets:lookup(mod_reaper_config, msg_sink) of
            [{msg_sink, on}]-> %%spawn(fun() ->
				{{Y,M,D},{H,Min,Sec}} = calendar:local_time(),
                                file:write(State#state.iod,
				io_lib:format("~4.10.0B-~2.10.0B-~2.10.0B ~2.10.0B:~2.10.0B:~2.10.0B ~p~p~n",[Y, M, D, H, Min, Sec, ": message collected is -> ", lists:flatten(Msg)]));
          			%%end);
              _ -> ok
      end,
      {noreply, State};
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = _Tag, routing_key = _RoutingKey},
	    Msg = #amqp_msg{props = #'P_basic'{content_type = <<"application/json">>}}},
	    State = #state{}) ->
    write_async_message(Msg#amqp_msg.payload),
    try parse(Msg) of
      ValidMessage ->
	  mod_reaper:amqp_message(State#state.mod_reaper_host, ValidMessage)
    catch
      error:X ->
	  ?ERROR_MSG("Error parsing AMQP message.  Error was ~p, ~p. Payload was: ~p",
		     [X, erlang:get_stacktrace(), Msg#amqp_msg.payload])
    end,
    {noreply, State};
handle_info({'DOWN', MRef, process, _Pid, Reason}, State = #state{mconnref = MConnRef, mchanref = MChanRef}) ->
    ?ERROR_MSG("Got a 'DOWN' mesage with reason of ~p", [Reason]),
    case MRef of
        MConnRef -> ?ERROR_MSG("The connection closed due to reason ~p", [Reason]);
        MChanRef -> ?ERROR_MSG("The channel closed due to reason ~p", [Reason]);
        _        -> ?ERROR_MSG("Unknown Reference received, with reason ~p", [Reason])
    end,
    {noreply, State};

handle_info({'EXITE', Connection, Reason}, State) ->
    ?ERROR_MSG("Lost AMQP connection. ~p: ~p", [Connection, Reason]),
    %%catch close_channel(State#state.channel),
    %%catch close_connection(State#state.connection),
    {MConnRef, NewConnection, MChanRef, NewChannel} = amqp_connect_loop(State),
    {noreply, State#state{mconnref = MConnRef, connection = NewConnection, mchanref = MChanRef, channel = NewChannel}};
handle_info(Msg, State) ->
    ?ERROR_MSG("Received unhandled EXIT? message in reaper_amqp: ~p", [Msg]),
    {noreply, State}.

terminate(Reason, State) ->
    ?ERROR_MSG("Terminating reaper_amqp server for reason ~p", [Reason]),
    close_channel(State#state.channel),
    close_connection(State#state.connection),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.


parse(#amqp_msg{props = AmqpProps, payload = AmqpPayload}) ->
    {struct, Props} = reaper_util:get_json(AmqpPayload),
    MachineId = reaper_util:get_json_string(<<"machine_id">>, Props),
    Action = reaper_util:get_json_property(<<"action">>, Props),
    ?ERROR_MSG("Action is ~p for machineId of ~p ~n", [Action, MachineId]),
    case reaper_util:get_json_property(<<"action">>, Props) of
      <<"register">> ->
                SoilId = reaper_util:get_json_string(<<"soil_id">>, Props),
                ?ERROR_MSG("Action is ~p for machineId of ~p and soil_id of ~p ~n", [Action, MachineId, SoilId]),
	  #register_gateway{machine_id = MachineId,
			    password = reaper_util:get_json_string(<<"password">>, Props),
			    utc_offset = reaper_util:get_json_property(<<"utc_offset">>, Props),
			    soil_id = reaper_util:get_json_string(<<"soil_id">>, Props),
			    production_soil = reaper_util:get_json_property(<<"production_soil">>, Props)};
      <<"unregister">> ->
	  #unregister_gateway{machine_id = MachineId};
      <<"change_password">> ->
	  #change_gateway_password{machine_id = MachineId,
				   password = reaper_util:get_json_string(<<"password">>, Props)};
      <<"rename">> ->
	  #rename_gateway{machine_id = MachineId,
			  password = reaper_util:get_json_string(<<"password">>, Props),
			  old_machine_id = reaper_util:get_json_string(<<"old_machine_id">>, Props)};
      <<"historical_data_request">> ->
	  #historical_data_request{machine_id = MachineId,
				   protocol_version = reaper_util:get_json_property(<<"protocol_version">>, Props),
				   device_name = reaper_util:get_json_string(<<"device_name">>, Props),
				   start_time = reaper_util:get_json_string(<<"start_time">>, Props),
				   end_time = reaper_util:get_json_string(<<"end_time">>, Props),
				   property_names = reaper_util:get_json_property(<<"property_names">>, Props)};
      <<"log_request">> ->
	  #log_request{machine_id = MachineId,
		       protocol_version = reaper_util:get_json_property(<<"protocol_version">>, Props),
		       device_name = reaper_util:get_json_string(<<"device_name">>, Props),
		       start_time = reaper_util:get_json_string(<<"start_time">>, Props),
		       end_time = reaper_util:get_json_string(<<"end_time">>, Props),
		       property_names = reaper_util:get_json_property(<<"property_names">>, Props)};
      <<"live_data_request">> ->
                SoilId = reaper_util:get_json_string(<<"soil_id">>, Props),
                ?ERROR_MSG("Action is ~p for machineId of ~p and soil_id of ~p ~n", [Action, MachineId, SoilId]),
	  #live_data_request{machine_id = MachineId,
			     protocol_version = reaper_util:get_json_property(<<"protocol_version">>, Props),
			     device_name = reaper_util:get_json_string(<<"device_name">>, Props),
			     property_names = reaper_util:get_json_property(<<"property_names">>, Props),
			     host = reaper_util:get_json_string(<<"host">>, Props),
			     soil_id = reaper_util:get_json_string(<<"soil_id">>, Props)};
      <<"manage_packages">> ->
	  parse_manage_packages(Props, MachineId,
				reaper_util:get_json_string(<<"command">>, Props),
				reaper_util:get_json_string(<<"protocol_version">>, Props),
				AmqpProps#'P_basic'.correlation_id);
      <<"alerter_config">> ->
	  #alerter_config{machine_id = MachineId,
			  protocol_version = reaper_util:get_json_property(<<"protocol_version">>, Props),
			  config_xml = xml_stream:parse_element(reaper_util:get_json_property(<<"config_xml">>, Props))};
      <<"publish_online_gateways">> ->
	  publish_online_gateways; %% TODO: This is only used in testing.
  Other -> ?ERROR_MSG("Error parsing Message of (~p)~n",[Other]), {error, unknown_action}
    end.

parse_manage_packages(Props, MachineId, Command, ProtocolVersion, undefined) ->
    parse_manage_packages(Props, MachineId, Command, ProtocolVersion, <<>>);
parse_manage_packages(Props, MachineId, Command, ProtocolVersion, CorrelationId) ->
    case is_supported_command(Command, ProtocolVersion) of
      true ->
	  case {parse_packages(Command, Props), parse_config(Command, Props)} of
	    {{error, _Reason} = E, _} -> E;
	    {_, {error, _Reason} = E} -> E;
	    {Packages, Config} ->
		#manage_packages{machine_id = MachineId,
				 protocol_version = reaper_util:get_json_string(<<"protocol_version">>, Props),
				 command = Command,
				 command_id = CorrelationId,
				 packages = Packages, config_xml = Config}
	  end;
      false -> {error, unsupported_command}
    end.

is_supported_command(Command, Version) when is_binary(Version) ->
    case reaper_util:format_value_content(Version) of
	Bin when is_binary(Bin) -> false;
	Num -> is_supported_command(Command, Num)
    end;
is_supported_command(Command, Version) when is_integer(Version), Version >= 1 ->
    lists:member(Command,
	[<<"install">>,<<"remove">>,<<"upgrade">>,<<"reconfigure">>,<<"status">>]);
is_supported_command(_Command, _Version) -> false.

parse_packages(<<"install">>, Props) -> reaper_util:get_packages(Props, true);
parse_packages(<<"remove">>, Props) -> reaper_util:get_packages(Props, true);
parse_packages(<<"status">>, Props) -> reaper_util:get_packages(Props, false);
parse_packages(<<"upgrade">>, _Props) -> undefined;
parse_packages(<<"reconfigure">>, _Props) -> undefined.

parse_config(<<"install">>, Props) -> reaper_util:get_config(Props, false);
parse_config(<<"remove">>, Props) -> reaper_util:get_config(Props, false);
parse_config(<<"status">>, _Props) -> undefined;
parse_config(<<"upgrade">>, Props) -> reaper_util:get_config(Props, false);
parse_config(<<"reconfigure">>, Props) -> reaper_util:get_config(Props, true).

handle_publish_to_reaper_manager(#status_basic{machine_id = MachineId,
					      status = Status},
				Channel) ->
    Payload = reaper_util:set_json({struct,
		[{action, <<"status_basic">>},
		 {machine_id, MachineId},
		 {status, Status}]}),
    publish(Payload, Channel, ?reaper_MANAGER_QUEUE, amqp_properties(true));
handle_publish_to_reaper_manager(#status_advanced{} = Status,
				Channel) ->
    User = Status#status_advanced.machine_id,
    [{_, Server}] = ets:lookup(host_table, host), %% P1: dirty code
    Ip = case ejabberd_sm:get_user_ip(User, Server, <<"roots">>) of
	    {{I1, I2, I3, I4}, _} -> [I1, I2, I3, I4];
	    _ -> [0, 0, 0, 0]
	 end,
    Packages = [format_package(P) || P <- Status#status_advanced.packages],
    Payload = reaper_util:set_json({struct,
		[{action, <<"status_advanced">>},
		 {machine_id, Status#status_advanced.machine_id},
		 {status, Status#status_advanced.status},
		 {protocol_version, Status#status_advanced.protocol_version},
		 {config_timestamp, Status#status_advanced.config_timestamp},
		 {last_connected_ip, Ip},
		 {packages, Packages}]}),
    publish(Payload, Channel, ?reaper_MANAGER_QUEUE, amqp_properties(true));
handle_publish_to_reaper_manager(#manage_packages_response{machine_id = MachineId,
							  command = Command,
							  progress = Progress,
							  success = Success,
							  command_id = Id},
				Channel) ->
    Status = case Success of
	       undefined -> {progress, Progress};
	       _ -> {success, Success}
	     end,
    Payload = reaper_util:set_json({struct,
		[{action, <<"manage_packages_response">>},
		 {machine_id, MachineId},
		 {command, Command},
		 Status]}),
    ?INFO_MSG("FAR::manage_packages_response ~p: ~n", [Payload]),
    publish(Payload, Channel, ?reaper_MANAGER_QUEUE, amqp_properties(true, Id));
handle_publish_to_reaper_manager(#crash_report{machine_id = MachineId,
					      content = Payload},
				Channel) ->
    Props = #'P_basic'{content_type = ?XML_CONTENT_TYPE,
		       delivery_mode = delivery_mode(true),
		       headers = [{<<"reaper_machine_id">>, longstr, MachineId}]},
    publish(Payload, Channel, ?reaper_GATEWAY_CRASH_EXCHANGE, MachineId, Props);
% FAR: alarm result
handle_publish_to_reaper_manager(#alarm_result{machine_id = MachineId,
					      content = Payload},
				Channel) ->
    Props = #'P_basic'{content_type = ?XML_CONTENT_TYPE,
		       delivery_mode = delivery_mode(true),
		       headers =
			   [{<<"reaper_machine_id">>, longstr, MachineId}]},
    ?INFO_MSG("FAR::Props ~p: ~n", [Payload]),
    publish(Payload, Channel, ?reaper_DATA_ALARM_EXCHANGE, MachineId, Props).

handle_publish_to_sod(#alerter_config_response{content = Payload},
		      Channel) ->
    ?INFO_MSG("RAW::ACR ~p", [Payload]),
    publish(Payload, Channel, ?reaper_ALERT_CONFIRMATION_QUEUE, amqp_properties(true)).

format_package(#package{name = Name, version = null,
			installed = Installed}) ->
    {struct, [{name, Name}, {installed, Installed}]};
format_package(#package{name = Name, version = Version,
			installed = Installed}) ->
    {struct,
     [{name, Name}, {version, Version},
      {installed, Installed}]}.

format_device({device, Name, Props}) ->
    {struct,
     [{name, Name},
      {properties, [serialize_property(Prop) || Prop <- Props]}]}.

serialize_property({property, Name, Values}) ->
    {struct, [{name, Name}, {values, Values}]}.

property_values_exchange(true) -> ?reaper_DATA_EXCHANGE;
property_values_exchange(false) -> ?reaper_STAGING_DATA_EXCHANGE.

property_values_routing_key(report, GatewayName) ->
    get_routing_key(GatewayName, <<"reaper.data.report">>);
property_values_routing_key(amr, GatewayName) ->
    get_routing_key(GatewayName, <<"reaper.data.amr">>);
property_values_routing_key(historical, GatewayName) ->
    get_routing_key(GatewayName, <<"reaper.data.historical">>);
property_values_routing_key(live, GatewayName) ->
    get_routing_key(GatewayName, <<"reaper.data.live">>);
property_values_routing_key(internal, GatewayName) ->
    get_routing_key(GatewayName, <<"reaper.data.internal">>);
property_values_routing_key(_, _) ->
    <<"reaper.data.unknown">>.

get_routing_key(GatewayName, Prefix) ->
    FirstChar = str:substr(GatewayName, 1, 1),
    Postfix = get_routing_key_post_fix(FirstChar),
    <<Prefix/binary, Postfix/binary>>.

get_routing_key_post_fix(FirstChar) ->
    case re:run(FirstChar, <<"[A-Za-z]">>) of
      nomatch -> <<>>;
      {match, _} -> <<".", (str:to_lower(FirstChar))/binary>>
    end.

is_property_values_persistent(live) -> false;
is_property_values_persistent(_) -> true.

publish(Payload, Channel, Exchange, RoutingKey, Properties) ->
    Publish = #'basic.publish'{exchange = Exchange,
			       routing_key = RoutingKey},
    amqp_channel:cast(Channel, Publish,
		      #amqp_msg{payload = Payload, props = Properties}).

publish(Payload, Channel, Queue, Properties) ->
    Publish = #'basic.publish'{exchange = <<>>,
			       routing_key = Queue},
    amqp_channel:cast(Channel, Publish,
		      #amqp_msg{payload = Payload, props = Properties}).

delivery_mode(true) -> 2;
delivery_mode(false) -> 1.

amqp_properties(Persistent) ->
    #'P_basic'{content_type = ?JSON_CONTENT_TYPE,
	       delivery_mode = delivery_mode(Persistent)}.

amqp_properties(Persistent, CorrelationId) ->
    Props = amqp_properties(Persistent),
    Props#'P_basic'{correlation_id = CorrelationId}.

amqp_declare_exchange(Channel, Name, Type) ->
    amqp_declare_exchange(Channel, Name, Type, true).

amqp_declare_exchange(Channel, Name, Type, Durable) ->
    #'exchange.declare_ok'{} = amqp_channel:call(Channel,
						 #'exchange.declare'{exchange = Name,
								     type = Type,
								     durable = Durable}).

amqp_connect_loop(State) ->
    case amqp_connect(State) of
      {error, _Error} -> timer:sleep(?RECONNECT_TIME), amqp_connect_loop(State);
      {ok, MConnRef, Connection, MChanRef, Channel} -> {MConnRef, Connection, MChanRef, Channel}
    end.

amqp_connect(State) ->
    ?INFO_MSG("reaper_amqp: Connecting to AMQP server", []),
    AmqpParams = #amqp_params_network{username = State#state.user,
			              password = State#state.pass,
			              host = State#state.amqp_host},
    case amqp_connection:start(AmqpParams) of
      {ok, Connection} ->
	  ?INFO_MSG("reaper_amqp: Successfully created AMQP connection", []),
	  MConnRef = erlang:monitor(process, Connection),
	  %%link(Connection),
	  {ok, Channel} = amqp_connection:open_channel(Connection),
	  %%amqp_selective_consumer:register_default_consumer(Channel, self()),
      MChanRef = erlang:monitor(process, Channel),
	  amqp_declare_exchange(Channel, ?reaper_DATA_EXCHANGE, <<"topic">>),
	  amqp_declare_exchange(Channel, ?reaper_STAGING_DATA_EXCHANGE, <<"topic">>),
	  amqp_declare_exchange(Channel, ?reaper_GATEWAY_CRASH_EXCHANGE, <<"fanout">>),
	  amqp_declare_exchange(Channel, ?reaper_ONLINE_EXCHANGE, <<"fanout">>),
	  amqp_declare_exchange(Channel, ?reaper_DATA_ALARM_EXCHANGE, <<"fanout">>),
	  #'queue.declare_ok'{} = amqp_channel:call(Channel,
						    #'queue.declare'{queue = ?reaper_MESSAGING_QUEUE,
								     durable = true}),
	  #'basic.consume_ok'{} = amqp_channel:subscribe(Channel,
							 #'basic.consume'{queue = ?reaper_MESSAGING_QUEUE,
									  no_ack = true},
							 self()),
	  {ok, MConnRef, Connection, MChanRef, Channel};
      {error, Error} ->
	  ?ERROR_MSG("Error connecting to AMQP. Error was ~p.", [Error]),
	  {error, Error}
    end.

close_channel(undefined) -> ok;
close_channel(Pid) -> amqp_channel:close(Pid).

close_connection(undefined) -> ok;
close_connection(Pid) -> amqp_connection:close(Pid).
