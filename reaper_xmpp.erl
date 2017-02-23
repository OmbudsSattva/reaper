%% @author Shailen Karur
%% @copyright 2014 Shailen Karur
%% @doc Translates XMPP stanzas into more concise Erlang terms and vice versa.

%% @author Shailen Karur <ombuds.sattva@gmail.com>
%% @doc This is a rewrite by Shailen Karur of the original module from ProcessOne to work on eBE

-module(reaper_xmpp).

-export([parse/2, retrieve_data/4, manage_packages/3,
	 machine_id/1, alerter_config/3]).

-include("jlib.hrl").
-include("logger.hrl").
-include("reaper.hrl").

-define(NS_ROOTS_V0, <<"http://sunedison.com/reaper/property_values">>).
-define(NS_ROOTS_V1, <<"http://sunedison.com/reaper/roots">>).
-define(NS_ROOTS_V5, <<"http://sunedison.com/reaper/roots-4.5">>).
-define(NS_ROOTS_V6, <<"http://sunedison.com/reaper/roots-4.6">>).
-define(NS_ROOTS_V7, <<"http://sunedison.com/reaper/roots-4.7">>).
-define(NS_ROOTS_V8, <<"http://sunedison.com/reaper/roots-5.1">>).
-define(NS_ROOTS_V9, <<"http://sunedison.com/reaper/roots-5.2">>).

parse(#xmlel{name = <<"message">>} = Message, From) ->
    parse_message(Message, From);
parse(#xmlel{name = <<"iq">>} = Iq, From) ->
    case xml:get_tag_attr_s(<<"type">>, Iq) of
      <<"result">> ->
	  case xml:get_subtag(Iq, <<"query">>) of
	    false -> {error, no_query};
	    #xmlel{name = <<"query">>} = Query ->
		Namespace = get_namespace(Query),
		case is_valid_namespace(Namespace) of
		  true ->
		      ?INFO_MSG("RAW::ACR ~p", [Query]),
		      case get_first_child(Query) of
			false -> {error, unparseable};
			#xmlel{name = <<"property_values">>} = PropertyValues ->
			    #property_values{machine_id = machine_id(From),
					     type = type_for_iq(Iq),
					     devices = parse_property_values(PropertyValues, Namespace)};
			#xmlel{name = <<"logged_property_values">>} = LoggedPropertyValues ->
			    #property_values{machine_id = machine_id(From),
					     type = type_for_iq(Iq),
					     devices = parse_logged_property_values(LoggedPropertyValues)};
			#xmlel{name = <<"manage_packages_response">>} = ManagePackages ->
			    parse_manage_packages_response(ManagePackages, From, xml:get_tag_attr_s(<<"id">>, Iq));
			#xmlel{name = <<"alerter_config_response">>} = AlerterConfig ->
			    parse_alerter_config_response(AlerterConfig, From);
			_ -> {error, unparseable}
		      end;
		  false -> {error, bad_namespace}
		end
	  end;
      <<"error">> -> parse_iq_error(Iq, From);
      _Other -> {error, iq_type}
    end;
parse(#xmlel{name = <<"presence">>} = Presence, From) ->
    case get_first_child(Presence) of
      #xmlel{name = <<"roots">>,
	     attrs = [{<<"xmlns">>, NS}]} = Roots
	  when NS == (?NS_ROOTS_V1);
	       NS == (?NS_ROOTS_V5);
	       NS == (?NS_ROOTS_V6);
	       NS == (?NS_ROOTS_V7);
	       NS == (?NS_ROOTS_V8);
	       NS == (?NS_ROOTS_V9) ->
	  parse_advanced_presence(Presence, From, Roots);
      _ -> parse_basic_presence(Presence, From)
    end;
parse(_Other, _From) -> {error, unparseable}.

retrieve_data({Type, MachineId, Protocol, Device,
	       Properties, Start, End},
	      From, Host, Counter)
    when Type == historical_data_request;
	 Type == log_request ->
    Source = case Type of
	       historical_data_request -> <<"db">>;
	       log_request -> <<"hardware">>
	     end,
    get_property_values(Source, MachineId, Protocol, Device,
			Properties, {times, Start, End}, From, Host, Counter);
retrieve_data(#live_data_request{machine_id = MachineId,
				 protocol_version = Protocol,
				 device_name = Device,
				 property_names = Properties},
	      From, Host, Counter) ->
    get_property_values(<<"live">>, MachineId, Protocol,
			Device, Properties, null, From, Host, Counter).

to(User, Host) ->
    jlib:make_jid(User, Host, <<"roots">>).

parse_message(#xmlel{name = <<"message">>,
		     attrs = Attrs, children = Els},
	      From) ->
    ?INFO_MSG("RAW::ACR Els ~p", [Els]),
    Id = xml:get_attr_s(<<"id">>, Attrs),
    parse_message_child(first_valid_message_child(Els), From, Id).

first_valid_message_child([#xmlel{name = Name} = N | _T])
    when Name =:= <<"manage_packages_response">>;
	 Name =:= <<"property_values">>;
	 Name =:= <<"crash_report">>;
	 Name =:= <<"alarm_result">> ->
    N;
first_valid_message_child([_H | T]) ->
    first_valid_message_child(T);
first_valid_message_child([]) -> null.

parse_message_child(#xmlel{name = <<"manage_packages_response">>} = MPR,
		    From, Id) ->
    parse_manage_packages_response(MPR, From, Id);
parse_message_child(#xmlel{name = <<"property_values">>,
			   attrs = Attrs} = PV,
		    From, _Id) ->
    Type = case xml:get_attr_s(<<"internal">>, Attrs) of
	     <<"true">> -> internal;
	     _ -> report
	   end,
    Namespace = xml:get_attr_s(<<"xmlns">>, Attrs),
    #property_values{machine_id = machine_id(From),
		     type = Type,
		     devices = parse_property_values(PV, Namespace)};
parse_message_child(#xmlel{name = <<"crash_report">>} = CR,
		    From, _Id) ->
    parse_crash_report(CR, From);
parse_message_child(#xmlel{name = <<"alarm_result">>} = AR,
		    From, _Id) ->
    parse_alarm_result(AR, From);
parse_message_child(null, _From, _Id) ->
    {error, unparseable}.

get_property_values(Source, User, ProtocolVersion,
		    Device, Properties, Times, From, Host, Counter) ->
    {RequestNode, RequestAttrs} = case Source of
				    <<"live">> ->
					{<<"get_properties">>,
					 [{<<"internal">>, <<"false">>}]};
				    _ ->
					{times, Start, End} = Times,
					{<<"get_logged_properties">>,
					 [{<<"source">>, Source},
					  {<<"start_time">>, Start},
					  {<<"end_time">>, End},
					  {<<"id">>, Device}]}
				  end,
    El = query_node(ProtocolVersion,
	    [#xmlel{name = RequestNode,
		    attrs = RequestAttrs,
		    children = [
			#xmlel{name = <<"device">>,
			       attrs = [{<<"id">>, Device}],
			       children = [
				#xmlel{name = <<"property">>,
				       attrs = [{<<"name">>, N}],
				       children = []}
				|| N <- Properties]}
			]}]),
    ejabberd_router:route(From, to(User, Host),
			  jlib:iq_to_xml(#iq{type = get,
					     id = iq_id(Source, Counter),
					     sub_el = [El]})),
    Counter + 1.

manage_packages(#manage_packages{command = Command} = MP,
		From, Host) ->
    El = query_node(MP#manage_packages.protocol_version,
	    [#xmlel{name = <<"manage_packages">>,
		    attrs = [],
		    children = [command_node(MP)]}]),
    ejabberd_router:route(From, to(MP#manage_packages.machine_id, Host),
			  jlib:iq_to_xml(#iq{type = case Command of
						       <<"status">> -> get;
						       _other -> set
						     end,
					     id = MP#manage_packages.command_id,
					     sub_el = [El]})).

alerter_config(#alerter_config{} = AC, From, Host) ->
    ?INFO_MSG("RAW::AlerterConfig ~p", [AC]),
    El = query_node(AC#alerter_config.protocol_version,
	    [AC#alerter_config.config_xml]),
    ejabberd_router:route(From, to(AC#alerter_config.machine_id, Host),
			  jlib:iq_to_xml(#iq{type = set,
					     id = <<"alerter_config_100">>,
					     sub_el = [El]})).

command_node(#manage_packages{command = <<"reconfigure">>,
			      config_xml = Config}) ->
    #xmlel{name = <<"reconfigure">>, attrs = [],
	   children = [Config]};
command_node(#manage_packages{command = Command,
			      config_xml = Config, packages = Packages}) ->
    #xmlel{name = Command, attrs = [],
	   children = config_nodes(Config) ++ package_nodes(Packages)}.

config_nodes(undefined) -> [];
config_nodes(Config) ->
    [#xmlel{name = <<"replace_config">>, attrs = [], children = [Config]}].

package_nodes(undefined) -> [];
package_nodes(Packages) ->
    [#xmlel{name = <<"packages">>, attrs = [],
	    children =
		[#xmlel{name = <<"package">>, attrs = [],
			children =
			    [#xmlel{name = <<"name">>, attrs = [],
				    children = [{xmlcdata, P}]}]}
		 || P <- Packages]}].

query_node(ProtocolVersion, Children) ->
    #xmlel{name = <<"query">>,
	   attrs = [{<<"xmlns">>, reaper_namespace(reaper_util:format_value_content(ProtocolVersion))}],
	   children = Children}.

parse_basic_presence(Presence, From) ->
    #status_basic{machine_id = machine_id(From),
		  status = get_status(Presence)}.

parse_advanced_presence(Presence, From, Roots) ->
    #status_advanced{machine_id = machine_id(From),
		     status = get_status(Presence),
		     config_timestamp = xml:get_subtag_cdata(Roots, <<"config_timestamp">>),
		     protocol_version = protocol_version(xml:get_tag_attr_s(<<"xmlns">>, Presence)),
		     packages = get_packages(Roots)}.

get_status(Presence) ->
    case xml:get_tag_attr_s(<<"type">>, Presence) of
      <<>> -> <<"available">>;
      Type -> Type
    end.

get_packages(Roots) ->
    case xml:get_subtag(Roots, <<"package_status">>) of
      false ->
	    [];
      #xmlel{name = <<"package_status">>, children = Els} ->
	    [parse_package(P) || P <- Els, is_element_named(P, <<"package">>)]
    end.

parse_package(P) ->
    #package{name = xml:get_subtag_cdata(P, <<"name">>),
	     installed = case xml:get_subtag_cdata(P, <<"installed">>) of
		           <<"true">> -> true;
		           _ -> false
		         end,
	     version = case xml:get_subtag_cdata(P, <<"version">>) of
		         <<>> -> null;
		         V -> V
		       end}.

parse_property_values(Values, Namespace) ->
    case protocol_version(Namespace) of
      N when N < 5 -> parse_property_values(Values);
      _ -> parse_property_values_v5(Values)
    end.

parse_property_values(#xmlel{name = <<"property_values">>, children = Els}) ->
    [parse_device(Dev) || Dev <- Els, is_element_named(Dev, <<"device">>)].

parse_device(#xmlel{name = <<"device">>, attrs = Attrs, children = Els}) ->
    {device, xml:get_attr_s(<<"id">>, Attrs),
     [parse_property(Prop) || Prop <- Els, is_element_named(Prop, <<"property">>)]}.

parse_property(#xmlel{name = <<"property">>, attrs = Attrs, children = Els}) ->
    {property, xml:get_attr_s(<<"name">>, Attrs),
     [parse_value(Val) || Val <- Els, is_element_named(Val, <<"value">>)]}.

parse_value(#xmlel{name = <<"value">>, attrs = Attrs, children = Els}) ->
    [xml:get_attr_s(<<"timestamp">>, Attrs),
     reaper_util:format_value_content(xml:get_cdata(Els))].

parse_property_values_v5(#xmlel{name = <<"property_values">>, children = Els}) ->
    [parse_device_v5(Dev) || Dev <- Els, is_element_named(Dev, <<"device">>)].

parse_device_v5(#xmlel{name = <<"device">>, attrs = Attrs, children = Els}) ->
    Timestamp = xml:get_attr_s(<<"timestamp">>, Attrs),
    {device, xml:get_attr_s(<<"id">>, Attrs),
     [parse_property_v5(Prop, Timestamp)
      || Prop <- Els, is_element_named(Prop, <<"property">>),
	 xml:get_subtag(Prop, <<"value">>) /= false]}.

parse_property_v5(Prop, Timestamp) ->
    {property, xml:get_tag_attr_s(<<"name">>, Prop),
     [[Timestamp, reaper_util:format_value_content(xml:get_subtag_cdata(Prop, <<"value">>))]]}.

parse_logged_property_values(#xmlel{name = <<"logged_property_values">>, children = Els}) ->
    [parse_logged_device(Dev) || Dev <- Els, is_element_named(Dev, <<"device">>)].

parse_logged_device(#xmlel{name = <<"device">>, attrs = Attrs} = D) ->
    {device, xml:get_attr_s(<<"id">>, Attrs),
     parse_logged_values(get_named_children(D, <<"values">>))}.

parse_logged_values(Vals) ->
    PropertyDict = lists:foldl(fun (V, Acc) ->
				       parse_logged_properties(get_named_children(V, <<"property">>),
							       xml:get_tag_attr_s(<<"timestamp">>, V),
							       Acc)
			       end,
			       dict:new(), Vals),
    [{property, PropName, Values}
     || {PropName, Values} <- lists:keysort(1, dict:to_list(PropertyDict))].

parse_logged_properties(Props, Timestamp, Dict) ->
    lists:foldl(fun (P, Acc) ->
			dict:append(xml:get_tag_attr_s(<<"name">>, P),
				    [Timestamp, reaper_util:format_value_content(xml:get_tag_cdata(P))],
				    Acc)
		end,
		Dict, Props).

parse_alerter_config_response(#xmlel{name = <<"alerter_config_response">>} = ACR,
			      From) ->
    ?INFO_MSG("RAW::ACR ~p", [ACR]),
    #alerter_config_response{machine_id = machine_id(From),
			     content = xml:element_to_binary(ACR)}.

parse_manage_packages_response(#xmlel{name = <<"manage_packages_response">>} = MP,
			       From, Id) ->
    case get_first_child(MP) of
      #xmlel{name = Command} = CommandNode ->
	  case get_first_child(CommandNode) of
	    #xmlel{name = <<"success">>} ->
		manage_packages_success(Command, true, From, Id);
	    #xmlel{name = <<"progress">>} = Progress ->
		manage_packages_progress(Command, xml:get_tag_cdata(Progress), From, Id);
	    _ ->
		manage_packages_success(Command, false, From, Id)
	  end;
      _Else -> {error, invalid_manage_packages_response}
    end.

manage_packages_success(Command, Success, From, Id) ->
    Base = manage_packages_base(Command, From, Id),
    Base#manage_packages_response{success = Success}.

manage_packages_progress(Command, Progress, From, Id) ->
    Base = manage_packages_base(Command, From, Id),
    Base#manage_packages_response{progress = Progress}.

manage_packages_base(Command, From, Id) ->
    #manage_packages_response{machine_id = machine_id(From),
			      command = Command,
			      command_id = Id}.

is_element_named(#xmlel{name = Name}, Name) -> true;
is_element_named(_, _) -> false.

get_named_children(#xmlel{children = Els}, Name) ->
    [E || E <- Els, is_element_named(E, Name)].

type_for_iq(Iq) ->
    case xml:get_tag_attr_s(<<"id">>, Iq) of
      <<"db_", _Id/binary>> -> historical;
      <<"hardware_", _Id/binary>> -> amr;
      <<"live_", _Id/binary>> -> live;
      _ -> unknown
    end.

reaper_namespace(0) -> ?NS_ROOTS_V0;
reaper_namespace(N) when N >= 1, N =< 4 -> ?NS_ROOTS_V1;
reaper_namespace(5) -> ?NS_ROOTS_V5;
reaper_namespace(6) -> ?NS_ROOTS_V6;
reaper_namespace(7) -> ?NS_ROOTS_V7;
reaper_namespace(8) -> ?NS_ROOTS_V8;
reaper_namespace(9) -> ?NS_ROOTS_V9;
reaper_namespace(_) -> ?NS_ROOTS_V5.

protocol_version(?NS_ROOTS_V0) -> 0;
protocol_version(?NS_ROOTS_V1) -> 1;
protocol_version(?NS_ROOTS_V5) -> 5;
protocol_version(?NS_ROOTS_V6) -> 6;
protocol_version(?NS_ROOTS_V7) -> 7;
protocol_version(?NS_ROOTS_V8) -> 8;
protocol_version(?NS_ROOTS_V9) -> 9;
protocol_version(_) -> 1.

is_valid_namespace(N) ->
    lists:member(N,
		 [?NS_ROOTS_V0, ?NS_ROOTS_V1, ?NS_ROOTS_V5, ?NS_ROOTS_V6,
		  ?NS_ROOTS_V7, ?NS_ROOTS_V8, ?NS_ROOTS_V9]).

get_namespace(#xmlel{attrs = Attrs}) ->
    xml:get_attr_s(<<"xmlns">>, Attrs).

iq_id(Prefix, Counter) ->
    <<Prefix/binary, "_", (integer_to_binary(Counter))/binary>>.

get_first_child(#xmlel{children = Els}) ->
    case lists:keysearch(xmlel, 1, Els) of
      false -> false;
      {value, Val} -> Val
    end.

machine_id(From) -> From#jid.user.

parse_crash_report(#xmlel{name = <<"crash_report">>} = CR,
		   From) ->
    #crash_report{machine_id = machine_id(From),
		  content = xml:element_to_binary(CR)}.

parse_alarm_result(#xmlel{name = <<"alarm_result">>} = AR,
		   From) ->
    #alarm_result{machine_id = machine_id(From),
		  content = xml:element_to_binary(AR)}.

parse_iq_error(#xmlel{name = <<"iq">>} = Iq, _From) ->
    case get_error_type(Iq) of
      <<"recipient-unavailable">> ->
	    {recipient_unavailable, type_for_iq(Iq)};
      Other ->
            case ets:lookup(mod_reaper_config, dump_iq_on_error) of
               [{dump_iq_on_error, on}] -> ?ERROR_MSG("IQ unparseable: ~n[~p]~n due to error ~p~n",[Iq, Other]);
                   _ ->                    ok
            end,
	    {error, unparseable_iq_error}
    end.

get_error_type(#xmlel{name = <<"iq">>} = Iq) ->
    get_error_type(xml:get_subtag(Iq, <<"error">>));
get_error_type(#xmlel{name = <<"error">>, children = Els}) ->
    get_error_type(Els);
get_error_type([#xmlel{name = Name} = Elem | T]) ->
    case get_namespace(Elem) of
      <<"urn:ietf:params:xml:ns:xmpp-stanzas">> -> Name;
      _Other -> get_error_type(T)
    end;
get_error_type([_Node | T]) -> get_error_type(T);
get_error_type([]) -> unknown;
get_error_type(false) -> unknown.
