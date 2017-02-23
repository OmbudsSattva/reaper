%% @author Shailen Karur
%% @ https://github.com/OmbudsSattva/reaper.git
%% @copyright 2014-2015 Shailen Karur
%% @doc This module implements the reaper&trade; protocol, versions 4.0 through 4.5.  It interfaces with the enterprise AMQP message bus.

%% @author Shailen Karur
%% @doc Rewritten/refactored over Dec 14 and Jan 15

-module(reaper).

-behavior(gen_server).

-behavior(gen_mod).

%%% API: gen_mod
-export([start_link/2, start/2, stop/1]).

%%% API: gen_server
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2, code_change/3]).

%% Module API
-export([amqp_message/2]).

%% exported for purposes of spawn() or timer:apply_interval() using module/function/args form only
-export([publish_online_gateways/0]).

-include("logger.hrl").
-include("reaper.hrl").

-include("amqp_client.hrl").

-define(PROCNAME, ejabberd_reaper).

-define(BOTNAME, reaper).

-define(GATEWAY_LIST_REFRESH, 60000).

%%% Records

-record(state,
	{component_host, server_host, from_jid, iq_counter,
	 gateway_procs}).

amqp_message(Host, Msg) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:cast(Proc, {amqp_message, Msg}).

start_link(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:start_link({local, Proc}, ?MODULE, [Host, Opts], []).

start(Host, Opts) ->
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    ChildSpec = {Proc, {?MODULE, start_link, [Host, Opts]},
		 transient, 10000, worker, [?MODULE]},
    supervisor:start_child(ejabberd_sup, ChildSpec).

stop(Host) ->
    ?INFO_MSG("reaper: Stopping host", []),
    Proc = gen_mod:get_module_proc(Host, ?PROCNAME),
    gen_server:call(Proc, stop),
    supervisor:terminate_child(ejabberd_sup, Proc),
    ?INFO_MSG("reaper: Deleting child from supervisor", []),
    supervisor:delete_child(ejabberd_sup, Proc).

init([Host, Opts]) ->
    ?INFO_MSG("reaper: Starting reaper module", []),
    MyHost = gen_mod:get_opt_host(Host, Opts, <<"data.@HOST@">>),
    ?INFO_MSG("reaper: using host ~s", [MyHost]),
    CacheHost = gen_mod:get_opt_host(Host, Opts, <<"@HOST@">>),
    ets:new(host_table, [named_table, protected, set, {keypos, 1}]),
    ets:insert(host_table, {host, CacheHost}),
    B2B = fun(V) when is_binary(V) -> V end,
    B2L = fun(V) when is_binary(V) -> binary_to_list(V) end,
    reaper_amqp:start_link(gen_mod:get_opt(amqp_host, Opts, B2L, "localhost"),
			  gen_mod:get_opt(amqp_user, Opts, B2B, <<"guest">>),
			  gen_mod:get_opt(amqp_pass, Opts, B2B, <<"guest">>), Host),
    timer:apply_interval(?GATEWAY_LIST_REFRESH, ?MODULE,
			 publish_online_gateways, []),
    ejabberd_router:register_route(MyHost),
    reaper_gateway_info:start_link(gen_mod:get_opt(reaper_manager_uri, Opts, B2L, "http://localhost:4000")),
    ?INFO_MSG("reaper: AMQP started, XMPP route registered", []),
    {ok,
     #state{server_host = Host, component_host = MyHost,
	    from_jid = jlib:make_jid(<<"agent">>, MyHost, <<"reaper">>),
	    iq_counter = 0, gateway_procs = dict:new()}}.

handle_call(stop, _From, State) ->
    ?INFO_MSG("reaper: handle_call stop", []),
    {stop, normal, ok, State}.

handle_cast({amqp_message, Msg}, State) ->
    ?DEBUG("reaper: handling an AMQP message ~p", [Msg]),
    Counter = case handle_amqp_message(Msg, State) of
		ok -> State#state.iq_counter;
		increment -> State#state.iq_counter + 1;
		{error, Reason} ->
		    ?ERROR_MSG("Unsupported AMQP message (~p):~n ~p", [Reason, Msg]),
		    State#state.iq_counter
	      end,
    {noreply, State#state{iq_counter = Counter}};
handle_cast(Msg, State) ->
    ?INFO_MSG("got a fallback cast: ~p", [Msg]),
    {noreply, State}.

handle_info({route, From, _To, Stanza}, State) ->
    {Gateway, GatewayProcs} = proc_for_gateway(From,
					       State#state.gateway_procs),
    reaper_gateway:handle_stanza(Gateway, From, Stanza),
    {noreply, State#state{gateway_procs = GatewayProcs}}.

terminate(_Reason, State) ->
    ?INFO_MSG("reaper: Terminating", []),
    ejabberd_router:unregister_route(State#state.component_host),
    ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

% XMPP Routing
%% Handles JSON messages coming in from AMQP.
handle_amqp_message(#register_gateway{machine_id = User} = Register,
		    State) ->
    ?INFO_MSG("reaper: Registering user ~p", [User]),
    reaper_gateway_info:store_gateway(#gateway{machine_id = User,
					      utc_offset = Register#register_gateway.utc_offset,
					      production_soil = Register#register_gateway.production_soil,
					      soil_id = iolist_to_binary(Register#register_gateway.soil_id)}),
    ejabberd_auth:try_register(User,
			       State#state.server_host,
			       Register#register_gateway.password),
    ok;
handle_amqp_message(#unregister_gateway{machine_id = User},
		    State) ->
    ?INFO_MSG("reaper: Removing user ~p", [User]),
    ejabberd_auth:remove_user(User, State#state.server_host),
    ok;
handle_amqp_message(#change_gateway_password{machine_id = User, password = Password},
		    State) ->
    ?INFO_MSG("reaper: Changing user password ~p", [User]),
    ejabberd_auth:set_password(User, State#state.server_host, Password),
    ok;
handle_amqp_message(#rename_gateway{machine_id = NewUser,
				    password = Password,
				    old_machine_id = OldUser},
		    State) ->
    ?INFO_MSG("reaper: Renaming user ~p to ~p",
	      [OldUser, NewUser]),
    reaper_gateway_info:rename_gateway(OldUser, NewUser),
    ejabberd_auth:remove_user(OldUser, State#state.server_host),
    ejabberd_auth:try_register(NewUser, State#state.server_host, Password),
    ok;
handle_amqp_message(Req, State)
    when is_record(Req, historical_data_request);
	 is_record(Req, log_request) ->
    reaper_xmpp:retrieve_data(Req, State#state.from_jid,
			     State#state.server_host, State#state.iq_counter),
    increment;
handle_amqp_message(#live_data_request{protocol_version = ProtocolVersion} = Req,
		    State) ->
    case reaper_util:format_value_content(ProtocolVersion) of
      PV when is_integer(PV), PV >= 0 ->
	  reaper_xmpp:retrieve_data(Req, State#state.from_jid,
				   State#state.server_host,
				   State#state.iq_counter),
	  increment;
      Other ->
	  ?ERROR_MSG("Unknown protocol version ~p", [Other])
    end,
    ok;
handle_amqp_message(#manage_packages{} = ManagePackages,
		    State) ->
    ?INFO_MSG("FAR::Manage Packages ~p", [ManagePackages]),
    reaper_xmpp:manage_packages(ManagePackages, State#state.from_jid, State#state.server_host),
    ok;
handle_amqp_message(#alerter_config{} = AlerterConfig,
		    State) ->
    ?INFO_MSG("reaper: Changing alert configuration to ~p", [AlerterConfig]),
    reaper_xmpp:alerter_config(AlerterConfig, State#state.from_jid, State#state.server_host),
    ok;
handle_amqp_message(publish_online_gateways, _State) ->
    publish_online_gateways(), ok;
handle_amqp_message(_Action, _State) ->
    {error, unsupported}.

proc_for_gateway(From, GatewayProcs) ->
    MachineId = reaper_xmpp:machine_id(From),
    case dict:find(MachineId, GatewayProcs) of
        {ok, Pid} ->
            case is_process_alive(Pid) of
                true ->
                    ?DEBUG("FAR::Gateway ~p in ~p", [Pid, dict:to_list(GatewayProcs)]),
                    {Pid, GatewayProcs};
                false ->
                    {ok, Gateway} = spawn_gateway(MachineId),
                    {Gateway, dict:store(MachineId, Gateway, GatewayProcs)}
            end;
        error ->
            {ok, Gateway} = spawn_gateway(MachineId),
            {Gateway, dict:store(MachineId, Gateway, GatewayProcs)}
    end.

spawn_gateway(MachineId) ->
    ?INFO_MSG("reaper: Spawning process for gateway ~p", [MachineId]),
    reaper_gateway:start_link(MachineId).

publish_online_gateways() ->
    Gateways = [S || {S, _, _} <- mnesia:dirty_all_keys(session)],
    reaper_amqp:publish_online_gateways(Gateways).
