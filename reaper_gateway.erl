%%%-------------------------------------------------------------------
%%% File          : reaper_gateway.erl
%%% Author        :
%%% Description   :
%%%
%%% Created       : 2010
%%%-------------------------------------------------------------------
-module(reaper_gateway).

-author("Shailen Karur").

-behaviour(gen_server).

-include("logger.hrl").
-include("reaper.hrl").
-include("jlib.hrl").

%% API
-export([start_link/1, handle_stanza/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2, code_change/3]).

-record(state, {machine_id, gateway, last_received_metadata}).

-define(METADATA_CACHE_TIME, 300000000). % set to 5 minutes (value is in microsecs)

start_link(MachineId) ->
    gen_server:start_link(?MODULE, [MachineId], []).

handle_stanza(GatewayPid, From, Stanza) ->
    gen_server:cast(GatewayPid, {stanza, From, Stanza}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([MachineId]) ->
    gen_server:cast(self(), init),
    {ok, #state{machine_id = MachineId}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(init, State = #state{machine_id = MachineId}) ->
    {noreply,
     State#state{gateway = reaper_gateway_info:for_id(MachineId),
		 last_received_metadata = now()}};
handle_cast({stanza, From, Stanza},
	    State = #state{machine_id = M, gateway = G, last_received_metadata = T}) ->
    xmpp_stanza(From, Stanza, G),
    case timer:now_diff(now(), T) of
      Diff when Diff > (?METADATA_CACHE_TIME) ->
	  {noreply,
	   State#state{gateway = reaper_gateway_info:for_id(M),
		       last_received_metadata = now()}};
      _Diff -> {noreply, State}
    end;
handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Info, State) -> {noreply, State}.

terminate(_Reason, _State) -> ok.

code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

xmpp_stanza(From, Stanza, Gateway) ->
    ?INFO_MSG("FAR::xmpp_stanza ~p ~n", [Stanza]),
    case reaper_xmpp:parse(Stanza, From) of
      #property_values{} = PropertyValues ->
	  case Gateway of
	     #gateway{soil_id = undefined} ->
	         ?ERROR_MSG("Received values from invalid gateway ~p: ~n~p", [Gateway, PropertyValues]);
	     #gateway{} ->
	       	 ?INFO_MSG("FAR::Post Gateway Properties to reaper ~p: ~n~p",
			                                                          [Gateway, PropertyValues]),
             reaper_amqp:write_async_message(io_lib:format("Gateway: ~p~n Property Values:~p~n",
                                                                      [Gateway, PropertyValues])),
		     reaper_amqp:publish_property_values(PropertyValues, Gateway)
	  end;
      #alerter_config_response{} = AlerterConfigResponse ->
	         reaper_amqp:publish_to_sod(AlerterConfigResponse);
      %% FAR: Alarm Result
      S
	     when is_record(S, status_basic);
	         is_record(S, status_advanced);
	         is_record(S, manage_packages_response);
	         is_record(S, crash_report);
	         is_record(S, alarm_result) ->
	              ?INFO_MSG("FAR::S record ~p", [S]),
	              reaper_amqp:publish_to_reaper_manager(S);
      {recipient_unavailable, live} ->
	              null; % Don't do anything in this case
      {recipient_unavailable, amr} ->
	              log_stanza(<<"Failed AMR request">>, From, Stanza, <<>>);
      {recipient_unavailable, _Other} ->
	              log_stanza(<<"Failed IQ">>, From, Stanza, <<>>);
      {error, Reason} ->
	              log_stanza(<<"Unable to parse XMPP Stanza">>, From, Stanza, Reason)
    end.

log_stanza(Message, From, Stanza, Reason) ->
    ?ERROR_MSG("~s (~s) from ~s", [Message, Reason, From#jid.luser]),
    ?INFO_MSG("Stanza: ~p", [Stanza]).
