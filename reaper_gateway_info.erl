%% @author Shailen Karur
%% @copyright 2014-2015 Shailen Karur
%% @doc This module handles the retrieval of gateway metadata from the reaper&trade; Manager.  It stores the data in a read-through ETS cache that is updated in a background when possible, so retrievals are usually quite fast in the normal case. As new gateways are added to ejabberd, they should be added here as well.

%% @author Shailen Karur <ombuds.sattva@gmail.com>
%% @doc This is a rewrite by Shailen Karur of the original module from ProcessOne to work on eBE

-module(reaper_gateway_info).

-behavior(gen_server).

-include("logger.hrl").
-include("reaper.hrl").

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2,
	 handle_info/2, terminate/2, code_change/3]).

-export([start_link/1, for_id/1, refresh_cache/0,
	 store_gateway/1, rename_gateway/2]).

-record(state, {reaper_manager_uri, cache}).

-define(SERVER, ?MODULE).
-define(CACHE_REFRESH, 300000).
-define(UNAVAILABLE_GATEWAY, #gateway{machine_id = MachineId}).

start_link(reaperManagerUri) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [reaperManagerUri], []).

for_id(MachineId) ->
    gen_server:call(?SERVER, {retrieve, MachineId}, infinity).

refresh_cache() ->
    gen_server:cast(?SERVER, refresh_cache).

store_gateway(Gateway) ->
    broadcast_cluster_cache(cast, {store, Gateway}),
    gen_server:cast(?SERVER, {store, Gateway}).

rename_gateway(Old, New) ->
    broadcast_cluster_cache(call, {rename, Old, New}),
    gen_server:call(?SERVER, {rename, Old, New}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([reaperManagerUri]) ->
    inets:start(),
    Cache = ets:new(gateway_cache, [{keypos, #gateway.machine_id}]),
    timer:apply_interval(?CACHE_REFRESH, ?MODULE, refresh_cache, []),
    gen_server:cast(self(), refresh_cache),
    {ok, #state{reaper_manager_uri = reaperManagerUri, cache = Cache}}.

handle_call({retrieve, MachineId}, _From, State) ->
    case ets:lookup(State#state.cache, MachineId) of
      [Gateway] -> {reply, Gateway, State};
      [] -> {reply, ?UNAVAILABLE_GATEWAY, State}
    end;
handle_call({store, Gateways}, _From, State) ->
    ets:insert(State#state.cache, Gateways),
    {reply, ok, State};
handle_call({rename, Old, New}, _From, State) ->
    case ets:lookup(State#state.cache, Old) of
      [Gateway] ->
	  ets:insert(State#state.cache, Gateway#gateway{machine_id = New}),
	  {reply, true, State};
      [] ->
	  {reply, false, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast({store, Gateway}, State) ->
    ets:insert(State#state.cache, Gateway),
    {noreply, State};
handle_cast(refresh_cache, State) ->
    spawn(fun () ->
		  refresh_cache(State#state.reaper_manager_uri)
	  end),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------

refresh_cache(reaperManagerUri) ->
    ?INFO_MSG("Refreshing cache of gateways", []),
    case retrieve_gateways(reaperManagerUri) of
	[] ->
	    %% This generally means we couldn't parse the response from the API.
	    %% In that case, just leave what was in the cache there.
	    ok;
	Gateways ->
	    gen_server:call(?SERVER, {store, Gateways})
    end.

valid_gateway({struct, Props}) ->
    case reaper_util:get_json_property(<<"machine_id">>, Props) of
	null ->
	    ?ERROR_MSG("Invalid Gateway (no machine_id) ~p", [Props]),
	    false;
	_ ->
	    true
    end.

gateway_from_json({struct, Props}) ->
    #gateway{machine_id = reaper_util:get_json_property(<<"machine_id">>, Props),
	     soil_id = reaper_util:get_json_property(<<"name">>, Props),
	     utc_offset = reaper_util:get_json_property(<<"utc_offset">>, Props),
	     production_soil = reaper_util:get_json_property(<<"production_soil">>, Props),
	     last_retrieved = now()}.

retrieve_gateways(reaperManagerUri) ->
    RequestURI = reaperManagerUri ++ "/api/reaper_gateways",
    case httpc:request(get,
		       {RequestURI, [{"Accept", "application/json"}]},
		       [], [])
	of
      {ok, {_Status, _Headers, Body}} ->
	  Gateways = try reaper_util:get_json(Body) catch
		       error:X ->
			   ?ERROR_MSG("reaper: Error parsing JSON from reaper "
				      "Manager API.  Error was: ~p, ~p.  Content was: ~p",
				      [X, erlang:get_stacktrace(), RequestURI]),
			   []
		     end,
	  [gateway_from_json(G) || G <- Gateways, valid_gateway(G)];
      {error, Reason} ->
	  ?ERROR_MSG("Error refreshing cache ~p~n", [Reason]), []
    end.

broadcast_cluster_cache(Method, Call) ->
    % apply server call to other cluster nodes to get cache in sync
    % this can be done in background
    spawn(fun () ->
		case ejabberd_cluster:get_nodes() -- [node()] of
		    [] -> ok;
		    Nodes -> ejabberd_cluster:multicall(Nodes,
			    gen_server, Method, [?SERVER, Call])
		end
	  end).
