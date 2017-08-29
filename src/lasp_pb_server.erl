-module(lasp_pb_server).
-behaviour(gen_server).

-include_lib("lasp_pb/include/lasp_pb.hrl").

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%% Defines for state encoding
-define (CRDT_COUNTERS, [bcounter, gcounter, pncounter, lexcounter]).
-define (CRDT_FLAGS, [ewflag, dwflag]).
-define (CRDT_SETS, [gset, awset, rwset, twopset, orset, gset]).
-define (CRDT_REGISTERS, [mvregister, lwwregister]).
-define (CRDT_MAPS, [mvmap, gmap, awmap]).

-record(state, {socket}).

start_link(Socket) ->
    gen_server:start_link(?MODULE, Socket, []).

init(Socket) ->
    gen_server:cast(self(), accept),
    {ok, #state{socket=Socket}}.

handle_cast(accept, State = #state{socket=ListenSocket}) ->
    lager:info("[PB Server]: Accepted new connection"),
    {ok, AcceptSocket} = gen_tcp:accept(ListenSocket),
    %% Boot a new listener to replace this one.
    lasp_pb_sup:start_socket(),
    {noreply, State#state{socket=AcceptSocket}};
handle_cast(_, State) ->
    {noreply, State}.

handle_info({tcp, Socket, Msg}, State) ->
    Decoded = decode_pb(Msg, req),
    Response = perform_op(Decoded),
    lager:info("[PB Server]: Received message from active connection, decoded value is:~n~p~n", [Decoded]),
    lager:info("Calling perform_op()... Result was ~p~nSending result back...", [lasp_pb:decode_msg(Response,reqresp)]),
    send(Socket, Response, []),
    {noreply, State};
handle_info({tcp_closed, _Socket}, State) ->
    lager:info("[PB Server]: A PB connection was closed."),
    {stop, normal, State};
handle_info({tcp_error, _Socket, _}, State) -> {stop, normal, State};
handle_info(E, State) ->
    lager:info("[PB Server]: Unexpected input: ~p~n",[E]),
    {noreply, State}.

perform_op({req, {get, {K, T}}}) ->
    case lasp:query({K, T}) of
        {ok, Value} ->
                            lager:info("crdt type = ~p",[get_crdt_type(T)]),
                            lager:info("value = ~p",[Value]),
                            CrdtType = get_crdt_type(T),
                            encode_pb({response, T, encode_value(CrdtType, Value)});
        {error, Reason} ->  encode_pb({response, error, Reason});
        Other ->            lager:info("Lasp query operation failed for unknown reasons!~n~p~n",[Other])
    end;

perform_op({req, {put, {{K, T}, Op, Actor}}}) ->
    case lasp:update({K, T}, Op, Actor) of
        {ok, Value} ->      encode_pb({response, success, true});
        {error, Reason} ->  encode_pb({response, error, Reason});
        Other ->            lager:info("Lasp query operation failed for unknown reasons!~n~p~n",[Other])
    end.

% update_and_reply(KeyId, Update, Actor) ->
%     lager:info("Inside update_and_reply Updates=~p~n",[Update]),
%     case lasp:update(KeyId, Update, Actor) of
%         {ok, Value} ->      encode_resp(success, true);
%         {error, Reason} ->  encode_resp(error, Reason);
%         Other ->            lager:info("Lasp query operation failed for unknown reasons!~n~p~n",[Other])
%     end.

encode_value(reg, Val) when is_atom(Val) ->       atom_to_list(Val);
encode_value(reg, Val) when is_list(Val) ->       Val;
encode_value(ctr, Val) ->                         Val;
encode_value(map, Map) ->                         Map;
encode_value(set, Set) ->                         sets:to_list(Set).

%% unneeded (here to implement the gen_server behaviour)
handle_call(_E, _From, State) -> {noreply, State}.
terminate(_Reason, _Tab) -> ok.
code_change(_OldVersion, Tab, _Extra) -> {ok, Tab}.

%% Send a message back to the client
send(Socket, Str, _) when is_binary(Str)->
    gen_tcp:send(Socket, Str);

%% Send a message back to the client
send(Socket, Msg, Args) ->
    lager:info("[PB Server]: Using default non-binary send function, encoding to protobuffer"),
    gen_tcp:send(Socket, lasp_pb:encode_msg(Msg)).

%% -------------------------------------------------------------------
%% Protobuffer related functions
%% -------------------------------------------------------------------
decode_pb(Msg, MsgType) ->
    lasp_pb_codec:decode(lasp_pb:decode_msg(Msg,MsgType)).

encode_pb(Msg) ->
    lasp_pb:encode_msg(lasp_pb_codec:encode(Msg)).

%% -------------------------------------------------------------------
%% Helper functions
%% -------------------------------------------------------------------

get_crdt_type(Type) ->
    Types = [
        {ctr, ?CRDT_COUNTERS},
        {reg, ?CRDT_REGISTERS},
        {flag, ?CRDT_FLAGS},
        {set, ?CRDT_SETS},
        {map, ?CRDT_MAPS}
    ],
    search_type(Type,Types).

search_type(_Type, []) ->
    undefined;
search_type(Type, [H|T]) ->
    {ReturnType, List} = H,
    case lists:member(Type, List) of
        true -> ReturnType;
        false -> search_type(Type, T)
    end.
