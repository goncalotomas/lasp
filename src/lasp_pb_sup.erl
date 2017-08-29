%%% The supervisor in charge of all the socket acceptors.
-module(lasp_pb_sup).
-behaviour(supervisor).

-export([start_link/0, start_socket/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, Port} = application:get_env(lasp, pb_port),
    {ok, ListenSocket} = gen_tcp:listen(Port, [binary, {packet, 4}, {active, true}]),
    %% We start our pool of empty listeners.
    %% We must do this in another, as it is a blocking process.
    spawn_link(fun empty_listeners/0),
    {ok, { {simple_one_for_one, 60, 3600},
           [
            {lasp_pb_server, {lasp_pb_server, start_link, [ListenSocket]}, temporary, 1000, worker, [lasp_pb_server]}
           ]
         } }.

start_socket() ->
    supervisor:start_child(?MODULE, []).

%% Start with 20 listeners so that many multiple connections can
%% be started at once, without serialization. In best circumstances,
%% a process would keep the count active at all times to insure nothing
%% bad happens over time when processes get killed too much.
empty_listeners() ->
    [start_socket() || _ <- lists:seq(1,20)],
    ok.
