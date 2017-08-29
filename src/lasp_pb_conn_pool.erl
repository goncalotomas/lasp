%% Heavily inspired in Peter Zeller's previous module antidote_pool.
%% This module manages connections between databases and FMKe.
-module(lasp_pb_conn_pool).
-author("Gonçalo Tomás <goncalo@goncalotomas.com>").
-behaviour(gen_nb_server).
-behaviour(poolboy_worker).
-behaviour(supervisor).

%% API
-export([start/1, with_connection/1]).

%% Supervisor callbacks
-export([init/1]).

%% Poolboy callbacks
-export([start_link/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

start(Options) ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, [Options]).

with_connection(Fun) ->
    poolboy:transaction(lasp_pb_conn_pool, Fun).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

init([Params]) ->
    ConnPoolSize = application:get_env(lasp,pb_conn_pool_size,32),
    PoolArgs = [
      {name, {local, lasp_pb_conn_pool}},
      {worker_module, ?MODULE},
      {size, ConnPoolSize},
      {max_overflow, 0}
    ],
    WorkerArgs = [],
    PoolSpec = poolboy:child_spec(lasp_pb_conn_pool, PoolArgs, WorkerArgs),
    {ok, {{one_for_one, 10, 10}, [PoolSpec]}}.

%%%===================================================================
%%% Poolboy callbacks
%%%===================================================================

start_link([]) ->
    PbPort = application:get_env(lasp,pb_port,8087),
    lasp_pb_listener:start_link("127.0.0.1", PbPort).
