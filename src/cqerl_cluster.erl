-module (cqerl_cluster).

-export([
    init/1,
    terminate/2,
    code_change/3,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-export([
	start_link/0,

	get_any_client/1,
	get_any_client/0,

    add_nodes/1,
    add_nodes/2,
    add_nodes/3
]).

-type client_key() :: cqerl_hash:key().

-define(PRIMARY_CLUSTER, '$primary_cluster').
-define(ADD_NODES_TIMEOUT, case application:get_env(cqerl, add_nodes_timeout) of
    undefined -> 30000;
    {ok, Val} -> Val
end).

-record(cluster_table, {
          key :: cqerl_hash:key(),
          client_key :: cqerl_hash:key()
         }).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec add_nodes([client_key()]) -> [{ok, any()} | {error, any()}].
add_nodes(ClientKeys) ->
    gen_server:call(?MODULE, {add_to_cluster, ?PRIMARY_CLUSTER, ClientKeys}, ?ADD_NODES_TIMEOUT).

add_nodes(ClientKeys, Opts) when is_list(ClientKeys) ->
    add_nodes(?PRIMARY_CLUSTER, ClientKeys, Opts);

add_nodes(Key, ClientKeys) when is_atom(Key) ->
    gen_server:call(?MODULE, {add_to_cluster, Key, ClientKeys}, ?ADD_NODES_TIMEOUT).

add_nodes(Key, ClientKeys, Opts0) ->
	add_nodes(Key, lists:map(fun
		({Inet, Opts}) when is_list(Opts) ->
			{Inet, Opts ++ Opts0};
		(Inet) ->
			{Inet, Opts0}
	end, ClientKeys)).

get_any_client(Key) ->
	case ets:lookup(cqerl_clusters, Key) of
		[] -> {error, cluster_not_configured};
		Nodes ->
            #cluster_table{client_key = {Node, Opts}} =
                           lists:nth(rand:uniform(length(Nodes)), Nodes),
			cqerl_hash:get_client(Node, Opts)
	end.

get_any_client() ->
	get_any_client(?PRIMARY_CLUSTER).

init(_) ->
    ets:new(cqerl_clusters, [named_table, {read_concurrency, true}, protected, 
                             {keypos, #cluster_table.key}, bag]),
    load_initial_clusters(),
    {ok, undefined}.

handle_cast(_Msg, State) -> 
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

handle_call({add_to_cluster, ClusterKey, ClientKeys}, _From, State) ->
    Reply = do_add_to_cluster(ClusterKey, ClientKeys),
    {reply, Reply, State};

handle_call(_Msg, _From, State) -> 
    {reply, {error, unexpected_message}, State}.

code_change(_OldVsn, State, _Extra) -> 
    {ok, State}.

terminate(_Reason, _State) ->
	ok.

prepare_client_keys(ClientKeys) ->
    prepare_client_keys(ClientKeys, []).

prepare_client_keys(ClientKeys, SharedOpts) ->
    lists:map(fun
        ({Inet, Opts}) when is_list(Opts) ->
            {cqerl:prepare_node_info(Inet), Opts ++ SharedOpts};
        (Inet) ->
            {cqerl:prepare_node_info(Inet), SharedOpts}
    end, ClientKeys).

load_initial_clusters() ->
    case application:get_env(cqerl, cassandra_clusters, undefined) of
        undefined ->
            case application:get_env(cqerl, cassandra_nodes, undefined) of
                undefined -> [];
                ClientKeys when is_list(ClientKeys) ->
                    do_add_to_cluster(?PRIMARY_CLUSTER, prepare_client_keys(ClientKeys))
            end;
        Clusters when is_list(Clusters) ->
            lists:flatmap(fun
                ({ClusterKey, {ClientKeys, Opts0}}) when is_list(ClientKeys) ->
                    do_add_to_cluster(ClusterKey, prepare_client_keys(ClientKeys, Opts0));
                ({ClusterKey, ClientKeys}) when is_list(ClientKeys) ->
                    do_add_to_cluster(ClusterKey, prepare_client_keys(ClientKeys))
            end, Clusters)
    end.

do_add_to_cluster(ClusterKey, ClientKeys) ->
    GlobalOpts = application:get_all_env(cqerl),
    NewClients = determine_new_clients(ClusterKey, ClientKeys),
    lists:map(fun (Key = {Node, Opts}) ->
        case cqerl_hash:get_client(Node, Opts) of
            {ok, R} ->
                ets:insert(cqerl_clusters, #cluster_table{key=ClusterKey, client_key=Key}),
                {ok, R};
            {error, Reason} ->
                {error, Reason}
        end
    end, prepare_client_keys(NewClients, GlobalOpts)).

determine_new_clients(ClusterKey, ClientKeys) ->
    Clusters = ets:lookup(cqerl_clusters, ClusterKey),
    AlreadyStarted = sets:from_list([ C#cluster_table.client_key || C <- Clusters ]),
    sets:to_list( sets:subtract(sets:from_list(ClientKeys), AlreadyStarted) ).
