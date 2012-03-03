#!/usr/bin/env escript
%%! -name claim@127.0.0.1 -setcookie riak

-module(dryrun).

-define(CHSTATE, #chstate_v2).
-record(chstate_v2, {
    nodename :: node(),          % the Node responsible for this chstate
    vclock   :: vclock:vclock(), % for this chstate object, entries are
                                 % {Node, Ctr}
    chring   :: chash:chash(),   % chash ring of {IndexAsInt, Node} mappings
    meta     :: dict(),          % dict of cluster-wide other data (primarily
                                 % bucket N-value, etc)

    clustername :: {node(), term()}, 
    next     :: [{integer(), node(), node(), [module()], awaiting | complete}],
    members  :: [{node(), {member_status(), vclock:vclock(), []}}],
    claimant :: node(),
    seen     :: [{node(), vclock:vclock()}],
    rvsn     :: vclock:vclock()
}). 
-type member_status() :: valid | invalid | leaving | exiting.

add_deps(Path) ->
    {ok, Deps} = file:list_dir(Path),
    [code:add_path(lists:append([Path, "/", Dep, "/ebin"])) || Dep <- Deps],
    ok.

main([Config]) ->
    io:format("Loading config: ~s~n", [Config]),
    Opts = 
        case file:consult(Config) of
            {ok, Terms} ->
                Terms;
            {error, Reason} ->
                erlang:error("Failed to parse config file", [Config, Reason])
        end,

    RiakLibs = proplists:get_value(riak_lib, Opts),
    Claimant = proplists:get_value(claimant, Opts),
    RingFile = proplists:get_value(ringfile, Opts),
    AppVars = proplists:get_value(riak_core, Opts, []),
    Cmds = proplists:get_value(cmds, Opts),
    add_deps(RiakLibs),

    case {Claimant, RingFile} of
        {undefined, undefined} ->
            ok;
        {Claimant, undefined} ->
            load_vars(Claimant),
            run_claimant(Claimant, Cmds);
        {undefined, RingFile} ->
            setup_environment(AppVars),
            run_ring(RingFile, Cmds);
        _ ->
            ok
    end,
    ok.

finish_transfers(Ring) ->
    Pending = riak_core_ring:pending_changes(Ring),
    Owners1 = riak_core_ring:all_owners(Ring),
    Owners2 = [{Idx,NOwner} || {Idx,_,NOwner,_,_} <- Pending],
    Owners3 = lists:ukeysort(1, Owners2++Owners1),
    CH1 = Ring?CHSTATE.chring,
    CH2 = {element(1,CH1), Owners3},
    Ring2 = Ring?CHSTATE{chring=CH2},
    Ring3 = Ring2?CHSTATE{next=[]},
    Ring3.

run_rebalance(Ring) ->
    io:format("~nAfter rebalance~n"),
    Ring2 = claim(Ring),
    result(Ring, Ring2),
    Ring2.

read_ringfile(RingFile) ->
    case file:read_file(RingFile) of
        {ok, Binary} ->
            binary_to_term(Binary);
        {error, Reason} ->
            throw({bad_ring, Reason})
    end.

setup_environment(Vars) ->
    default_vars(),
    [application:set_env(riak_core, Key, Val) || {Key, Val} <- Vars],
    ok.

load_var(Node, App, Var, _Default) ->
    Result = rpc:call(Node, application, get_env, [App, Var]),
    case Result of
        {ok, Value} ->
            application:set_env(App, Var, Value);
        _ ->
            ok
    end.

load_vars(Node) ->
    application:load(riak_core),
    load_var(Node, riak_core, default_bucket_props, [{n_val, 3}]),
    load_var(Node, riak_core, wants_claim_fun, {riak_core_claim, wants_claim_v2}),
    load_var(Node, riak_core, choose_claim_fun, {riak_core_claim, choose_claim_v2}),
    load_var(Node, riak_core, target_n_val, 4).

default_vars() ->
    application:load(riak_core),
    application:set_env(riak_core, default_bucket_props, [{n_val, 3}]),
    application:set_env(riak_core, wants_claim_fun, {riak_core_claim, wants_claim_v2}),
    application:set_env(riak_core, choose_claim_fun, {riak_core_claim, choose_claim_v2}),
    application:set_env(riak_core, target_n_val, 4).

run_ring(RingFile, CmdsL) ->
    Ring = read_ringfile(RingFile),
    dryrun(Ring, CmdsL).

run_claimant(Claimant, CmdsL) ->
    {ok, Ring} = rpc:call(Claimant, riak_core_ring_manager, get_raw_ring, []),
    TargetN = rpc:call(Claimant, app_helper, get_env, [riak_core, target_n_val]),
    application:set_env(riak_core, target_n_val, TargetN),
    dryrun(Ring, CmdsL).

dryrun(Ring00, CmdsL) ->
    io:format("Current ring:~n"),
    riak_core_ring:pretty_print(Ring00, [legend]),

    Ring0 = finish_transfers(Ring00),
    io:format("~nCurrent after pending transfers:~n"),
    riak_core_ring:pretty_print(Ring0, [legend]),

    _Ring2 = 
        lists:foldl(
          fun(Cmds, RingAcc1) ->
                  io:format("~n"),
                  NewRing =
                      lists:foldl(
                        fun(Cmd, RingAcc2) ->
                                RingAcc3 = command(Cmd, RingAcc2),
                                RingAcc3
                        end, RingAcc1, Cmds),
                  {_, NewRing2} = reassign_indices(NewRing),
                  Pending = riak_core_ring:pending_changes(NewRing2),
                  case Pending of
                      [] ->
                          NewRing3 = NewRing2;
                      _ ->
                          io:format("~nAfter new pending changes (probably "
                                    "from leave)~n"),
                          NewRing3 = finish_transfers(NewRing2),
                          result(NewRing2, NewRing3)
                  end,
                  NewRing4 = run_rebalance(NewRing3),
                  NewRing5 = run_rebalance(NewRing4),
                  NewRing5
          end, Ring0, CmdsL),
    ok.

sim_node(N) ->
    list_to_atom(lists:flatten(io_lib:format("sim~b@basho.com",[N]))).
    
command({join, Num}, Ring) when is_integer(Num) ->
    lists:foldl(fun(N, RingAcc) ->
                        Node = sim_node(N),
                        command({join, Node}, RingAcc)
                end, Ring, lists:seq(1,Num));

command({join, Node}, Ring) ->
    Members = riak_core_ring:all_members(Ring),
    %% (not lists:member(Node, Members)) orelse throw(invalid_member),
    io:format("Joining ~p~n", [Node]),
    Ring2 = riak_core_ring:add_member(Node, Ring, Node),
    Ring2;
command({leave, Node}, Ring) ->
    Members = riak_core_ring:all_members(Ring),
    lists:member(Node, Members) orelse throw(invalid_member),
    io:format("Leaving ~p~n", [Node]),
    Ring2 = riak_core_ring:leave_member(Node, Ring, Node),
    Ring2.

claim(Ring) ->
    Members = riak_core_ring:claiming_members(Ring),
    Ring2 = lists:foldl(fun(Node, Ring0) ->
                                riak_core_gossip:claim_until_balanced(Ring0,
                                                                      Node)
                        end, Ring, Members),
    Ring2.

result(Ring, Ring2) ->
    Owners1 = riak_core_ring:all_owners(Ring),
    Owners2 = riak_core_ring:all_owners(Ring2),
    Owners3 = lists:zip(Owners1, Owners2),
    Next = [{Idx, PrevOwner, NewOwner}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner],
    Tally =
        lists:foldl(fun({_, PrevOwner, NewOwner}, Tally) ->
                            dict:update_counter({PrevOwner, NewOwner}, 1, Tally)
                    end, dict:new(), Next),
    riak_core_ring:pretty_print(Ring2, [legend]),
    io:format("Pending: ~p~n", [length(Next)]),
    io:format("Check: ~p~n", [riak_core_ring_util:check_ring(Ring2)]),
    [io:format("~b transfers from ~p to ~p~n", [Count, PrevOwner, NewOwner])
     || {{PrevOwner, NewOwner}, Count} <- dict:to_list(Tally)],
    ok.

%% Lovely copy/paste + modify code from riak_core to model node removal
reassign_indices(CState=?CHSTATE{next=Next}) ->
    Invalid = get_members(CState?CHSTATE.members, [invalid]),
    CState2 =
        lists:foldl(fun(Node, CState0) ->
                            remove_node(CState0, Node, invalid)
                    end, CState, Invalid),
    CState3 = case Next of
                  [] ->
                      Leaving = get_members(CState?CHSTATE.members, [leaving]),
                      lists:foldl(fun(Node, CState0) ->
                                          remove_node(CState0, Node, leaving)
                                  end, CState2, Leaving);
                  _ ->
                      io:format("N: ~p~n", [Next]),
                      throw(next),
                      CState2
              end,
    Owners1 = riak_core_ring:all_owners(CState),
    Owners2 = riak_core_ring:all_owners(CState3),
    RingChanged = (Owners1 /= Owners2),
    NextChanged = (Next /= CState3?CHSTATE.next),
    {RingChanged or NextChanged, CState3}.

%% @private
remove_node(CState, Node, Status) ->
    Indices = riak_core_ring:indices(CState, Node),
    remove_node(CState, Node, Status, Indices).

%% @private
remove_node(CState, _Node, _Status, []) ->
    CState;
remove_node(CState, Node, Status, Indices) ->
    CStateT1 = change_owners(CState, all_next_owners(CState)),
    CStateT2 = riak_core_gossip:remove_from_cluster(CStateT1, Node),
    Owners1 = riak_core_ring:all_owners(CState),
    Owners2 = riak_core_ring:all_owners(CStateT2),
    Owners3 = lists:zip(Owners1, Owners2),
    RemovedIndices = case Status of
                         invalid ->
                             Indices;
                         leaving ->
                             []
                     end,
    Reassign = [{Idx, NewOwner} || {Idx, NewOwner} <- Owners2,
                                   lists:member(Idx, RemovedIndices)],
    Next = [{Idx, PrevOwner, NewOwner, [], awaiting}
            || {{Idx, PrevOwner}, {Idx, NewOwner}} <- Owners3,
               PrevOwner /= NewOwner,
               not lists:member(Idx, RemovedIndices)],

    %% [log(reassign, {Idx, NewOwner, CState}) || {Idx, NewOwner} <- Reassign],

    %% Unlike rebalance_ring, remove_node can be called when Next is non-empty,
    %% therefore we need to merge the values. Original Next has priority.
    Next2 = lists:ukeysort(1, CState?CHSTATE.next ++ Next),
    CState2 = change_owners(CState, Reassign),
    CState2?CHSTATE{next=Next2}.

%% @private
get_members(Members, Types) ->
    [Node || {Node, {V, _, _}} <- Members, lists:member(V, Types)].

%% @private
all_next_owners(?CHSTATE{next=Next}) ->
    [{Idx, NextOwner} || {Idx, _, NextOwner, _, _} <- Next].

%% @private
change_owners(CState, Reassign) ->
    lists:foldl(fun({Idx, NewOwner}, CState0) ->
                        riak_core_ring:transfer_node(Idx, NewOwner, CState0)
                end, CState, Reassign).
