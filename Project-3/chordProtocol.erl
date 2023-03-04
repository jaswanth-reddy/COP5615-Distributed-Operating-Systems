-module(chordProtocol).
-export([init_protocol/2, init_network/2, process_task/2, process_node/1, init_node/4]).



generate_random_node(Node_ID, []) -> Node_ID;
generate_random_node(_, Present_Nodes) -> lists:nth(rand:uniform(length(Present_Nodes)), Present_Nodes).


appendNode_Protocol(NodesIn_Protocol, Count_AllNodes, Raise, Net_State) ->
    Pending_Hashes = lists:seq(0, Count_AllNodes - 1, 1) -- NodesIn_Protocol,
    Hashed_Val = lists:nth(rand:uniform(length(Pending_Hashes)), Pending_Hashes),
    Process_ID = spawn(chordProtocol, init_node, [Hashed_Val, Raise, NodesIn_Protocol, dict:new()]),
    [Hashed_Val, dict:store(Hashed_Val, Process_ID, Net_State)]
.


fetchForward_dist(KeyDesc, KeyDesc, _, Dist) ->
    Dist;
fetchForward_dist(KeyDesc, Node_ID, Raise, Dist) ->
    fetchForward_dist(KeyDesc, (Node_ID + 1) rem trunc(math:pow(2, Raise)), Raise, Dist + 1)
.

fetch_Near(_, [], Least_Node, _, _) ->
    Least_Node;
fetch_Near(KeyDesc, FingerNode_IDs, Least_Node, Least_Value, State) ->
    [Start| Rem] = FingerNode_IDs,
    Dist = fetchForward_dist(KeyDesc, Start, dict:fetch(m, State), 0),
    if
        Dist < Least_Value ->
            fetch_Near(KeyDesc, Rem, Start, Dist, State);
        true -> 
            fetch_Near(KeyDesc, Rem, Least_Node, Least_Value, State)
    end
.

fetch_NearestNode(KeyDesc, FingerNode_IDs, State) ->
    case lists:member(KeyDesc, FingerNode_IDs) of
        true -> KeyDesc;
        _ -> fetch_Near(KeyDesc, FingerNode_IDs, -1, 10000000, State)
    end

.


checkInRange(StartPoint, EndPoint, KeyDesc, Raise) ->
    if 
        StartPoint < EndPoint -> 
            (StartPoint =< KeyDesc) and (KeyDesc =< EndPoint);
        trunc(StartPoint) == trunc(EndPoint) ->
            trunc(KeyDesc) == trunc(StartPoint);
        StartPoint > EndPoint ->
            ((KeyDesc >= 0) and (KeyDesc =< EndPoint)) or ((KeyDesc >= StartPoint) and (KeyDesc < trunc(math:pow(2, Raise))))
    end
.

prevFinger_Nearest(_, Node_State, 0) -> Node_State;
prevFinger_Nearest(ID_VAL, Node_State, Raise) -> 
    KthFinger = lists:nth(Raise, dict:fetch(finger_table, Node_State)),
    
    case checkInRange(dict:fetch(iD_Val, Node_State), ID_VAL, dict:fetch(init_node ,KthFinger), dict:fetch(m, Node_State)) of
        true -> 

            dict:fetch(process_ID ,KthFinger) ! {state, self()},
            receive
                {state_response, State_FingerNode} ->
                    State_FingerNode
            end,
            State_FingerNode;

        _ -> prevFinger_Nearest(ID_VAL, Node_State, Raise - 1)
    end
.

fetchPrevious(ID_VAL, Node_State) ->
    case 
        checkInRange(dict:fetch(iD_Val, Node_State) + 1, dict:fetch(iD_Val, dict:fetch(nextMember, Node_State)), ID_VAL, dict:fetch(m, Node_State)) of 
            true -> Node_State;
            _ -> fetchPrevious(ID_VAL, prevFinger_Nearest(ID_VAL, Node_State, dict:fetch(m, Node_State)))
    end
.

fetchNext(ID_VAL, Node_State) ->
    PrevNode_State = fetchPrevious(ID_VAL, Node_State),
    dict:fetch(nextMember, PrevNode_State)
.


process_node(Node_State) ->
    Hashed_Val = dict:fetch(iD_Val, Node_State),
    receive
            
            
        {find, ID_VAL, KeyDesc, Count_Of_Hops, _Process_ID} ->

                Value_Of_Node = fetch_NearestNode(KeyDesc, dict:fetch_keys(dict:fetch(finger_table ,Node_State)), Node_State),
                Latest_State = Node_State,
                if 
                    (Hashed_Val == KeyDesc) -> 
                        is_Task_Complete ! {complete, Hashed_Val, Count_Of_Hops, KeyDesc};
                    (Value_Of_Node == KeyDesc) and (Hashed_Val =/= KeyDesc) -> 
                        is_Task_Complete ! {complete, Hashed_Val, Count_Of_Hops, KeyDesc};
                    
                    true ->
                        dict:fetch(Value_Of_Node, dict:fetch(finger_table, Node_State)) ! {find, ID_VAL, KeyDesc, Count_Of_Hops + 1, self()}
                end
                ;
        {kill} ->
            Latest_State = Node_State,
            exit("Exit Signal is Received");
        {state, Process_ID} -> Process_ID ! Node_State,
                        Latest_State = Node_State;
        {fetch_NextMember, ID_VAL, Process_ID} ->
                        FoundSeccessor = fetchNext(ID_VAL, Node_State),
                        Latest_State = Node_State,
                        {Process_ID} ! {get_successor_reply, FoundSeccessor};

        
        {fix_fingers, Finger_Table} -> 
            % io:format("Received Finger for ~p ~p", [Hashed_Val, Finger_Table]),
            Latest_State = dict:store(finger_table, Finger_Table, Node_State)
    end, 
    process_node(Latest_State).


init_node(Hashed_Val, Raise, NodesIn_Protocol, _NodeState) -> 
    %io:format("Node is spawned with hash ~p",[Hashed_Val]),
    Finger_Table = lists:duplicate(Raise, generate_random_node(Hashed_Val, NodesIn_Protocol)),
    New_Node_State = dict:from_list([{iD_Val, Hashed_Val}, {prev_node, nil}, {finger_table, Finger_Table}, {nxt, 0}, {m, Raise}]),
    process_node(New_Node_State)        
.


generate_new_nodes(NodesIn_Protocol, _, _, 0, Net_State) -> 
    [NodesIn_Protocol, Net_State];
generate_new_nodes(NodesIn_Protocol, Count_AllNodes, Raise, Count_Nodes, Net_State) ->
    [Hashed_Val, New_Net_State] = appendNode_Protocol(NodesIn_Protocol, Count_AllNodes,  Raise, Net_State),
    generate_new_nodes(lists:append(NodesIn_Protocol, [Hashed_Val]), Count_AllNodes, Raise, Count_Nodes - 1, New_Net_State)
.



fetch_kth_nextMember(Hashed_Val, Net_State, K,  Raise) -> 
    case dict:find((Hashed_Val + K) rem trunc(math:pow(2, Raise)), Net_State) of
        error ->
             fetch_kth_nextMember(Hashed_Val, Net_State, K + 1, Raise);
        _ -> (Hashed_Val + K) rem trunc(math:pow(2, Raise))
    end
.

fetch_fingerTable(_, _, Raise, Raise,Finger_List) ->
    Finger_List;
fetch_fingerTable(Node, Net_State, Raise, K, Finger_List) ->
    Hashed_Val = element(1, Node),
    Kth_NextMember = fetch_kth_nextMember(Hashed_Val, Net_State, trunc(math:pow(2, K)), Raise),
    fetch_fingerTable(Node, Net_State, Raise, K + 1, Finger_List ++ [{Kth_NextMember, dict:fetch(Kth_NextMember, Net_State)}] )
.


collect_finger_tables(_, [], FTDict,_) ->
    FTDict;

collect_finger_tables(Net_State, NetList, FTDict,Raise) ->
    [Start | Rem] = NetList,
    Finger_Tables = fetch_fingerTable(Start, Net_State,Raise, 0,[]),
    collect_finger_tables(Net_State, Rem, dict:store(element(1, Start), Finger_Tables, FTDict), Raise)
.



relay_FTNodes([], _, _) ->
    ok;
relay_FTNodes(Relaying_Node, Net_State, Finger_Tables) ->
    [Start|Rem] = Relaying_Node,
    Process_ID = dict:fetch(Start ,Net_State),
    Process_ID ! {fix_fingers, dict:from_list(dict:fetch(Start, Finger_Tables))},
    relay_FTNodes(Rem, Net_State, Finger_Tables)
.


relay_FT(Net_State,Raise) ->
    Finger_Tables = collect_finger_tables(Net_State, dict:to_list(Net_State), dict:new(),Raise),
    relay_FTNodes(dict:fetch_keys(Finger_Tables), Net_State, Finger_Tables)
.

fetch_NPID(Hashed_Val, Net_State) -> 
    case dict:find(Hashed_Val, Net_State) of
        error -> nil;
        _ -> dict:fetch(Hashed_Val, Net_State)
    end
.

relay_msg2Node(_, [], _) ->
    ok;
relay_msg2Node(KeyDesc, NodesIn_Protocol, Net_State) ->
    [Start | Rem] = NodesIn_Protocol,
    Process_ID = fetch_NPID(Start, Net_State),
    Process_ID ! {find, Start, KeyDesc, 0, self()},
    relay_msg2Node(KeyDesc, Rem, Net_State)
.

relay_msg2All(_, 0, _, _) ->
    ok;
relay_msg2All(NodesIn_Protocol, Requested_Number, Raise, Net_State) ->
    timer:sleep(1000),
    KeyDesc = lists:nth(rand:uniform(length(NodesIn_Protocol)), NodesIn_Protocol),
    relay_msg2Node(KeyDesc, NodesIn_Protocol, Net_State),
    relay_msg2All(NodesIn_Protocol, Requested_Number - 1, Raise, Net_State)
.

killAll([], _) ->
    ok;
killAll(NodesIn_Protocol, Net_State) -> 
    [Start | Rem] = NodesIn_Protocol,
    fetch_NPID(Start, Net_State) ! {kill},
    killAll(Rem, Net_State).

fetch_HopsCount() ->
    receive
        {hops_count, Count_Of_Hops} ->
            Count_Of_Hops
        end.


process_task(0, Count_Of_Hops) ->
    initProc ! {hops_count, Count_Of_Hops}
;

process_task(Requested_Number, Count_Of_Hops) ->
    receive 
        {complete, _Process_ID, TaskHops_Cnt, _KeyDesc} ->
            % io:format("received completion from ~p, Number of Hops ~p, For KeyDesc ~p", [Process_ID, TaskHops_Cnt, KeyDesc]),
            process_task(Requested_Number - 1, Count_Of_Hops + TaskHops_Cnt)
    end
.


relayMsg_Kill(NodesIn_Protocol, Count_Nodes, Requested_Number, Raise, Net_State) ->
    register(is_Task_Complete, spawn(chordProtocol, process_task, [Count_Nodes * Requested_Number, 0])),

    relay_msg2All(NodesIn_Protocol, Requested_Number, Raise, Net_State),

    TotalHops = fetch_HopsCount(),
    
    {ok, File} = file:open("./Result.txt", [append]),
    io:format(File, "~n Average Hops = ~p   TotalHops = ~p    Count_Nodes = ~p    Requested_Number = ~p  ~n", [TotalHops/(Count_Nodes * Requested_Number), TotalHops, Count_Nodes , Requested_Number]),
    io:format("~n Average Hops = ~p   TotalHops = ~p    Count_Nodes = ~p    Requested_Number = ~p  ~n", [TotalHops/(Count_Nodes * Requested_Number), TotalHops, Count_Nodes , Requested_Number]),
    killAll(NodesIn_Protocol, Net_State)
.

get_m(Count_Nodes) ->
    trunc(math:ceil(math:log2(Count_Nodes)))
.

init_network(Count_Nodes, Requested_Number) ->
    Raise = get_m(Count_Nodes),
    [NodesIn_Protocol, Net_State] = generate_new_nodes([], round(math:pow(2, Raise)), Raise, Count_Nodes, dict:new()),
    
    relay_FT(Net_State,Raise),
    relayMsg_Kill(NodesIn_Protocol, Count_Nodes, Requested_Number, Raise, Net_State)
.


init_protocol(Count_Nodes, Requested_Number) ->
    register(initProc, spawn(chordProtocol, init_network, [Count_Nodes, Requested_Number]))
.
