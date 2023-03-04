-module(twitterclient).
-export([join/1, fetchusers/0, subscribe/2 , processtweet/2, joinuser/1,processretweet/3, simulatetwitter/1, listentweet/1, search/2, fetchsubdetails/3, registeruser/1, intiateretweet/2, subscribe_to_user/1, initiatetweet/1]).



listentweet(Username) -> 
    receive 
        {initiatetweet, Tweet, FromUser} -> 
            % {ok, Fd} = file:open("feed.txt", [append]),
            % io:format(Fd, "~p's Feed Update~n~p:~p ~n", [Username,FromUser,Tweet]);
            ok;
        {disconnect} -> disconnected
    end,
    listentweet(Username)
.


joinuser(Username) ->
    Pid = spawn(twitterclient, listentweet, [Username]),
    engine ! {registeruser, Username,Pid},
    [Pid, Username]
.

join(Username) -> 
    spawn(twitterclient, joinuser, [Username])
.

subscribe(SubscriberName, SubscribeeName) ->
    engine ! {subscribe , SubscriberName, SubscribeeName}
.

search(Type, Term) ->
    engine ! {querytweet , Type, Term, self()},
    receive 
        {result, ResultsDict} -> 
            % {ok, Fd} = file:open("queryresults.txt", [append]),
            % io:format(Fd, "Results ~p~n",[ResultsDict])
            ok
end
.

processtweet(Username, Message) ->
    engine ! {sendtweet, Username, Message}
.

processretweet(Username, Usermain , Message_id) ->
    engine ! {intiateretweet , Username , Usermain , Message_id}
.

fetchusers() -> 
    engine ! {fetchusers, self()},
    receive 
        {userlist, Users} -> io:format("~p", [Users])
end.





fetchrndname() ->
    Length = 5,
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(rand:uniform(length("ABCDEFGHIJKLMNOPQRSTUVWXYZ")),
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ")]
                            ++ Acc
                end, [], lists:seq(1, Length)).


fetchrndmsg() ->
    Length = 15,
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(rand:uniform(length("ABCDEFGHIJKLMNOPQRSTUVWXYZ   ")),
                                    "ABCDEFGHIJKLMNOPQRSTUVWXYZ   ")]
                            ++ Acc
                end, [], lists:seq(1, Length)).


createusers(0, UserList) -> UserList;
createusers(UserCount, UserList) ->
    UserDetails = joinuser(fetchrndname()),
    createusers(UserCount - 1, lists:append(UserList, [UserDetails]))
.


createusersubscriptions(_, []) -> ok;
createusersubscriptions(User, SubUsers) -> 
    [First| Rest] = SubUsers,
    subscribe(lists:last(User), lists:last(First)),
    createusersubscriptions(User, Rest)
.

getnusers(_, _, 0, _, SubList, _) -> SubList; 
getnusers(UserList, User, Count, UserCount, SubList, I) -> 
    U = lists:nth(I, UserList),
    getnusers(UserList, User, Count - 1,UserCount, lists:append(SubList, [U]), ((I rem UserCount) + 1))
.



processsubscriptions(UserCount, UserList, I) -> 
    if I > UserCount -> 
        ok;
        true ->
            User = lists:nth(I, UserList),
            SubUsers = getnusers(UserList, User, round(UserCount * (1 / I)), UserCount, [], (I rem UserCount) + 1),
            createusersubscriptions(User, SubUsers),
            processsubscriptions(UserCount, UserList, I + 1)
    end
.

fetchrndtwitterhandle() ->
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(rand:uniform(length("#@")),
                                   "#@")]
                            ++ Acc
                end, [], lists:seq(1, 1)).


fetchrndusername(Usernames) ->
    lists:nth(rand:uniform(length(Usernames)), Usernames)
    .


fetchsubdetails(UserCount, UserList, I) -> 
    if I > UserCount -> 
        ok;
        true -> 
            User = lists:nth(I, UserList),
            engine ! {getsubscriber, lists:last(lists:nth(I, UserList)), self()},
            receive 
                {subscriber, V} -> io:format("~p :::: ~p ~n", [User, V])
            end,
            fetchsubdetails(UserCount, UserList, I + 1)
    end
    .

getnmessages(0, _, Messages) -> Messages; 
getnmessages(Count, Usernames, Messages) ->
    getnmessages(Count - 1, Usernames, lists:append(Messages, [fetchrndmsg() ++ " "  ++ fetchrndtwitterhandle() ++ lists:last(fetchrndusername(Usernames))]))
    .

sendusertweets(_, []) -> ok;
sendusertweets(User, Tweets) ->
    [First|_] = Tweets,
    processtweet(lists:last(User), First)
    .



createtweets(UserCount, UserList, I) ->
    if I > UserCount -> 
        ok;
        true ->
            User = lists:nth(I, UserList),
            Tweets = getnmessages(round(UserCount * (1 / I)), UserList, []),
            sendusertweets(User, Tweets),
            createtweets(UserCount, UserList, I + 1)
    end
.

createqueries(_, 0) -> done;
createqueries(UserList, Count) ->
    U = fetchrndusername(UserList),
    search(lists:nth(rand:uniform(2), ["Mentions", "Hashtags"]), lists:last(U)),
    createqueries(UserList, Count - 1)
.

simulatetwitter(UserCount) ->
    {ok, Fd} = file:open("stats.txt", [append]),
    io:format(Fd, "No of users ~p~n", [UserCount]),
    statistics(runtime),
    UserList = createusers(UserCount, []),
    {_, Time1} = statistics(runtime),
    io:format(Fd, "Time for create users ~p~n", [Time1]),
    % io:format("~p~n", [UserList]),

    statistics(runtime),
    processsubscriptions(UserCount, UserList, 1),
    {_, Time2} = statistics(runtime),
    io:format(Fd, "Time for subscribe ~p~n", [Time2]),
    % fetchsubdetails(UserCount, UserList, 1),

    statistics(runtime),
    createtweets(UserCount, UserList, 1),

    {_, Time3} = statistics(runtime),

    io:format(Fd, "Time for sendtweets ~p~n", [Time3]),

    statistics(runtime),
    createqueries(UserList, UserCount * 2),
    {_, Time4} = statistics(runtime),
    io:format(Fd, "Time for queries ~p~n", [Time4])
.



initiatetweet(Tweet) ->
    processtweet(persistent_term:get(username), Tweet)
    .

subscribe_to_user(User) ->
    subscribe(persistent_term:get(username), User)
    .

intiateretweet(Username, Message_id) ->
    processretweet(persistent_term:get(username), Username, Message_id)
    .

registeruser(Username) -> 
    persistent_term:put(username, Username),
    join(Username)
.