-module(twitterengine).

-export([start/0, engine_start/1]).




register_user(Username,  Pid, State) -> 
    CurUsersDict = dict:fetch(users, State),
    UpdtUsers = dict:store(Username, Pid, CurUsersDict),
    UpdtState = dict:store(users, UpdtUsers, State),
    
    CurSubrDict = dict:fetch(subrdict, State),
    CurSubeDict = dict:fetch(subedict, State),
    CurSubrDict = dict:store(Username, [], CurSubrDict),
    CurSubeDict = dict:store(Username, [], CurSubeDict),
    Updtstate1 = dict:store(subrdict, CurSubrDict, UpdtState),
    Updtstate2 = dict:store(subedict, CurSubeDict, Updtstate1),


    UserSpecifiedTweets = dict:fetch(usertweets, State),
    UpdtTweetDict = dict:store(Username, [], UserSpecifiedTweets),
    Updtstate3 = dict:store(usertweets, UpdtTweetDict, Updtstate2),
    Updtstate3
.


subscribe(Subr, Sube, State) -> 
    CurSubrDict = dict:fetch(subrdict, State),
    CurSubeDict = dict:fetch(subedict, State),
    CurSubrDict = dict:append(Subr, Sube, CurSubrDict),
    CurSubeDict = dict:append(Sube, Subr, CurSubeDict),
    Updtstate1 = dict:store(subrdict, CurSubrDict, State),
    Updtstate2 = dict:store(subedict, CurSubeDict, Updtstate1),
    Updtstate2

.



posttweet_subr([], _, _,_) -> ok;
posttweet_subr(SubscriberList, Tweet, UsersDict, Username) ->
    [First | Rest] = SubscriberList,
    Pid = dict:fetch(First, UsersDict),
    Pid ! {tweet, Tweet, Username},
    posttweet_subr(Rest, Tweet, UsersDict, Username)
.

size([], Length) -> Length;
size(Elements, Length) ->
    [_ | Rest] = Elements,
    size(Rest, Length + 1).


send_tweet(Username, Tweet, State) -> 
    UserSpecifiedTweets = dict:fetch(usertweets, State),
    User = dict:fetch(Username, UserSpecifiedTweets),
    Pos =  size(User, 0) + 1,
    UserList = lists:append(User, [[Pos, Tweet]]),
    CurrentUserTweets = dict:store(Username, UserList, UserSpecifiedTweets),
    CurSubeDict = dict:fetch(subedict, State),
    CurrentUserSubscriptionList = dict:fetch(Username, CurSubeDict),

    UsersDict = dict:fetch(users, State),
    posttweet_subr(CurrentUserSubscriptionList, [Pos, Tweet], UsersDict, Username),
    UpdtState = dict:store(usertweets, CurrentUserTweets, State),
    UpdtState
.


search([], _) -> useridnotfound;
search(MessageList, Id) ->
    [First|Rest] = MessageList,
    % io:format("~p", [First]),
    case lists:nth(1, First) == Id of
        true -> lists:last(First);
        false -> search(Rest, Id)
    end.


processretweet(Username,Usermain, Message_id, State) ->
    Userlist = dict:fetch(usertweets , State),
    Tweetslist = dict:fetch(Usermain , Userlist),
    % io:format("~p", [Tweetslist]),
    Tweet = search(Tweetslist, Message_id),
    UpdtState = send_tweet(Username, Tweet, State),
    UpdtState
.

searchtweetswithword([], _, ResultsList) -> ResultsList;
searchtweetswithword(Tweets, Word, ResultsList) ->
    [First|Rest] = Tweets,
    % io:format("WOrd   ~p~p~p~n", [First, Word, string:str(lists:last(First), Word)]),
    case string:str(lists:last(First), Word)  > 0 of
        true -> searchtweetswithword(Rest, Word, lists:append(ResultsList, [First]));
        false -> searchtweetswithword(Rest, Word, ResultsList)
end.

searchusertweets([], _, _, ResultsDict) -> ResultsDict;
searchusertweets(Users, UserSpecifiedTweets, Word, ResultsDict) ->
    [First|Rest] = Users,
    Tweets = dict:fetch(First, UserSpecifiedTweets),
    TweetsWithWord = searchtweetswithword(Tweets, Word, []),
    % io:format("~p~n", [TweetsWithWord]),
    case size(TweetsWithWord, 0) > 0 of
        true -> searchusertweets(Rest, UserSpecifiedTweets, Word, dict:store(First, TweetsWithWord, ResultsDict));
        false -> searchusertweets(Rest, UserSpecifiedTweets, Word, ResultsDict)
end.


fetchtweetwithstring(UserSpecifiedTweets, Word) ->
    Users = dict:fetch_keys(UserSpecifiedTweets),
    MJ = searchusertweets(Users, UserSpecifiedTweets, Word, dict:new()),
    MJ
.




query_tweet(Filter, Qury, Pid, State) -> 
    if Filter == "Mentions" ->
            Pid ! {result, fetchtweetwithstring(dict:fetch(usertweets, State), "@"++Qury)};
        true -> notcorrectfilter
    end,
    if Filter == "Hashtags" ->
            Pid ! {result, fetchtweetwithstring(dict:fetch(usertweets, State), "#"++Qury)};
        true -> notcorrectfilter
    end
    .


disconnect_user(Username, State) -> 
    CurUsersDict = dict:fetch(users, State),
    Pid = dict:fetch(Username, CurUsersDict),
    Pid ! {disconnectuser},
    dict:store(users, dict:erase(Username, CurUsersDict), State)
.

engine_start(State) ->
    receive
        {registeruser, Username, Pid} -> 
            UpdtState = register_user(Username, Pid, State),
            engine_start(UpdtState);
        {fetchlistofusers, Pid} -> 
            Pid ! {userlist, dict:fetch(users, State)},
            engine_start(State); 
        {subscribe, Subr, Sube} -> 
            UpdtState = subscribe(Subr, Sube, State),
            engine_start(UpdtState);
        {fetchsubr, Username, Pid} ->
            CurSubrDict = dict:fetch(subrdict, State),
            Pid ! {subr, dict:fetch(Username, CurSubrDict)},
            engine_start(State);
        {posttweet, Username, Message} -> 
            UpdtState = send_tweet(Username, Message, State),
            engine_start(UpdtState);
        {processtweetquery, Filter, Qury, Pid} -> 
            query_tweet(Filter, Qury, Pid, State),
            engine_start(State);
        {processretweet, Username, Usermain ,Message_id} -> 
            UpdtState = processretweet(Username,Usermain, Message_id, State),
            engine_start(UpdtState);
        {disconnectuser, Username} -> 
            UpdtState = disconnect_user(Username, State),
            engine_start(UpdtState)
    end
.

start() ->
    State = dict:from_list([{users, dict:new()}, {subrdict,dict:new()}, {subedict, dict:new()}, {usertweets, dict:new()}]),
    register(twitterengine, spawn(twitterengine, engine_start, [State]))
.