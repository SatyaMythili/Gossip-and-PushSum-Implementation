%%%-------------------------------------------------------------------
%%% @author SatyaMythiliNuthalapati&SaiKrishnaNikhithaMikkilineni
%%% @copyright (C) 2022, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. Oct 2022 10:28 PM
%%%-------------------------------------------------------------------
-module(converge).
-author("SatyaMythiliNuthalapati&SaiKrishnaNikhithaMikkilineni").

-import(math, []).

%% API
-export[start_converge/0, startActors_converge/1, startActorsPushSum_converge/2, startGossip_converge/2, sendGossip_converge/5].

start_converge() ->

{ok, [NoOfNodes]} = io:fread("\nEnter the no of nodes: ", "~d\n"),
{ok, [TopologyType]} = io:fread("\nEnter the type of Topology: ", "~s\n"),
{ok, [Algorithm]} = io:fread("\nEnter the Algorithm (pushsum/gossip): ", "~s\n"),

if
TopologyType == "2D" ->
NumberOfNodes = getNextSquare(NoOfNodes);
TopologyType == "imp3D" ->
NumberOfNodes = getNextSquare(NoOfNodes);
true ->
NumberOfNodes = NoOfNodes
end,

io:format("Number of Nodes: ~p\n", [NumberOfNodes]),
io:format("Type of Topology: ~p\n", [TopologyType]),
io:format("Algorithm: ~p\n", [Algorithm]),

case Algorithm of
"gossip" -> startGossip_converge(NumberOfNodes, TopologyType);
"pushsum" -> startPushSum_converge(NumberOfNodes, TopologyType)
end.

startGossip_converge(NumberOfNodes, TopologyType) ->
  io:format('Gossip Algorithm Starts \n'),
  ActorsList = createActors_converge(NumberOfNodes),
  {SelectedActor, SelectedActor_PID} = lists:nth(rand:uniform(length(ActorsList)), ActorsList),
  io:format("\nThe Actor Selected by the Main Process is : ~p \n\n", [SelectedActor]),

  %start_time
  Starting_Time = erlang:system_time(millisecond),
  SelectedActor_PID ! {self(), {TopologyType, ActorsList, NumberOfNodes}},
  checkAliveActors_converge(ActorsList),
  %End_time
  End_Time = erlang:system_time(millisecond),
  io:format("\nTime Taken in milliseconds: ~p\n", [End_Time - Starting_Time]).

checkAliveActors_converge(ActorsList) ->
  Alive_Actors_List = [{A, A_PID} || {A, A_PID} <- ActorsList, is_process_alive(A_PID) == true],

  if
    Alive_Actors_List == [] ->
      io:format("\nCONVERGED: All Processes received the rumor 10 times.\n");
    true ->
      checkAliveActors_converge(ActorsList)
  end.

getAliveActorsList(ActorsList) ->
  Alive_Actors_List = [{A, A_PID} || {A, A_PID} <- ActorsList, is_process_alive(A_PID) == true],
  Alive_Actors_List.

startPushSum_converge(NumberOfNodes, TopologyType) ->

  io:format('Starting the Push Sum Algorithm \n'),

  W = 1,
  ActorsList = createActorsPushSum_converge(NumberOfNodes, W),
  {SelectedActor, SelectedActor_PID} = lists:nth(rand:uniform(length(ActorsList)), ActorsList),
  io:format("\nThe Selected actor is : ~p \n", [SelectedActor]),

  %start time
  Starting_Time = erlang:system_time(millisecond),

  SelectedActor_PID ! {self(), {0, 0, TopologyType, ActorsList, NumberOfNodes, self()}},
  receive
    {_, {ok, Child_Id, Child_Count}} ->
      io:format("\nCONVERGED ---> Process ~p has converged with ~p subsequent very small changes on its ratio.\n", [Child_Id, Child_Count])
  end,
  %End time
  End_Time = erlang:system_time(millisecond),
  io:format("\nTime Taken in milliseconds: ~p\n", [End_Time - Starting_Time]).

createTopology(TopologyType, ActorsList, NumberOfNodes, Id) ->
  ActorsMap = maps:from_list(ActorsList),
  case TopologyType of
    "full" -> getFullNetworkNeighbours(Id, NumberOfNodes, ActorsMap);
    "2D" -> get2DGridNeighbours(Id, NumberOfNodes, ActorsMap);
    "line" -> getLineGridNeighbours(Id, NumberOfNodes, ActorsMap);
    "imp3D" -> getImperfect3DGridNeighbours(Id, NumberOfNodes, ActorsMap)
  end.

getFullNetworkNeighbours(Id, N, ActorsMap) ->
  Neighbours_List = lists:subtract(lists:seq(1, N), [Id]),
  Detailed_List_Neighbours = [
    {N, maps:get(N, ActorsMap)}
    || N <- Neighbours_List, maps:is_key(N, ActorsMap)
  ],
  Detailed_List_Neighbours.

get2DGridNeighbours(Id, N, ActorsMap) ->


  GetRows = erlang:trunc(math:sqrt(N)),
  ModValue = Id rem GetRows,

  if
    ModValue == 1 ->
      Neighbours_List = [Id+1];
    ModValue == 0 ->
      Neighbours_List = [Id-1];
    true ->
      Neighbours_List = lists:append([[Id-1], [Id+1]])
  end,

  if
    Id+GetRows > N ->
      Neighbours_2 = Neighbours_List;
    true ->
      Neighbours_2 = lists:append([Neighbours_List, [Id+GetRows]])
  end,
  if
    Id-GetRows < 1 ->
      Neighbours_3 = Neighbours_2;
    true ->
      Neighbours_3 = lists:append([Neighbours_2, [Id-GetRows]])
  end,

  Detailed_List_Neighbours = [
    {N, maps:get(N, ActorsMap)}
    || N <- Neighbours_3, maps:is_key(N, ActorsMap)
  ],
  Detailed_List_Neighbours.

getLineGridNeighbours(Id, N, ActorsMap) ->

  if
    Id > N ->
      Neighbours_List = [];
    Id < 1 ->
      Neighbours_List = [];
    Id + 1 > N ->
      if
        Id - 1 < 1 ->
          Neighbours_List = [];
        true ->
          Neighbours_List = [Id-1]
      end;
    true ->
      if
        Id - 1 < 1 ->
          Neighbours_List = [Id+1];
        true ->
          Neighbours_List = [Id-1, Id+1]
      end
  end,
  Detailed_List_Neighbours = [
    {N, maps:get(N, ActorsMap)}
    || N <- Neighbours_List, maps:is_key(N, ActorsMap)
  ],
  Detailed_List_Neighbours.

getImperfect3DGridNeighbours(Id, N, ActorsMap) ->

  GetRows = erlang:trunc(math:sqrt(N)),
  ModValue = Id rem GetRows,

  if
    ModValue == 1 ->
      Neighbours_List = [Id+1];
    ModValue == 0 ->
      Neighbours_List = [Id-1];
    true ->
      Neighbours_List = lists:append([[Id-1], [Id+1]])
  end,

  if
    Id+GetRows > N ->
      Neighbours_2 = Neighbours_List;
    true ->
      if
        ModValue == 1 ->
          Neighbours_2 = lists:append([Neighbours_List, [Id+GetRows], [Id+GetRows+1]]);
        ModValue == 0 ->
          Neighbours_2 = lists:append([Neighbours_List, [Id+GetRows], [Id+GetRows-1]]);
        true ->
          Neighbours_2 = lists:append([Neighbours_List, [Id+GetRows], [Id+GetRows-1], [Id+GetRows+1]])
      end
  end,
  if
    Id-GetRows < 1 ->
      NextImmediateNeighbours = Neighbours_2;
    true ->
      if
        ModValue == 1 ->
          NextImmediateNeighbours = lists:append([Neighbours_2, [Id-GetRows], [Id-GetRows+1]]);
        ModValue == 0 ->
          NextImmediateNeighbours = lists:append([Neighbours_2, [Id-GetRows], [Id-GetRows-1]]);
        true ->
          NextImmediateNeighbours = lists:append([Neighbours_2, [Id-GetRows], [Id-GetRows-1], [Id-GetRows+1]])
      end
  end,

  NeighboursToIgnore = lists:append([NextImmediateNeighbours, [Id]]),
  LeftOverNeighbours = lists:subtract(lists:seq(1, N), NeighboursToIgnore),

  RandomRemaningNeighbour = lists:nth(rand:uniform(length(LeftOverNeighbours)), LeftOverNeighbours),
  %RandomImmediateNeighbour = lists:nth(rand:uniform(length(NextImmediateNeighbours)), NextImmediateNeighbours),

  Neighbours_List_Final = lists:append([[RandomRemaningNeighbour], NextImmediateNeighbours]),

  Detailed_List_Neighbours = [
    {N, maps:get(N, ActorsMap)}
    || N <- Neighbours_List_Final, maps:is_key(N, ActorsMap)
  ],
  Detailed_List_Neighbours.

startActors_converge(Id) ->
  gossipResponse_Await(Id, 0).

gossipResponse_Await(Id, Count) ->
  receive
    {From, {TopologyType, ActorsList, NumberOfNodes}} ->
      if
        Count == 10 ->
          %io:format("CONVERGED: in Process: ~p || Count: ~p\n", [Id, Count]);
          exit(0);
        true ->
          spawn(converge, sendGossip_converge, [self(), TopologyType, ActorsList, NumberOfNodes, Id]),
          gossipResponse_Await(Id, Count+1)
      end
  end.

sendGossip_converge(Current, TopologyType, ActorsList, NumberOfNodes, Id) ->
  Status = is_process_alive(Current),
  if
    Status == true ->
      Alive_Actors_List = getAliveActorsList(ActorsList),

      Neighbours_List = createTopology(TopologyType, Alive_Actors_List, NumberOfNodes, Id),
      if
        Neighbours_List == [] ->
          exit(0);
        true ->
          {_, SelectedNeighbour_PID} = lists:nth(rand:uniform(length(Neighbours_List)), Neighbours_List),
          SelectedNeighbour_PID ! {Current, {TopologyType, ActorsList, NumberOfNodes}},
          sendGossip_converge(Current, TopologyType, ActorsList, NumberOfNodes, Id)
      end;
    true ->
      exit(0)
  end.


createActors_converge(N) ->

  ActorsList = [  % { {Pid, Ref}, Id }
    {Id, spawn(converge, startActors_converge, [Id])}
    || Id <- lists:seq(1, N)
  ],

  ActorsList.

createActorsPushSum_converge(N, W) ->
  ActorsList = [  % { {Pid, Ref}, Id }
    {Id, spawn(converge, startActorsPushSum_converge, [Id, W])}
    || Id <- lists:seq(1, N)
  ],
  ActorsList.

startActorsPushSum_converge(Id, W) ->
  %io:fwrite("I am an actor with Id : ~w\n", [Id]),
  responsePushSum_Await(Id, Id, W, 0, 0).

responsePushSum_Await(Id, S, W, Prev_ratio, Count) ->
  receive
    {_, {S1, W1, TopologyType, ActorsList, NumberOfNodes, Main}} ->

      if
        Count == 3 ->
          Main ! {self(), {ok, Id, Count}};
        true ->

          S2 = S + S1,
          W2 = W + W1,

          Alive_Actors_List = getAliveActorsList(ActorsList),
          Neighbours_List = createTopology(TopologyType, Alive_Actors_List, NumberOfNodes, Id),

          if
            Neighbours_List == [] ->
              exit(0);
            true ->
              {_, SelectedNeighbour_PID} = lists:nth(rand:uniform(length(Neighbours_List)), Neighbours_List),


              S3 = S2 / 2,
              W3 = W2 / 2,

              SelectedNeighbour_PID ! {self(), {S3, W3, TopologyType, ActorsList, NumberOfNodes, Main}},

              Curr_ratio = S / W,
              Difference = math:pow(10, -10),
              if
                abs(Curr_ratio - Prev_ratio) < Difference ->
                  %io:format("\nPrevious Ratio: ~p & Current Ratio ~p & Difference is ~p\n",[Prev_ratio, Curr_ratio, abs(Curr_ratio - Prev_ratio)]),
                  responsePushSum_Await(Id, S3, W3, Curr_ratio, Count + 1);
                true ->
                  responsePushSum_Await(Id, S3, W3, Curr_ratio, 0)
              end
          end
      end
  end.

getNextSquare(NumberOfNodes) ->
  NearestSquare =  round(math:pow(math:ceil(math:sqrt(NumberOfNodes)),2)),
  NearestSquare.
