-module(cberl_worker).
-behaviour(poolboy_worker).
-include("cberl.hrl").
-behaviour(gen_server).

%% API
-export([start_link/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link(Args) -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([{host, Host}, {username, Username}, {password, Password},
      {bucketname, BucketName}, {transcoder, Transcoder}]) ->
    process_flag(trap_exit, true),
    {ok, Handle} = cberl_nif:new(),
    DirtySchedulers = try erlang:system_info(dirty_cpu_schedulers) of
        _ -> true
    catch
        _ -> false
    end,
    State = #instance{handle = Handle, transcoder = Transcoder, supports_dirty_schedulers = DirtySchedulers},
    case control(op(connect), [Host, Username, Password, BucketName], State) of
        ok -> {ok, State};
        {error, Error} -> {stop, Error}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({mtouch, Keys, ExpTimesE}, _From, 
            State = #instance{handle = Handle}) ->
    case control(op(mtouch), [Keys, ExpTimesE], State) of
        Reply -> {reply, Reply, State}
    end;
handle_call({unlock, Key, Cas}, _From, 
            State = #instance{handle = Handle}) ->
    case control(op(unlock), [Key, Cas], State) of
        Reply -> {reply, Reply, State}
    end;
handle_call({store, Op, Key, Value, TranscoderOpts, Exp, Cas}, _From, 
            State = #instance{handle = Handle, transcoder = Transcoder}) ->
    StoreValue = Transcoder:encode_value(TranscoderOpts, Value), 
    case control(op(store), [operation_value(Op), Key, StoreValue, 
                 Transcoder:flag(TranscoderOpts), Exp, Cas], State) of
        Reply -> {reply, Reply, State}
    end;
handle_call({mget, Keys, Exp, Lock}, _From, 
            State = #instance{handle = Handle, transcoder = Transcoder}) ->
    Reply = case control(op(mget), [Keys, Exp, Lock], State) of
        {error, Error} -> {error, Error};
        {ok, Results} ->
            lists:map(fun(Result) ->
                        case Result of
                            {Cas, Flag, Key, Value} ->
                                DecodedValue = Transcoder:decode_value(Flag, Value),
                                {Key, Cas, DecodedValue};
                            {_Key, {error, _Error}} ->
                                Result
                        end
                end, Results)
    end,
    {reply, Reply, State};
handle_call({arithmetic, Key, OffSet, Exp, Create, Initial}, _From,
            State = #instance{handle = Handle, transcoder = Transcoder}) ->
    Reply = case control(op(arithmetic), [Key, OffSet, Exp, Create, Initial], State) of
        {error, Error} -> {error, Error};
        {ok, {Cas, Flag, Value}} ->
            DecodedValue = Transcoder:decode_value(Flag, Value),
            {ok, Cas, DecodedValue}
    end,
    {reply, Reply, State};
handle_call({remove, Key, N}, _From,
            State = #instance{handle = Handle}) ->
    case control(op(remove), [Key, N], State) of
        Reply -> {reply, Reply, State}
    end;
handle_call({http, Path, Body, ContentType, Method, Chunked}, _From,
            State = #instance{handle = Handle}) ->
    case control(op(http), [Path, Body, ContentType, Method, Chunked], State) of
        Reply -> {reply, Reply, State}
    end;
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State = #instance{handle = Handle}) ->
    cberl_nif:destroy(Handle),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

control(Op, Args, State = #instance{handle = Handle, supports_dirty_schedulers = true}) ->
    cberl_nif:control(Handle, Op, Args);
control(Op, Args, State = #instance{handle = Handle, supports_dirty_schedulers = false}) ->
    ok = cberl_nif:control(Handle, Op, Args),
    receive
        R -> R
    end.

-spec operation_value(operation_type()) -> integer().
operation_value(add) -> ?'CBE_ADD';
operation_value(replace) -> ?'CBE_REPLACE';
operation_value(set) -> ?'CBE_SET';
operation_value(append) -> ?'CBE_APPEND';
operation_value(prepend) -> ?'CBE_PREPEND'.

-spec op(atom()) -> integer().
op(connect) -> ?'CMD_CONNECT';
op(store) -> ?'CMD_STORE';
op(mget) -> ?'CMD_MGET';
op(unlock) -> ?'CMD_UNLOCK';
op(mtouch) -> ?'CMD_MTOUCH';
op(arithmetic) -> ?'CMD_ARITHMETIC';
op(remove) -> ?'CMD_REMOVE';
op(http) -> ?'CMD_HTTP'.
