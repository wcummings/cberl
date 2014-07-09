#include <string.h>
#include <stdio.h>
#include "cberl.h"
#include "cberl_nif.h"
#include "cb.h"

static ErlNifResourceType* cberl_handle = NULL;

static void cberl_handle_cleanup(ErlNifEnv* env, void* arg) {}

static int load(ErlNifEnv* env, void** priv, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    cberl_handle =  enif_open_resource_type(env, "cberl_nif",
                                                 "cberl_handle",
                                                 &cberl_handle_cleanup,
                                                 flags, 0);
    return 0;
}

NIF(cberl_nif_new)
{
    handle_t* handle = enif_alloc_resource(cberl_handle, sizeof(handle_t));
    handle->queue = queue_new();

    handle->calltable[CMD_CONNECT]         = cb_connect;
    handle->args_calltable[CMD_CONNECT]    = cb_connect_args;
    handle->calltable[CMD_STORE]           = cb_store;
    handle->args_calltable[CMD_STORE]      = cb_store_args;
    handle->calltable[CMD_MGET]            = cb_mget;
    handle->args_calltable[CMD_MGET]       = cb_mget_args;
    handle->calltable[CMD_UNLOCK]          = cb_unlock;
    handle->args_calltable[CMD_UNLOCK]     = cb_unlock_args;
    handle->calltable[CMD_MTOUCH]          = cb_mtouch;
    handle->args_calltable[CMD_MTOUCH]     = cb_mtouch_args;
    handle->calltable[CMD_ARITHMETIC]      = cb_arithmetic;
    handle->args_calltable[CMD_ARITHMETIC] = cb_arithmetic_args;
    handle->calltable[CMD_REMOVE]          = cb_remove;
    handle->args_calltable[CMD_REMOVE]     = cb_remove_args;
    handle->calltable[CMD_HTTP]            = cb_http;
    handle->args_calltable[CMD_HTTP]       = cb_http_args;

    #ifndef ERL_NIF_DIRTY_SCHEDULER_SUPPORT
    handle->thread_opts = enif_thread_opts_create("thread_opts");

    if (enif_thread_create("", &handle->thread, worker, handle, handle->thread_opts) != 0) {
        return enif_make_atom(env, "error");
    }
    #endif

    return enif_make_tuple2(env, enif_make_atom(env, "ok"), enif_make_resource(env, handle));
}

NIF(cberl_nif_dirty)
{
    task_t* task = get_task(env, argc, argv);

    if (task == NULL) {
        return enif_schedule_dirty_nif_finalizer(env, NULL, cberl_dirty_nif_badarg_finalizer);
    }

    ERL_NIF_TERM result = task->handle->calltable[task->cmd](env, task->handle, task->args);

    return enif_schedule_dirty_nif_finalizer(env, result, cberl_dirty_nif_finalizer);
}

#ifdef ERL_NIF_DIRTY_SCHEDULER_SUPPORT

NIF(cberl_nif_control)
{
    return enif_schedule_dirty_nif(env, ERL_NIF_DIRTY_JOB_IO_BOUND, cberl_nif_dirty, argc, argv);
}

#else

NIF(cberl_nif_control)
{
    task_t* task = get_task(env, argc, argv);
    if (task == NULL) {
        return enif_make_badarg(env);
    }

    queue_put(task->handle->queue, task);

    return A_OK(env);
}

#endif

NIF(cberl_nif_destroy) {
    handle_t * handle;
    void* resp;
    assert_badarg(enif_get_resource(env, argv[0], cberl_handle, (void **) &handle), env);      
    queue_put(handle->queue, NULL); // push NULL into our queue so the thread will join
    #ifndef ERL_NIF_DIRTY_SCHEDULER_SUPPORT
    enif_thread_join(handle->thread, &resp);
    #endif
    queue_destroy(handle->queue);
    #ifndef ERL_NIF_DIRTY_SCHEDULER_SUPPORT
    enif_thread_opts_destroy(handle->thread_opts);
    #endif
    lcb_destroy(handle->instance);
    enif_release_resource(handle); 
    return A_OK(env);
}


ERL_NIF_TERM cberl_dirty_nif_finalizer(ErlNifEnv* env, ERL_NIF_TERM result)
{
    return result;
}

ERL_NIF_TERM cberl_dirty_nif_badarg_finalizer(ErlNifEnv* env, ERL_NIF_TERM result) {
    return enif_make_badarg(env);
}

task_t* get_task(ErlNifEnv* env, int argc, const ERL_NIF_TERM argv[]) {
    handle_t* handle;

    if(!enif_get_resource(env, argv[0], cberl_handle, (void **) &handle)) {
        return NULL;
    }

    unsigned int len;
    enif_get_atom_length(env, argv[1], &len, ERL_NIF_LATIN1);
    int cmd;
    enif_get_int(env, argv[1], &cmd);

    if (cmd == -1) {
        return NULL;
    }

    ErlNifPid* pid = (ErlNifPid*)enif_alloc(sizeof(ErlNifPid));
    task_t* task = (task_t*)enif_alloc(sizeof(task_t));

    unsigned arg_length;
    if (!enif_get_list_length(env, argv[2], &arg_length)) {
        enif_free(pid);
        enif_free(task);
        return NULL;
    }

    ERL_NIF_TERM nargs = argv[2];
    ERL_NIF_TERM head, tail;
    ERL_NIF_TERM* new_argv = (ERL_NIF_TERM*)enif_alloc(sizeof(ERL_NIF_TERM) * arg_length);
    int i = 0;
    while (enif_get_list_cell(env, nargs, &head, &tail)) {
        new_argv[i] = head;
        i++;
        nargs = tail;
    }

    void* args = handle->args_calltable[cmd](env, argc, new_argv);

    enif_free(new_argv);

    if(args == NULL) {
        enif_free(pid);
        enif_free(task);
        return NULL;
    }

    enif_self(env, pid);

    task->pid  = pid;
    task->cmd  = cmd;
    task->args = args;
    task->handle = handle;

    return task;
}

static void* worker(void *obj)
{
    handle_t* handle = (handle_t*)obj;

    task_t* task;
    ErlNifEnv* env = enif_alloc_env();

    while ((task = (task_t*)queue_get(handle->queue)) != NULL) {
        ERL_NIF_TERM result = handle->calltable[task->cmd](env, handle, task->args);
        enif_send(NULL, task->pid, env, result);
        enif_free(task->pid);
        enif_free(task->args);
        enif_free(task);
        enif_clear_env(env);
    }

    return NULL;
}

static ErlNifFunc nif_funcs[] = {
    {"new", 0, cberl_nif_new},
    {"control", 3, cberl_nif_control},
    {"destroy", 1, cberl_nif_destroy}
};

ERL_NIF_INIT(cberl_nif, nif_funcs, load, NULL, NULL, NULL);
