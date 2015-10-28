/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Filipe Manana  <filipe@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include "mapreduce.h"
#include "mapreduce_internal.h"
#include <iostream>
#include <cstring>
#include <stdlib.h>
#include <v8.h>
#include <libplatform/libplatform.h>

#undef V8_POST_3_19_API
#undef V8_PRE_3_19_API

using namespace v8;

typedef struct {
    Persistent<Object>    jsonObject;
    Persistent<Function>  jsonParseFun;
    Persistent<Function>  stringifyFun;
    mapreduce_ctx_t       *ctx;
} isolate_data_t;


static const char *SUM_FUNCTION_STRING =
    "(function(values) {"
    "    var sum = 0;"
    "    for (var i = 0; i < values.length; ++i) {"
    "        sum += values[i];"
    "    }"
    "    return sum;"
    "})";

static const char *DATE_FUNCTION_STRING =
    // I wish it was on the prototype, but that will require bigger
    // C changes as adding to the date prototype should be done on
    // process launch. The code you see here may be faster, but it
    // is less JavaScripty.
    // "Date.prototype.toArray = (function() {"
    "(function(date) {"
    "    date = date.getUTCDate ? date : new Date(date);"
    "    return isFinite(date.valueOf()) ?"
    "      [date.getUTCFullYear(),"
    "      (date.getUTCMonth() + 1),"
    "       date.getUTCDate(),"
    "       date.getUTCHours(),"
    "       date.getUTCMinutes(),"
    "       date.getUTCSeconds()] : null;"
    "})";

static const char *BASE64_FUNCTION_STRING =
    "(function(b64) {"
    "    var i, j, l, tmp, scratch, arr = [];"
    "    var lookup = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';"
    "    if (typeof b64 !== 'string') {"
    "        throw 'Input is not a string';"
    "    }"
    "    if (b64.length % 4 > 0) {"
    "        throw 'Invalid base64 source.';"
    "    }"
    "    scratch = b64.indexOf('=');"
    "    scratch = scratch > 0 ? b64.length - scratch : 0;"
    "    l = scratch > 0 ? b64.length - 4 : b64.length;"
    "    for (i = 0, j = 0; i < l; i += 4, j += 3) {"
    "        tmp = (lookup.indexOf(b64[i]) << 18) | (lookup.indexOf(b64[i + 1]) << 12);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) << 6) | lookup.indexOf(b64[i + 3]);"
    "        arr.push((tmp & 0xFF0000) >> 16);"
    "        arr.push((tmp & 0xFF00) >> 8);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    if (scratch === 2) {"
    "        tmp = (lookup.indexOf(b64[i]) << 2) | (lookup.indexOf(b64[i + 1]) >> 4);"
    "        arr.push(tmp & 0xFF);"
    "    } else if (scratch === 1) {"
    "        tmp = (lookup.indexOf(b64[i]) << 10) | (lookup.indexOf(b64[i + 1]) << 4);"
    "        tmp |= (lookup.indexOf(b64[i + 2]) >> 2);"
    "        arr.push((tmp >> 8) & 0xFF);"
    "        arr.push(tmp & 0xFF);"
    "    }"
    "    return arr;"
    "})";



#ifdef V8_POST_3_19_API
static Local<Context> createJsContext();
static void emit(const v8::FunctionCallbackInfo<Value> &args);
#elif V8_VER_4_8_API
static Local<Context> createJsContext();
static void emit(const v8::FunctionCallbackInfo<Value> &args);
#elif V8_PRE_3_19_API
static Persistent<Context> createJsContext();
static Handle<Value> emit(const Arguments &args);
#endif

static void doInitContext(mapreduce_ctx_t *ctx);
static Handle<Function> compileFunction(const std::string &function, Local<Context> jsContext);
static std::string exceptionString(const TryCatch &tryCatch);
static void loadFunctions(mapreduce_ctx_t *ctx,
                          const std::list<std::string> &function_sources);
static inline isolate_data_t *getIsolateData();
static inline mapreduce_json_t jsonStringify(const Handle<Value> &obj);
static inline Handle<Value> jsonParse(const mapreduce_json_t &thing);
static inline void taskStarted(mapreduce_ctx_t *ctx);
static inline void taskFinished(mapreduce_ctx_t *ctx);
static void freeKvListEntries(kv_list_int_t &kvs);
static void freeJsonListEntries(json_results_list_t &list);
static inline Handle<Array> jsonListToJsArray(const mapreduce_json_list_t &list);


void initContext(mapreduce_ctx_t *ctx,
                 const std::list<std::string> &function_sources)
{
    doInitContext(ctx);

    try {
        Locker locker(ctx->isolate);
#ifdef V8_POST_3_19_API
        Isolate::Scope isolateScope(ctx->isolate);
        HandleScope handleScope(ctx->isolate);
        Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
        Isolate::Scope isolateScope(ctx->isolate);
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);
#elif  V8_VER_4_8_API
        Isolate::Scope isolate_scope(ctx->isolate);
        HandleScope handle_scope(ctx->isolate);
        v8::Local<v8::Context> jsContext =
            v8::Local<v8::Context>::New(ctx->isolate, ctx->jsContext);
        Context::Scope context_scope(jsContext);
#endif

        loadFunctions(ctx, function_sources);
    } catch (...) {
        destroyContext(ctx);
        throw;
    }
}


void destroyContext(mapreduce_ctx_t *ctx)
{
    {
        Locker locker(ctx->isolate);
        Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
        HandleScope handleScope(ctx->isolate);
        Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
        HandleScope handleScope;
        Context::Scope contextScope(ctx->jsContext);
#elif  V8_VER_4_8_API
        HandleScope handle_scope(ctx->isolate);
        v8::Local<v8::Context> jsContext =
            v8::Local<v8::Context>::New(ctx->isolate, ctx->jsContext);
        Context::Scope context_scope(jsContext);
#endif

        for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
#ifdef V8_POST_3_19_API
            (*ctx->functions)[i]->Dispose();
            delete (*ctx->functions)[i];
#elif  V8_PRE_3_19_API
            (*ctx->functions)[i].Dispose();
#elif  V8_VER_4_8_API
            (*ctx->functions)[i]->Reset();
            delete (*ctx->functions)[i];
#endif

        }
        delete ctx->functions;

        isolate_data_t *isoData = getIsolateData();
#ifdef  V8_VER_4_8_API
	isoData->jsonObject.Reset();
        isoData->jsonObject.Empty();
        isoData->jsonParseFun.Reset();
        isoData->jsonParseFun.Empty();
        isoData->stringifyFun.Reset();
        isoData->stringifyFun.Empty();
#else
        isoData->jsonObject.Dispose();
        isoData->jsonObject.Clear();
        isoData->jsonParseFun.Dispose();
        isoData->jsonParseFun.Clear();
        isoData->stringifyFun.Dispose();
        isoData->stringifyFun.Clear();
#endif
        delete isoData;

#ifdef V8_VER_4_8_API
        ctx->jsContext.Reset();
#elif
        ctx->jsContext.Dispose();
        ctx->jsContext.Clear();
#endif
    }

    // XXX vmx 2015-10-28: somehow this call leads to a segfault
    //ctx->isolate->Dispose();

#ifdef V8_VER_4_8_API
    V8::Dispose();
    V8::ShutdownPlatform();
#endif
}

#ifdef V8_VER_4_8_API
class MapReduceBufferAllocator : public ArrayBuffer::Allocator {
 public:
  virtual void* Allocate(size_t length) {
    void* data = AllocateUninitialized(length);
    return data == NULL ? data : memset(data, 0, length);
  }
  virtual void* AllocateUninitialized(size_t length) { return malloc(length); }
  virtual void Free(void* data, size_t) { free(data); }
};
#endif

static void doInitContext(mapreduce_ctx_t *ctx)
{
V8::InitializeICU();
  //V8::InitializeExternalStartupData("mapreduce");
 Platform* platform = platform::CreateDefaultPlatform();
 V8::InitializePlatform(platform);
   V8::Initialize();

#ifdef V8_VER_4_8_API
    MapReduceBufferAllocator mapReduceBufferAllocator;
    Isolate::CreateParams createParams;
    createParams.array_buffer_allocator = &mapReduceBufferAllocator;
    ctx->isolate = Isolate::New(createParams);
#else
    ctx->isolate = Isolate::New();
#endif
    Locker locker(ctx->isolate);

#ifdef V8_POST_3_19_API
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope(ctx->isolate);
    ctx->jsContext.Reset(ctx->isolate, createJsContext());
    Local<Context> context = Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope contextScope(context);
    Handle<Object> jsonObject = Local<Object>::Cast(context->Global()->Get(String::New("JSON")));
#elif  V8_PRE_3_19_API
    Isolate::Scope isolateScope(ctx->isolate);
    HandleScope handleScope;
    ctx->jsContext = createJsContext();
    Context::Scope contextScope(ctx->jsContext);
    Handle<Object> jsonObject = Local<Object>::Cast(ctx->jsContext->Global()->Get(String::New("JSON")));
#elif  V8_VER_4_8_API
    Isolate::Scope isolate_scope(ctx->isolate);
    HandleScope handle_scope(ctx->isolate);
    //Local<Context> jsContext = createJsContext();
    ctx->jsContext.Reset(ctx->isolate, createJsContext());
    v8::Local<v8::Context> jsContext =
            v8::Local<v8::Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(jsContext);
    Local<String> jsonString = String::NewFromUtf8(ctx->isolate, "JSON", NewStringType::kNormal).ToLocalChecked();
    Handle<Object> jsonObject = Local<Object>::Cast(jsContext->Global()->Get(jsonString));
#endif

#ifdef V8_VER_4_8_API
    Local<String> parseFunString = String::NewFromUtf8(ctx->isolate, "parse", NewStringType::kNormal).ToLocalChecked();
    Handle<Function> parseFun = Local<Function>::Cast(jsonObject->Get(parseFunString));

    Local<String> stringifyFunString = String::NewFromUtf8(ctx->isolate, "stringify", NewStringType::kNormal).ToLocalChecked();
    Handle<Function> stringifyFun = Local<Function>::Cast(jsonObject->Get( stringifyFunString));
#else
    Handle<Function> parseFun = Local<Function>::Cast(jsonObject->Get(String::New("parse")));
    Handle<Function> stringifyFun = Local<Function>::Cast(jsonObject->Get(String::New("stringify")));
#endif

    isolate_data_t *isoData = new isolate_data_t();
#ifdef V8_PRE_3_19_API
    isoData->jsonObject = Persistent<Object>::New(jsonObject);
    isoData->jsonParseFun = Persistent<Function>::New(parseFun);
    isoData->stringifyFun = Persistent<Function>::New(stringifyFun);
#else
    isoData->jsonObject.Reset(ctx->isolate, jsonObject);
    isoData->jsonParseFun.Reset(ctx->isolate, parseFun);
    isoData->stringifyFun.Reset(ctx->isolate, stringifyFun);
#endif
    isoData->ctx = ctx;

#ifdef V8_VER_4_8_API
    ctx->isolate->SetData(0, (void *)isoData);
#else
    ctx->isolate->SetData(isoData);
#endif
    ctx->taskStartTime = -1;
}


#ifdef V8_POST_3_19_API
static Local<Context> createJsContext()
{
    HandleScope handleScope(Isolate::GetCurrent());
#elif  V8_PRE_3_19_API
static Persistent<Context> createJsContext()
{
    HandleScope handleScope;
#elif  V8_VER_4_8_API
static Local<Context> createJsContext()
{
    Isolate *isolate = Isolate::GetCurrent();
    HandleScope handleScope(isolate);
#endif

    Handle<ObjectTemplate> global = ObjectTemplate::New();
#ifdef V8_VER_4_8_API
    global->Set(String::NewFromUtf8(isolate, "emit", NewStringType::kNormal).ToLocalChecked(), FunctionTemplate::New(isolate, emit));
#else
    global->Set(String::New("emit"), FunctionTemplate::New(emit));
#endif

#ifdef V8_POST_3_19_API
    Handle<Context> context = Context::New(Isolate::GetCurrent(), NULL, global);
    Context::Scope contexScope(context);
#elif  V8_PRE_3_19_API
    Persistent<Context> context = Context::New(NULL, global);
    Context::Scope contexScope(context);
#elif  V8_VER_4_8_API
    Local<Context> context = Context::New(isolate, NULL, global);
    Context::Scope context_scope(context);
#endif

    Handle<Function> sumFun = compileFunction(SUM_FUNCTION_STRING, context);
    Handle<Function> decodeBase64Fun = compileFunction(BASE64_FUNCTION_STRING, context);
    Handle<Function> dateToArrayFun = compileFunction(DATE_FUNCTION_STRING, context);
#ifdef V8_VER_4_8_API
    context->Global()->Set(String::NewFromUtf8(isolate, "sum", NewStringType::kNormal).ToLocalChecked(), sumFun);
    context->Global()->Set(String::NewFromUtf8(isolate, "decodeBase64", NewStringType::kNormal).ToLocalChecked(), decodeBase64Fun);
    context->Global()->Set(String::NewFromUtf8(isolate, "dateToArray", NewStringType::kNormal).ToLocalChecked(), dateToArrayFun);
#else
    context->Global()->Set(String::New("sum"), sumFun);
    context->Global()->Set(String::New("decodeBase64"), decodeBase64Fun);
    context->Global()->Set(String::New("dateToArray"), dateToArrayFun);
#endif

#ifdef V8_POST_3_19_API
    return handle_scope.Close(context);
#elif  V8_PRE_3_19_API
    return context;
#elif  V8_VER_4_8_API
    //return handle_scope.Escape(context);
    return context;
#endif
}


void mapDoc(mapreduce_ctx_t *ctx,
            const mapreduce_json_t &doc,
            const mapreduce_json_t &meta,
            mapreduce_map_result_list_t *results)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
    Context::Scope context_scope(ctx->jsContext);
#elif  V8_VER_4_8_API
    HandleScope handle_scope(ctx->isolate);
    Local<Context> jsContext =
        Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(jsContext);
#endif
    Handle<Value> docObject = jsonParse(doc);
    Handle<Value> metaObject = jsonParse(meta);

    if (!metaObject->IsObject()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "metadata is not a JSON object");
    }

    Handle<Value> funArgs[] = { docObject, metaObject };

    taskStarted(ctx);
    kv_list_int_t kvs;
    ctx->kvs = &kvs;

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
        mapreduce_map_result_t mapResult;
#ifndef V8_PRE_3_19_API
        Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
#else
        Handle<Function> fun = (*ctx->functions)[i];
#endif
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 2, funArgs);

        if (!result.IsEmpty()) {
            mapResult.error = MAPREDUCE_SUCCESS;
            mapResult.result.kvs.length = kvs.size();
            size_t sz = sizeof(mapreduce_kv_t) * mapResult.result.kvs.length;
            mapResult.result.kvs.kvs = (mapreduce_kv_t *) malloc(sz);
            if (mapResult.result.kvs.kvs == NULL) {
                freeKvListEntries(kvs);
                throw std::bad_alloc();
            }
            kv_list_int_t::iterator it = kvs.begin();
            for (int j = 0; it != kvs.end(); ++it, ++j) {
                mapResult.result.kvs.kvs[j] = *it;
            }
        } else {
            freeKvListEntries(kvs);

            if (!trycatch.CanContinue()) {
                throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
            }

            mapResult.error = MAPREDUCE_RUNTIME_ERROR;
            std::string exceptString = exceptionString(trycatch);
            size_t len = exceptString.length();

            mapResult.result.error_msg = (char *) malloc(len + 1);
            if (mapResult.result.error_msg == NULL) {
                throw std::bad_alloc();
            }
            memcpy(mapResult.result.error_msg, exceptString.data(), len);
            mapResult.result.error_msg[len] = '\0';
        }

        results->list[i] = mapResult;
        results->length += 1;
        kvs.clear();
    }

    taskFinished(ctx);
}


json_results_list_t runReduce(mapreduce_ctx_t *ctx,
                              const mapreduce_json_list_t &keys,
                              const mapreduce_json_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#elif  V8_VER_4_8_API
    HandleScope handle_scope(ctx->isolate);
    Local<Context> jsContext =
        Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(jsContext);
#endif
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
    json_results_list_t results;

#ifdef V8_VER_4_8_API
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(ctx->isolate, false) };
#else
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };
#endif

    taskStarted(ctx);

    for (unsigned int i = 0; i < ctx->functions->size(); ++i) {
#ifndef V8_PRE_3_19_API
        Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[i]);
#else
        Handle<Function> fun = (*ctx->functions)[i];
#endif
        TryCatch trycatch;
        Handle<Value> result = fun->Call(fun, 3, args);

        if (result.IsEmpty()) {
            freeJsonListEntries(results);

            if (!trycatch.CanContinue()) {
                throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
            }

            throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
        }

        try {
            mapreduce_json_t jsonResult = jsonStringify(result);
            results.push_back(jsonResult);
        } catch(...) {
            freeJsonListEntries(results);
            throw;
        }
    }

    taskFinished(ctx);

    return results;
}


mapreduce_json_t runReduce(mapreduce_ctx_t *ctx,
                           int reduceFunNum,
                           const mapreduce_json_list_t &keys,
                           const mapreduce_json_list_t &values)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#elif  V8_VER_4_8_API
    HandleScope handle_scope(ctx->isolate);
    Local<Context> jsContext =
        Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(jsContext);
#endif

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "invalid reduce function number");
    }

#ifndef V8_PRE_3_19_API
    Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
#else
    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
#endif
    Handle<Array> keysArray = jsonListToJsArray(keys);
    Handle<Array> valuesArray = jsonListToJsArray(values);
#ifdef  V8_VER_4_8_API
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(ctx->isolate, false) };
#else
    Handle<Value> args[] = { keysArray, valuesArray, Boolean::New(false) };
#endif

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
        }

        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return jsonStringify(result);
}


mapreduce_json_t runRereduce(mapreduce_ctx_t *ctx,
                             int reduceFunNum,
                             const mapreduce_json_list_t &reductions)
{
    Locker locker(ctx->isolate);
    Isolate::Scope isolateScope(ctx->isolate);
#ifdef V8_POST_3_19_API
    HandleScope handleScope(ctx->isolate);
    Context::Scope contextScope(ctx->isolate, ctx->jsContext);
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
    Context::Scope contextScope(ctx->jsContext);
#elif  V8_VER_4_8_API
    HandleScope handle_scope(ctx->isolate);
    Local<Context> jsContext =
        Local<Context>::New(ctx->isolate, ctx->jsContext);
    Context::Scope context_scope(jsContext);
#endif

    reduceFunNum -= 1;
    if (reduceFunNum < 0 ||
        static_cast<unsigned int>(reduceFunNum) >= ctx->functions->size()) {
        throw MapReduceError(MAPREDUCE_INVALID_ARG, "invalid reduce function number");
    }

#ifndef V8_PRE_3_19_API
    Local<Function> fun = Local<Function>::New(ctx->isolate, *(*ctx->functions)[reduceFunNum]);
#elif  V8_PRE_3_19_API
    Handle<Function> fun = (*ctx->functions)[reduceFunNum];
#endif
    Handle<Array> valuesArray = jsonListToJsArray(reductions);
#ifdef V8_VER_4_8_API
    Handle<Value> args[] = { Null(ctx->isolate), valuesArray, Boolean::New(ctx->isolate, true) };
#else
    Handle<Value> args[] = { Null(), valuesArray, Boolean::New(true) };
#endif

    taskStarted(ctx);

    TryCatch trycatch;
    Handle<Value> result = fun->Call(fun, 3, args);

    taskFinished(ctx);

    if (result.IsEmpty()) {
        if (!trycatch.CanContinue()) {
            throw MapReduceError(MAPREDUCE_TIMEOUT, "timeout");
        }

        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return jsonStringify(result);
}


void terminateTask(mapreduce_ctx_t *ctx)
{
    V8::TerminateExecution(ctx->isolate);
    taskFinished(ctx);
}


static void freeKvListEntries(kv_list_int_t &kvs)
{
    kv_list_int_t::iterator it = kvs.begin();

    for ( ; it != kvs.end(); ++it) {
        mapreduce_kv_t kv = *it;
        free(kv.key.json);
        free(kv.value.json);
    }
    kvs.clear();
}


static void freeJsonListEntries(json_results_list_t &list)
{
    json_results_list_t::iterator it = list.begin();

    for ( ; it != list.end(); ++it) {
        free((*it).json);
    }
    list.clear();
}


static Handle<Function> compileFunction(const std::string &funSource, Local<Context> jsContext)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(Isolate::GetCurrent());
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
#elif  V8_VER_4_8_API
    Isolate *isolate = Isolate::GetCurrent();
    HandleScope handle_scope(isolate);
#endif
    TryCatch trycatch;
#ifdef V8_VER_4_8_API
    Local<String> source = String::NewFromUtf8(isolate, funSource.data(), NewStringType::kNormal, funSource.length()).ToLocalChecked();
    Handle<Script> script = Script::Compile(jsContext, source).ToLocalChecked();
#else
    Handle<String> source = String::New(funSource.data(), funSource.length());
    Handle<Script> script = Script::Compile(source);
#endif
    if (script.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR, exceptionString(trycatch));
    }

    Handle<Value> result = script->Run();

    if (result.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR, exceptionString(trycatch));
    }

    if (!result->IsFunction()) {
        throw MapReduceError(MAPREDUCE_SYNTAX_ERROR,
                             std::string("Invalid function: ") + funSource.c_str());
    }
#ifdef V8_VER_4_8_API
//XXX vmx 2015-10-28: check if this is the right thing
    return Handle<Function>::Cast(result);
#else
    return handleScope.Close(Handle<Function>::Cast(result));
#endif
}


static std::string exceptionString(const TryCatch &tryCatch)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(Isolate::GetCurrent());
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
#elif  V8_VER_4_8_API
    HandleScope handle_scope(Isolate::GetCurrent());
#endif
    String::Utf8Value exception(tryCatch.Exception());
    const char *exceptionString = (*exception);

    if (exceptionString) {
        Handle<Message> message = tryCatch.Message();
        return std::string(exceptionString) + " (line " +
            std::to_string(message->GetLineNumber()) + ":" +
            std::to_string(message->GetStartColumn()) + ")";
    }

    return std::string("runtime error");
}


static void loadFunctions(mapreduce_ctx_t *ctx,
                          const std::list<std::string> &function_sources)
{
#ifdef V8_POST_3_19_API
    HandleScope handleScope(Isolate::GetCurrent());
#elif  V8_PRE_3_19_API
    HandleScope handleScope;
#elif  V8_VER_4_8_API
    HandleScope handle_scope(Isolate::GetCurrent());
#endif

    ctx->functions = new function_vector_t();

    std::list<std::string>::const_iterator it = function_sources.begin();

    Local<Context> jsContext =
        Local<Context>::New(ctx->isolate, ctx->jsContext);

    for ( ; it != function_sources.end(); ++it) {
      //Handle<Function> fun = compileFunction(*it, ctx->jsContext);
      Handle<Function> fun = compileFunction(*it, jsContext);
#ifndef V8_PRE_3_19_API
        Persistent<Function> *perFn = new Persistent<Function>();
        perFn->Reset(ctx->isolate, fun);
        ctx->functions->push_back(perFn);
#else
        ctx->functions->push_back(Persistent<Function>::New(fun));
#endif
    }
}


#ifndef V8_PRE_3_19_API
static void emit(const v8::FunctionCallbackInfo<Value> &args)
#else
static Handle<Value> emit(const Arguments &args)
#endif
{
    isolate_data_t *isoData = getIsolateData();

    if (isoData->ctx->kvs == NULL) {
#ifndef V8_POST_3_19_API
        return;
#else
        return Undefined();
#endif
    }

    try {
        mapreduce_kv_t result;

        result.key   = jsonStringify(args[0]);
        result.value = jsonStringify(args[1]);
        isoData->ctx->kvs->push_back(result);

#ifndef V8_POST_3_19_API
        return;
#else
        return Undefined();
#endif

#ifdef V8_VER_4_8_API
    } catch(Local<String> &ex) {
#else
    } catch(Handle<Value> &ex) {
#endif

#ifdef V8_POST_3_19_API
        ThrowException(ex);
#elif  V8_PRE_3_19_API
        return ThrowException(ex);
#elif  V8_VER_4_8_API
        Exception::Error(ex);
#endif
    }
}


static inline isolate_data_t *getIsolateData()
{
    Isolate *isolate = Isolate::GetCurrent();
#ifdef V8_VER_4_8_API
    return reinterpret_cast<isolate_data_t*>(isolate->GetData(0));
#else
    return reinterpret_cast<isolate_data_t*>(isolate->GetData());
#endif
}


static inline mapreduce_json_t jsonStringify(const Handle<Value> &obj)
{
    isolate_data_t *isoData = getIsolateData();
    Handle<Value> args[] = { obj };
    TryCatch trycatch;
#ifndef V8_PRE_3_19_API
    Local<Function> stringifyFun = Local<Function>::New(Isolate::GetCurrent(), isoData->stringifyFun);
    Local<Object> jsonObject = Local<Object>::New(Isolate::GetCurrent(), isoData->jsonObject);
    Handle<Value> result = stringifyFun->Call(jsonObject, 1, args);
#else
    Handle<Value> result = isoData->stringifyFun->Call(isoData->jsonObject, 1, args);
#endif

    if (result.IsEmpty()) {
        throw trycatch.Exception();
    }

    mapreduce_json_t jsonResult;

    if (!result->IsUndefined()) {
        Handle<String> str = Handle<String>::Cast(result);
        jsonResult.length = str->Utf8Length();
        jsonResult.json = (char *) malloc(jsonResult.length);
        if (jsonResult.json == NULL) {
            throw std::bad_alloc();
        }
        str->WriteUtf8(jsonResult.json, jsonResult.length,
                       NULL, String::NO_NULL_TERMINATION);
    } else {
        jsonResult.length = sizeof("null") - 1;
        jsonResult.json = (char *) malloc(jsonResult.length);
        if (jsonResult.json == NULL) {
            throw std::bad_alloc();
        }
        memcpy(jsonResult.json, "null", jsonResult.length);
    }

    // Caller responsible for freeing jsonResult.json
    return jsonResult;
}


static inline Handle<Value> jsonParse(const mapreduce_json_t &thing)
{
    isolate_data_t *isoData = getIsolateData();
#ifdef V8_VER_4_8_API
    Handle<Value> args[] = { String::NewFromUtf8(Isolate::GetCurrent(), thing.json, NewStringType::kNormal, thing.length).ToLocalChecked() };
    Local<Function> jsonParseFun = Local<Function>::New(Isolate::GetCurrent(), isoData->jsonParseFun);
    Local<Object> jsonObject = Local<Object>::New(Isolate::GetCurrent(), isoData->jsonObject);
    Handle<Value> result = jsonParseFun->Call(jsonObject, 1, args);
#else
    Handle<Value> args[] = { String::New(thing.json, thing.length) };
#endif

    TryCatch trycatch;

#ifdef V8_POST_3_19_API
    Local<Function> jsonParseFun = Local<Function>::New(Isolate::GetCurrent(), isoData->jsonParseFun);
    Local<Object> jsonObject = Local<Object>::New(Isolate::GetCurrent(), isoData->jsonObject);
    Handle<Value> result = jsonParseFun->Call(jsonObject, 1, args);
#elif V8_PRE_3_19_API
    Handle<Value> result = isoData->jsonParseFun->Call(isoData->jsonObject, 1, args);
#endif

    if (result.IsEmpty()) {
        throw MapReduceError(MAPREDUCE_RUNTIME_ERROR, exceptionString(trycatch));
    }

    return result;
}


static inline void taskStarted(mapreduce_ctx_t *ctx)
{
    ctx->taskStartTime = time(NULL);
    ctx->kvs = NULL;
}


static inline void taskFinished(mapreduce_ctx_t *ctx)
{
    ctx->taskStartTime = -1;
}


static inline Handle<Array> jsonListToJsArray(const mapreduce_json_list_t &list)
{
#ifdef V8_VER_4_8_API
    Isolate *isolate = Isolate::GetCurrent();
    Handle<Array> array = Array::New(isolate, list.length);
#else
    Handle<Array> array = Array::New(list.length);
#endif

    for (int i = 0 ; i < list.length; ++i) {
        Handle<Value> v = jsonParse(list.values[i]);
#ifdef V8_VER_4_8_API
        array->Set(Number::New(isolate, i), v);
#else
        array->Set(Number::New(i), v);
#endif
    }

    return array;
}
