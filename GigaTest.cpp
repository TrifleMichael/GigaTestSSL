#include <curl/curl.h>
#include <unordered_map>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <curl/curl.h>
#include <string>
#include <iostream>
#include <thread>     // get_id
#include <vector>
#include <condition_variable>
#include <mutex>

#include <chrono>   // time measurement
#include <unistd.h> // time measurement

#include <sys/types.h> /* See NOTES */
#include <sys/socket.h>
#include "GigaTest.h"


// THIS IS THE SSL TEST ! THIS IS THE SSL TEST ! THIS IS THE SSL TEST ! THIS IS THE SSL TEST !
bool aliceServer = true;
o2::ccdb::CcdbApi *api;

/*
g++ -std=c++11 GigaTest.cpp -lpthread -lcurl -luv -o GigaTest && ./GigaTest
*/

// Ideas: call handleSocket for each socket to close them (mark with flag passed outside)

/*
TODO:

- add asynchronous closeLoop call

- check what can be free'd in destructor
- free things in finalize Download

- change name "checkGlobals"
- pooling threads only when they exist

- multiple uv loop threads

Information:

- Curl multi handle automatically reuses connections. Source: https://everything.curl.dev/libcurl/connectionreuse
- Keepalive for http is set to 118 seconds by default by CURL Source: https://stackoverflow.com/questions/60141625/libcurl-how-does-connection-keep-alive-work

*/

AsynchronousDownloader::AsynchronousDownloader()
{
  // Preparing timer to be used by curl
  timeout = new uv_timer_t();
  timeout->data = this;
  uv_loop_init(&loop);
  uv_timer_init(&loop, timeout);

  // Preparing timer that will close multi handle and it's sockets some time after transfer completes
  socketTimoutTimer = new uv_timer_t();
  uv_timer_init(&loop, socketTimoutTimer);
  socketTimoutTimer->data = this;

  // Preparing curl handle
  initializeMultiHandle();  

  // Global timer
  // uv_loop runs only when there are active handles, this handle guarantees the loop won't close after finishing first batch of requests
  auto timerCheckQueueHandle = new uv_timer_t();
  timerCheckQueueHandle->data = this;
  uv_timer_init(&loop, timerCheckQueueHandle);
  uv_timer_start(timerCheckQueueHandle, checkGlobals, 100, 100);

  loopThread = new std::thread(&AsynchronousDownloader::asynchLoop, this);
}

void AsynchronousDownloader::initializeMultiHandle()
{
  multiHandleActive = true;
  curlMultiHandle = curl_multi_init();
  curl_multi_setopt(curlMultiHandle, CURLMOPT_SOCKETFUNCTION, handleSocket);
  auto socketData = new DataForSocket();
  socketData->curlm = curlMultiHandle;
  socketData->objPtr = this;
  curl_multi_setopt(curlMultiHandle, CURLMOPT_SOCKETDATA, socketData);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_TIMERFUNCTION, startTimeout);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_TIMERDATA, timeout);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_MAX_TOTAL_CONNECTIONS, maxHandlesInUse);

}

void onUVClose(uv_handle_t* handle)
{
  if (handle != NULL)
  {
    delete handle;
  }
}

void closeHandles(uv_handle_t* handle, void* arg)
{
  if (!uv_is_closing(handle))
    uv_close(handle, onUVClose);
}

AsynchronousDownloader::~AsynchronousDownloader()
{
  for(auto socketTimerPair : socketTimerMap) {
    uv_timer_stop(socketTimerPair.second);
    uv_close((uv_handle_t*)socketTimerPair.second, onUVClose);
  }

  // Close async thread
  closeLoop = true;
  loopThread->join();

  free(loopThread);

  // Close and if any handles are running then signal to close, and run loop once to close them
  // This may take more then one iteration of loop - hence the "while"
  while (UV_EBUSY == uv_loop_close(&loop)) {
    closeLoop = false;
    uv_walk(&loop, closeHandles, NULL);
    uv_run(&loop, UV_RUN_ONCE);
  }
  curl_multi_cleanup(curlMultiHandle);
}

bool alienRedirect(CURL* handle)
{
  char *url = NULL;
  curl_easy_getinfo(handle, CURLINFO_REDIRECT_URL, &url);
  if (url)
  {
    std::string urlStr(url);
    if (urlStr.find("alien") == std::string::npos)
      return false;
  }
  return true;
}

void closePolls(uv_handle_t* handle, void* arg)
{
  if (handle->type == UV_POLL) {
    if (!uv_is_closing(handle)) {
      uv_close(handle, onUVClose);
    }
  }
}

void closeMultiHandle(uv_timer_t* handle) {
  auto AD = (AsynchronousDownloader*)handle->data;
  curl_multi_cleanup(AD->curlMultiHandle);
  AD->multiHandleActive = false;

  uv_walk(&AD->loop, closePolls, NULL);
  uv_timer_stop(handle);
}

void onTimeout(uv_timer_t *req)
{
  auto AD = (AsynchronousDownloader *)req->data;
  int running_handles;
  curl_multi_socket_action(AD->curlMultiHandle, CURL_SOCKET_TIMEOUT, 0,
                           &running_handles);
  AD->checkMultiInfo();
}

// Is used to react to polling file descriptors in poll_handle
// Calls handle_socket indirectly for further reading*
// If call is finished closes handle indirectly by check multi info
void curl_perform(uv_poll_t *req, int status, int events)
{
  int running_handles;
  int flags = 0;
  if (events & UV_READABLE)
    flags |= CURL_CSELECT_IN;
  if (events & UV_WRITABLE)
    flags |= CURL_CSELECT_OUT;

  auto context = (AsynchronousDownloader::curl_context_t *)req->data;
  
  curl_multi_socket_action(context->objPtr->curlMultiHandle, context->sockfd, flags,
                           &running_handles);
  context->objPtr->checkMultiInfo();
}

// Initializes a handle using a socket and passes it to context
AsynchronousDownloader::curl_context_t *AsynchronousDownloader::createCurlContext(curl_socket_t sockfd, AsynchronousDownloader *objPtr)
{
  curl_context_t *context;

  context = (curl_context_t *)malloc(sizeof(*context));
  context->objPtr = objPtr;
  context->sockfd = sockfd;

  uv_poll_init_socket(&objPtr->loop, &context->poll_handle, sockfd);
  context->poll_handle.data = context;

  return context;
}

// Frees data from curl handle inside uv_handle*
void AsynchronousDownloader::curlCloseCB(uv_handle_t *handle)
{
  curl_context_t *context = (curl_context_t *)handle->data;
  free(context);
}

// Makes an asynchronious call to free curl context*
void AsynchronousDownloader::destroyCurlContext(curl_context_t *context)
{
  uv_close((uv_handle_t *)&context->poll_handle, curlCloseCB);
}

void callbackWrappingFunction(void (*cbFun)(void*), void* data, bool* completionFlag)
{
  cbFun(data);
  *completionFlag = true;
}

void AsynchronousDownloader::finalizeDownload(CURL* easy_handle)
{
  handlesInUse--;
  char* done_url;
  curl_easy_getinfo(easy_handle, CURLINFO_EFFECTIVE_URL, &done_url);
  // printf("%s DONE\n", done_url);

  PerformData *data;
  curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &data);
  curl_multi_remove_handle(curlMultiHandle, easy_handle);

  long code;
  curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &code);

  if (code != 200) {
    if (code != 303 && code != 304)
    {
      std::cout << "Weird code returned: " << code << "\n";
    }
    else
    {
      if (!alienRedirect(easy_handle))
        std::cout << "Redirected to a different server than alien\n";
    }
  }
  // curl_easy_getinfo(easy_handle,  CURLINFO_RESPONSE_CODE, data->codeDestination);

  if (data->callback)
  {
    bool *cbFlag = (bool *)malloc(sizeof(bool));
    *cbFlag = false;
    auto cbThread = new std::thread(&callbackWrappingFunction, data->cbFun, data->cbData, cbFlag);
    threadFlagPairVector.emplace_back(cbThread, cbFlag);
  }

  // If no requests left then signal finished based on type of operation
  if (--(*data->requestsLeft) == 0)
  {
    switch (data->type)
    {
    case BLOCKING:
      data->cv->notify_all();
      break;
    case ASYNCHRONOUS:
      *data->completionFlag = true;
      break;
    case ASYNCHRONOUS_WITH_CALLBACK:
      *data->completionFlag = true;
      // TODO launch callback
      break;
    }
  }
  free(data);

  // Check if any handles are waiting in queue
  checkHandleQueue();

  // Calling timout starts a new download
  int running_handles;
  curl_multi_socket_action(curlMultiHandle, CURL_SOCKET_TIMEOUT, 0, &running_handles);
  checkMultiInfo();

  // If no running handles left then schedule multihandle to close
  if (running_handles == 0)
  {
    uv_timer_start(socketTimoutTimer, closeMultiHandle, socketTimoutMS, 0);
    socketTimoutTimerRunning = true;
  }
}

void AsynchronousDownloader::checkMultiInfo()
{
  CURLMsg *message;
  int pending;

  while ((message = curl_multi_info_read(curlMultiHandle, &pending)))
  {
    switch (message->msg)
    {
    case CURLMSG_DONE:
    {
      /* Do not use message data after calling curl_multi_remove_handle() and
        curl_easy_cleanup(). As per curl_multi_info_read() docs:
        "WARNING: The data the returned pointer points to will not survive
        calling curl_multi_cleanup, curl_multi_remove_handle or
        curl_easy_cleanup." */
      finalizeDownload(message->easy_handle);
      
    }
    break;

    default:
      fprintf(stderr, "CURLMSG default\n");
      break;
    }
  }
}

// Connects curl timer with uv timer
int AsynchronousDownloader::startTimeout(CURLM *multi, long timeout_ms, void *userp)
{
  auto timeout = (uv_timer_t *)userp;

  if (timeout_ms < 0)
  {
    uv_timer_stop(timeout);
  }
  else
  {
    if (timeout_ms == 0)
      timeout_ms = 1; // Calling onTimout when timeout = 0 could create an infinite loop                       
    uv_timer_start(timeout, onTimeout, timeout_ms, 0);
  }
  return 0;
}

void closeHandleTimerCallback(uv_timer_t* handle)
{
  auto data = (AsynchronousDownloader::DataForClosingSocket*)handle->data;
  auto AD = data->AD;
  auto sock = data->socket;

  if (AD->socketTimerMap.find(sock) != AD->socketTimerMap.end()) {
    std::cout << "Closing socket (timer)" << sock << "\n";
    uv_timer_stop(AD->socketTimerMap[sock]);
    AD->socketTimerMap.erase(sock);
    close(sock);
    return;
  }
  std::cout << "Socket not found " << sock << " (timer)\n";
}

// Is used to react to curl_multi_socket_action
// If INOUT then assigns socket to multi handle and starts polling file descriptors in poll_handle by callback
int handleSocket(CURL *easy, curl_socket_t s, int action, void *userp,
                                         void *socketp)
{
  auto socketData = (AsynchronousDownloader::DataForSocket *)userp;
  auto AD = (AsynchronousDownloader*)socketData->objPtr;
  AsynchronousDownloader::curl_context_t *curl_context;
  int events = 0;

  switch (action)
  {
  case CURL_POLL_IN:
  case CURL_POLL_OUT:
  case CURL_POLL_INOUT:

    // Create context associated with socket and create a poll for said socket
    curl_context = socketp ? (AsynchronousDownloader::curl_context_t *)socketp : AD->createCurlContext(s, socketData->objPtr);
    curl_multi_assign(socketData->curlm, s, (void *)curl_context);

    if (action != CURL_POLL_IN)
      events |= UV_WRITABLE;
    if (action != CURL_POLL_OUT)
      events |= UV_READABLE;

    if (AD->socketTimerMap.find(s) != AD->socketTimerMap.end()) {
      uv_timer_stop(AD->socketTimerMap[s]);
    }

    uv_poll_start(&curl_context->poll_handle, events, curl_perform);
    break;
  case CURL_POLL_REMOVE:
    if (socketp)
    {
      if (AD->socketTimerMap.find(s) != AD->socketTimerMap.end()) {
        uv_timer_start(AD->socketTimerMap[s], closeHandleTimerCallback, AD->socketTimoutMS, 0);
      }

      // Stop polling the socket, remove context assiciated with it. Socket will stay open until multi handle is closed.
      uv_poll_stop(&((AsynchronousDownloader::curl_context_t *)socketp)->poll_handle);
      AD->destroyCurlContext((AsynchronousDownloader::curl_context_t *)socketp);
      curl_multi_assign(socketData->curlm, s, NULL);
    }
    break;
  default:
    abort();
  }

  return 0;
}

void AsynchronousDownloader::checkHandleQueue()
{
  if (!multiHandleActive) {
    initializeMultiHandle();
  }

  // Lock access to handle queue
  handlesQueueLock.lock();
  if (handlesToBeAdded.size() > 0)
  {
    // Postpone closing sockets
    if (socketTimoutTimerRunning) {
      uv_timer_stop(socketTimoutTimer);
      socketTimoutTimerRunning = false;
    }

    // Add handles without going over the limit
    while(handlesToBeAdded.size() > 0 && handlesInUse < maxHandlesInUse) {
      curl_multi_add_handle(curlMultiHandle, handlesToBeAdded.front());
      handlesInUse++;
      handlesToBeAdded.erase(handlesToBeAdded.begin());
    }
  }
  handlesQueueLock.unlock();
}

void checkGlobals(uv_timer_t *handle)
{
  // Check for closing signal
  auto AD = (AsynchronousDownloader*)handle->data;
  if(AD->closeLoop) {
    uv_timer_stop(handle);
    uv_stop(&AD->loop);
  }

  // Join and erase threads that finished running callback functions
  for (int i = 0; i < AD->threadFlagPairVector.size(); i++)
  {
    if (*(AD->threadFlagPairVector[i].second))
    {
      AD->threadFlagPairVector[i].first->join();
      delete (AD->threadFlagPairVector[i].first);
      delete (AD->threadFlagPairVector[i].second);
      AD->threadFlagPairVector.erase(AD->threadFlagPairVector.begin() + i);
    }
  }
}

void AsynchronousDownloader::asynchLoop()
{
  uv_run(&loop, UV_RUN_DEFAULT);
}

std::vector<CURLcode>* AsynchronousDownloader::batchAsynchPerform(std::vector<CURL*> handleVector, bool *completionFlag)
{
  auto codeVector = new std::vector<CURLcode>(handleVector.size());
  size_t *requestsLeft = new size_t();
  *requestsLeft = handleVector.size();

  handlesQueueLock.lock();
  for(int i = 0; i < handleVector.size(); i++)
  {
    auto *data = new AsynchronousDownloader::PerformData();

    data->codeDestination = &(*codeVector)[i];
    data->requestsLeft = requestsLeft;
    data->completionFlag = completionFlag;
    data->type = ASYNCHRONOUS;

    curl_easy_setopt(handleVector[i], CURLOPT_PRIVATE, data);
    handlesToBeAdded.push_back(handleVector[i]);
  }
  handlesQueueLock.unlock();
  makeLoopCheckQueueAsync();
  return codeVector;
}

CURLcode AsynchronousDownloader::blockingPerform(CURL* handle)
{
  std::vector<CURL*> handleVector;
  handleVector.push_back(handle);
  return batchBlockingPerform(handleVector).back();
}

std::vector<CURLcode> AsynchronousDownloader::batchBlockingPerform(std::vector<CURL*> handleVector)
{
  std::condition_variable cv;
  std::mutex cv_m;
  std::unique_lock<std::mutex> lk(cv_m);

  std::vector<CURLcode> codeVector(handleVector.size());
  size_t requestsLeft = handleVector.size();

  handlesQueueLock.lock();
  for(int i = 0; i < handleVector.size(); i++)
  {
    auto *data = new AsynchronousDownloader::PerformData();
    data->codeDestination = &codeVector[i];
    data->cv = &cv;
    data->type = BLOCKING;
    data->requestsLeft = &requestsLeft;

    curl_easy_setopt(handleVector[i], CURLOPT_PRIVATE, data);
    handlesToBeAdded.push_back(handleVector[i]);
  }
  handlesQueueLock.unlock();
  makeLoopCheckQueueAsync();
  cv.wait(lk);
  return codeVector;
}

// TODO turn to batch asynch with callback
CURLcode *AsynchronousDownloader::asynchPerformWithCallback(CURL* handle, bool *completionFlag, void (*cbFun)(void*), void* cbData)
{
  auto data = new AsynchronousDownloader::PerformData();
  auto code = new CURLcode();
  data->completionFlag = completionFlag;
  data->codeDestination = code;

  data->cbFun = cbFun;
  data->cbData = cbData;
  data->callback = true;
  data->type = ASYNCHRONOUS_WITH_CALLBACK;

  curl_easy_setopt(handle, CURLOPT_PRIVATE, data);

  handlesQueueLock.lock();
  handlesToBeAdded.push_back(handle);
  handlesQueueLock.unlock();

  return code;
}

void asyncUVHandleCallback(uv_async_t *handle)
{
  auto AD = (AsynchronousDownloader*)handle->data;
  uv_close((uv_handle_t*)handle, onUVClose);
  // stop handle and free its memory
  AD->checkHandleQueue();
  // uv_check_t will delete be deleted in its callback
}

void AsynchronousDownloader::makeLoopCheckQueueAsync()
{
  auto asyncHandle = new uv_async_t();
  asyncHandle->data = this;
  uv_async_init(&loop, asyncHandle, asyncUVHandleCallback);
  uv_async_send(asyncHandle);
}

// ------------------------------ TESTING ------------------------------ 

std::string extractETAG(std::string headers)
{
  auto etagLine = headers.find("ETag");
  auto etagStart = headers.find("\"", etagLine)+1;
  auto etagEnd = headers.find("\"", etagStart+1);
  return headers.substr(etagStart, etagEnd - etagStart);
}

size_t writeToString(void *contents, size_t size, size_t nmemb, std::string *dst)
{
  char *conts = (char *)contents;
  for (int i = 0; i < nmemb; i++)
  {
    (*dst) += *(conts++);
  }
  return size * nmemb;
}

void cleanAllHandles(std::vector<CURL*> handles)
{
  for(auto handle : handles)
    curl_easy_cleanup(handle);
}

void closesocket_callback(void *clientp, curl_socket_t item)
{
  auto AD = (AsynchronousDownloader*)clientp;
  if (AD->socketTimerMap.find(item) != AD->socketTimerMap.end()) {
    uv_timer_stop(AD->socketTimerMap[item]);
    AD->socketTimerMap.erase(item);
    close(item);
  }
}

curl_socket_t opensocket_callback(void *clientp, curlsocktype purpose, struct curl_sockaddr *address)
{
  auto AD = (AsynchronousDownloader*)clientp;
  auto sock = socket(address->family, address->socktype, address->protocol);

  AD->socketTimerMap[sock] = new uv_timer_t();
  uv_timer_init(&AD->loop, AD->socketTimerMap[sock]);

  auto data = new AsynchronousDownloader::DataForClosingSocket();
  data->AD = AD;
  data->socket = sock;
  AD->socketTimerMap[sock]->data = data;

  return sock;
}

void setHandleOptions(CURL* handle, std::string* dst, std::string* headers, std::string* path, AsynchronousDownloader* AD)
{
  if (AD) {
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback);
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETDATA, AD);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETDATA, AD);
  }

  curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_HEADERDATA, headers);

  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, dst);
  curl_easy_setopt(handle, CURLOPT_URL, path->c_str());

  if (aliceServer)
    api->curlSetSSLOptions(handle);
}

void setHandleOptionsForValidity(CURL* handle, std::string* dst, std::string* url, std::string* etag, AsynchronousDownloader* AD)
{

  if (AD) {
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback);
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETDATA, AD);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETDATA, AD);
  }
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, dst);
  curl_easy_setopt(handle, CURLOPT_URL, url->c_str());

  if (aliceServer)
    api->curlSetSSLOptions(handle);

  std::string etagHeader = "If-None-Match: \"" + *etag + "\"";
  struct curl_slist *curlHeaders = nullptr;
  curlHeaders = curl_slist_append(curlHeaders, etagHeader.c_str());
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, curlHeaders);
}

std::vector<std::string> createPathsFromCS()
{
  std::vector<std::string> vec;

  std::string* pathsCS;
  if (aliceServer) {
    pathsCS = &alicePathsCS;
  } else {
    pathsCS = &ccdbPathsCS;
  }

  std::string tmp;
  for(int i = 0; i < pathsCS->size(); i++) {
    if ((*pathsCS)[i] == ',') {
      vec.push_back(tmp);
      tmp = "";
    } else {
      tmp += (*pathsCS)[i];
    }
  }
  vec.push_back(tmp);
  return vec;
}

std::vector<std::string> createEtagsFromCS()
{
  std::vector<std::string> vec;

  std::string* etagsCS;
  if (aliceServer) {
    etagsCS = &aliceEtagsCS;
  } else {
    etagsCS = &ccdbEtagsCS;
  }

  std::string tmp;
  for(int i = 0; i < etagsCS->size(); i++) {
    if ((*etagsCS)[i] == ',') {
      vec.push_back(tmp);
      tmp = "";
    } else {
      tmp += (*etagsCS)[i];
    }
  }
  vec.push_back(tmp);
  return vec;
}

int64_t blockingBatchTestSockets(int pathLimit = 0, bool printResult = false)
{
  // Preparing for downloading
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  AsynchronousDownloader AD;

  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading objects
  std::cout << "Starting first batch\n";
  AD.batchBlockingPerform(handles);
  cleanAllHandles(handles);
  std::cout << "First batch completed\n";


  // Waiting for sockets to close
  sleep(5);

  std::vector<CURL*> handles2;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles2.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles2.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading again
  std::cout << "Starting second batch\n";
  AD.batchBlockingPerform(handles2);
  cleanAllHandles(handles2);
  std::cout << "Second batch completed\n";

  return 0;
}

int64_t blockingBatchTest(int pathLimit = 0, bool printResult = false)
{
  // Preparing for downloading
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  AsynchronousDownloader AD;

  auto start = std::chrono::system_clock::now();
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading objects
  AD.batchBlockingPerform(handles);

  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "BLOCKING BATCH TEST:  Download - " <<  difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t blockingBatchTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  AsynchronousDownloader AD;

  std::vector<std::string*> results;
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    auto handle = curl_easy_init();
    results.push_back(new std::string());
    handles.push_back(handle);
    setHandleOptionsForValidity(handle, results.back(), &paths[i], &etags[i], &AD);
  }

  // Checking objects validity
  AD.batchBlockingPerform(handles);

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    long code;
    curl_easy_getinfo(handles[i], CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      char* url;
      curl_easy_getinfo(handles[i], CURLINFO_EFFECTIVE_URL, &url);
      std::cout << "INVALID CODE: " << code << ", URL: " << url << "\n";
    }
  }

  // Clean handles and print time
  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "BLOCKING BATCH TEST:  Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t asynchBatchTest(int pathLimit = 0, bool printResult = false)
{
  // Preparing urls and objects to write into
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  // Preparing downloader
  AsynchronousDownloader AD;

  auto start = std::chrono::system_clock::now();

  // Preparing handles
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Performing requests
  bool requestFinished = false;
  AD.batchAsynchPerform(handles, &requestFinished);
  while (!requestFinished) sleep(0.05);

  // Cleanup and print time
  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "ASYNC BATCH TEST:     download - " << difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t asynchBatchTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();
  AsynchronousDownloader AD;

  std::vector<std::string*> results;
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    auto handle = curl_easy_init();
    results.push_back(new std::string());
    handles.push_back(handle);
    setHandleOptionsForValidity(handle, results.back(), &paths[i], &etags[i], &AD);
  }

  // Checking objects validity
  bool requestFinished = false;
  AD.batchAsynchPerform(handles, &requestFinished);
  while (!requestFinished) sleep(0.001);
  cleanAllHandles(handles);

  // Checking if objects did not change
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    long code;
    curl_easy_getinfo(handles[i], CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "ASYNC BATCH TEST:     Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t linearTest(int pathLimit = 0, bool printResult = false)
{
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;

  auto start = std::chrono::system_clock::now();
  CURL *handle = curl_easy_init();
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handle, results.back(), headers.back(), &paths[i], nullptr);
    curl_easy_perform(handle);

    long code;
    curl_easy_getinfo(handle, CURLINFO_HTTP_CODE, &code);
    if (code != 303) 
      std::cout << "Result different that 303. Received code: " << code << "\n";
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  curl_easy_cleanup(handle);
  
  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  if (printResult)
    std::cout << "LINEAR TEST:          download - " << difference << "ms.\n";
  return difference;
}

int64_t linearTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  std::vector<std::string*> results;
  CURL* handle = curl_easy_init();
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    results.push_back(new std::string());    
    setHandleOptionsForValidity(handle, results.back(), &paths[i], &etags[i], nullptr);
    
    curl_easy_perform(handle);
    long code;
    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
  }

  curl_easy_cleanup(handle);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR TEST:          Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t linearTestNoReuse(int pathLimit = 0, bool printResult = false)
{
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;

  auto start = std::chrono::system_clock::now();

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    CURL* handle = curl_easy_init();
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handle, results.back(), headers.back(), &paths[i], nullptr);
    curl_easy_perform(handle);

    long code;
    curl_easy_getinfo(handle, CURLINFO_HTTP_CODE, &code);
    if (code != 303) 
      std::cout << "Result different that 303. Received code: " << code << "\n";

    curl_easy_cleanup(handle);
  }
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR TEST no reuse:      download - " <<  difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t linearTestNoReuseValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  std::vector<std::string*> results;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    CURL* handle = curl_easy_init();
    results.push_back(new std::string());
    setHandleOptionsForValidity(handle, results.back(), &paths[i], &etags[i], nullptr);
    
    curl_easy_perform(handle);
    long code;
    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
    curl_easy_cleanup(handle);
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR NO REUSE TEST: Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t countAverageTime(int64_t (*function)(int, bool), int arg, int repeats)
{
  int64_t sum = 0;
  for(int i = 0; i < repeats; i++) {
    sum += function(arg, false);
  }
  return sum / repeats;
}

// int main()
void GigaTest()
{
  std::cout << "Test will be conducted on ";
  if (aliceServer) {
    std::cout << "https://alice-ccdb.cern.ch\n";
    api = new o2::ccdb::CcdbApi();
    api->init("https://alice-ccdb.cern.ch");
  } else {
    std::cout << "http://ccdb-test.cern.ch:8080\n";
  }

  if (curl_global_init(CURL_GLOBAL_ALL))
  {
    fprintf(stderr, "Could not init curl\n");
    return;
  }

  int testSize = 100; // max 185

  if (testSize != 0)
    std::cout << "-------------- Testing for " << testSize << " objects with " << AsynchronousDownloader::maxHandlesInUse << " parallel connections. -----------\n";
  else
    std::cout << "-------------- Testing for all objects with " << AsynchronousDownloader::maxHandlesInUse << " parallel connections. -----------\n";


  int repeats = 10;


  // Just checking for 303
  // std::cout << "Benchmarking redirect time\n";
  // std::cout << "Blocking perform: " << countAverageTime(blockingBatchTest, testSize, repeats) << "ms.\n";
  // std::cout << "Async    perform: " << countAverageTime(asynchBatchTest, testSize, repeats) << "ms.\n";
  // std::cout << "Single   handle : " << countAverageTime(linearTest, testSize, repeats) << "ms.\n";
  // std::cout << "Single no reuse : " << countAverageTime(linearTestNoReuse, testSize, repeats) << "ms.\n";


  // std::cout << "--------------------------------------------------------------------------------------------\n";

  std::cout << "Benchmarking test validity times\n";
  std::cout << "Blocking perform validity: " << countAverageTime(blockingBatchTestValidity, testSize, repeats) << "ms.\n";
  // std::cout << "Async    perform validity: " << countAverageTime(asynchBatchTestValidity, testSize, repeats) << "ms.\n";
  // std::cout << "Single   handle  validity: " << countAverageTime(linearTestValidity, testSize, repeats) << "ms.\n";
  // std::cout << "Single no reuse  validity: " << countAverageTime(linearTestNoReuseValidity, testSize, repeats) << "ms.\n";

  // blockingBatchTestSockets(0, false);

  curl_global_cleanup();
  return;
}