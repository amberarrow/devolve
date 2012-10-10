# Devolve -- A Simple Ruby Tool for Distributed Computation

## Overview

Devolve is a simple singleton class for a dynamic thread pool and associated job queue.
The constructor is automatically invoked by the first call to __`instance()`__ and
starts a listener thread that listens for worker connections on a configurable port. Client
threads can add jobs by calling __`add()`__ at any time.

When a worker connects, the pool listener spawns a new proxy thread dedicated to that
worker and resumes listening. All proxy threads run an infinite loop performing these
steps within:

1. Dequeue job from the job queue (blocking if the queue is empty).
2. If what was just dequeued was the QUIT token, put it back on the queue so other
   proxy threads can see it, close socket and terminate.
3. Otherwise, send job data to worker and wait for result.
4. Put result string in job object and resume step 1.

Any number of workers may connect and process jobs; currently select() is used to listen
for connections so that may limit the number of workers to 1024. When the select is
replaced by epoll, this limit should go away.

Any client thread can terminate the pool by invoking __`close()`__; this causes the pool
listener to write the __QUIT__ token to the job queue, wait for all worker proxy threads to
end and then terminate; no new connections are accepted.

Jobs enqueued must be objects that respond to __`get_work()`__ and __`put_result()`__
methods. The first must return a string that is sent to the worker; this can be a normal
string or a string obtained by marshalling an object. The second should take a single
argument which will be either:
+ nil, indicating that the worker crashed or some unexpected problem was encountered
  in the protocol or a bug in the worker code; or
+ the result string sent by the worker.

The __`put_result()`__ method can re-enqueue the job if the argument is nil; if non-nil,
it can dispose of the result in any suitable way, for example, unmarshal the result (if
it is not a plain string), write it to a file or a database, add it to objects, etc.
The __`get_work()`__ method allows clients to delay fetching the actual data until it is
about to be sent to a worker without consuming memory while the job is in the queue. The
application can decide whether this string is a plain string or one that needs to be
unmarshalled into an object by the worker, the thread pool does not care.

Each worker __must__ have a unique name, which should be a short string. Workers should
do the following after connecting:

1. Send the name using __`socket.puts`__.

2. Send the process id using __`socket.puts`__.

3. Enter an infinite loop with these steps:

    1. Read a short string  using __`socket.gets`__; if this string is the __QUIT__
       token, close the socket and terminate. Otherwise, convert it to an integer,
       say n, denoting the size (in bytes) of the data string to come.

    2. Read the data string of length n bytes (clients can decide whether this is a
       plain string or one that needs to be unmarshalled into an object; the thread pool
       does not care).

    3. Process the input, wrap it in a __Result__ object, marshal that object and send it
       back; it will be passed unchanged to the __`put_result()`__ method of the
       corresponding job object. This result string should _never_ be nil.

The sample application under the __`example`__ subdirectory illustrates usage. It is
intended to be run on an Ubuntu Linux system. Some performance numbers are given below.

** Performance:

The example application parses package descriptions on an Ubuntu Linux machine from the
file __`/var/lib/dpkg/status`__. Here are some details of a couple of runs:

Data file: /var/lib/dpkg/status on an Ubuntu 12.04 system with: 2239086 bytes,
51317 lines, 2184 package stanzas.

Machines: M1 = Intel Core i3 laptop, running Ubuntu 12.04; M2 = AMD Quad core Phenom
desktop, running Ubuntu 10.04.

Column titles (except the first) are the number of workers.

<table border="1">
  <tr><th>Machine</th><th> 1</th><th> 2</th><th> 3</th><th> 4</th></tr>
  <tr><td>M1</td><td>3m 04s</td><td>1m 35s</td><td>1m 21s</td><td>1m 15s</td></tr>
  <tr><td>M2</td><td>3m 01s</td><td>1m 34s</td><td>1m 06s</td><td>0m 52s</td></tr>
</table>

** Contact

I appreciate feedback, so if you have any questions, suggestions or patches, please
send me email: amberarrow at gmail with the usual extension. Thanks.

