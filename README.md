# Devolve -- A Simple Ruby Tool for Distributed Computation

## Overview

Devolve is a simple singleton class for a dynamic thread pool and associated job queue.
The constructor is automatically invoked by the first call to
<strong><code>instance()</code></strong> and
starts a listener thread that listens for worker connections on a configurable port. Client
threads can add jobs by calling <strong><code>add()</code></strong> at any time.

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

Any client thread can terminate the pool by invoking
<strong><code>close()</code></strong>;
this causes the pool listener to write the __QUIT__ token to the job queue, wait for all
worker proxy threads to
end and then terminate; no new connections are accepted.

Jobs enqueued must be objects that respond to
<strong><code>get\_work()</code></strong> and
<strong><code>put\_result()</code></strong> methods. The first must return a string that
is sent to the worker; this can be a normal string or the result of marshalling an
object. The second should take a single argument which will be either:
+ __nil__, indicating that the worker crashed or some unexpected problem was encountered
  in the protocol or a bug in the worker code; or
+ the result string sent by the worker.

The <strong><code>put\_result()</code></strong> method can re-enqueue the job if the
argument is __nil__; if non-__nil__, it can dispose of the result in any suitable way,
for example, unmarshal the result (if it is not a plain string), write it to a file or a
database, add it to objects, etc. The <strong><code>get\_work()</code></strong> method
allows clients to delay fetching the actual data until it is about to be sent to a worker
without consuming memory while the job is in the queue. The application can decide whether
this string is a plain string or one that needs to be unmarshalled into an object by the
worker; the thread pool does not care.

Each worker __must__ have a unique name, which should be a short string. Workers should
do the following after connecting:

1. Send the name using <strong><code>socket.puts</code></strong>.

2. Send the process id using <strong><code>socket.puts</code></strong>.

3. Enter an infinite loop with these steps:

    1. Read a short string  using <strong><code>socket.gets</code></strong>; if this
       string is the __QUIT__ token, close the socket and terminate. Otherwise, convert it
       to an integer, say __n__, denoting the size (in bytes) of the data string to come.

    2. Read the data string of length __n__ bytes (clients can decide whether this is a
       plain string or one that needs to be unmarshalled into an object; the thread pool
       does not care).

    3. Process the input, wrap it in a <strong><code>Result</code></strong> object,
       marshal that object and send it back to the boss; it will be passed unchanged to
       the <strong><code>put\_result()</code></strong> method of the corresponding job
       object. Since the result string is a marshalled
       <strong><code>Result</code></strong>
       object, it should *never* be __nil__.

The sample application under the <strong><code>example</code></strong> subdirectory
illustrates usage. It is intended to be run on Ubuntu Linux systems. Run the boss like
this:

    cd example
    ruby -w deb-pkg.rb

Now run as many workers as you have cores, one in each terminal window where the
worker name should be different for each worker, w1, w2, etc.:

    cd example
    ruby -w worker.rb -n w1 -b localhost

Appending the <strong><code>-h</code></strong> option to either the boss or worker
invocation will display a short summary of available options.
On completion, you should see a short message indicating the number of package stanzas
processed and log files named <b><code>boss.log</code></b>,
<b><code>w1.log</code></b>, <b><code>w2.log</code></b>, etc.

Some performance numbers are given below.

## Performance:

The example application parses package descriptions on an Ubuntu Linux machine from the
file <strong><code>/var/lib/dpkg/status</code></strong>. Here are some details of a
couple of runs:

Data file: <b><code>/var/lib/dpkg/status</b></code> on an Ubuntu 12.04 system
with: 2239086 bytes, 51317 lines, 2184 package stanzas.

Machines: M1 = Intel Core i3 laptop, running Ubuntu 12.04; M2 = AMD Quad core Phenom
desktop, running Ubuntu 10.04.

Each time is the average completion time of all the worker processes; this is a better
indicator of performance than the completion time of the boss since the listener thread
has a 30s socket timeout which makes timing unreliable. The times are also affected by
the number of stanzas included in each job and controlled by the "-s" option; for example,
worker completion times drop by half when we use "-s 8".

Column titles (except the first) are the number of workers.

<table border="1">
  <tr><th>Machine</th><th> 1</th><th> 2</th><th> 3</th><th> 4</th></tr>
  <tr><td>M1</td><td>44s</td><td>22s</td><td>15s</td><td>11s</td></tr>
  <tr><td>M2</td><td>32s</td><td>16s</td><td>11s</td><td>8s</td></tr>
</table>

## Other similar tools

There are of course other tools (such as Drb and EventMachine) but they seemed more
geared towards web servers than the rather simple distributed computation I was
interested in. Some scenarios in which I find 'devolve' useful:

1. Generating large number of combinatorial objects such as graphs, permutations, etc.
   and farming out the analysis of those objects to worker processes.
2. Parsing a large file (or set of files) such as the lists of Debian packages where
   the files are naturally separated into stanzas that can be independently parsed by
   worker processes.

## Contact

I appreciate feedback, so if you have any questions, suggestions or patches, please
send me email: amberarrow at gmail with the usual extension. Thanks.

