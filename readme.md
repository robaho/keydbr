**About**

Remote access to keydb database. Uses Google protobufs and gRPC.

**Notes**

There is a single gRPC stream per open database,
upon which all requests are multiplexed. The client API automatically locks the stream until the previous request is
acknowledged, but this may change in the future to allow overlapping requests - that is, asynchronous handling
by the server.

**To Use**

go run cmd/server

There is a sample command line client in cmd/client which uses the client API.

**Performance**

Using the same 'performance' test as keydb, but using the remote layer:

<pre>
insert time  1000000 records =  3080 ms, usec per op  3.08019
close time  1700 ms
scan time  2129 ms, usec per op  2.129979
scan time 50%  1468 ms, usec per op  2.93625
random access time  83.98727 us per get
close time  1000 ms
scan time  1963 ms, usec per op  1.963984
scan time 50%  930 ms, usec per op  1.861844
random access time  82.95168 us per get
</pre>

**TODOs**

Implement "read ahead" for more efficient lookup over the network
