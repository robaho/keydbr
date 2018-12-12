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
insert time  1000000 records =  27845 ms, usec per op  27
close time  17247 ms
scan time  16946 ms, usec per op  16
scan time 50%  7483 ms, usec per op  14
random access time  267 us per get
close time  1000 ms
scan time  17067 ms, usec per op  17
scan time 50%  7590 ms, usec per op  15
random access time  270 us per get
</pre>

**TODOs**

Implement "read ahead" for more efficient lookup over the network
