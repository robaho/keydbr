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
insert time  1000000 records =  5770 ms, usec per op  5
close time  3088 ms
scan time  5558 ms, usec per op  5
scan time 50%  2626 ms, usec per op  5
random access time  137 us per get
close time  1002 ms
scan time  5397 ms, usec per op  5
scan time 50%  2624 ms, usec per op  5
random access time  136 us per get
</pre>

**TODOs**

Implement "read ahead" for more efficient lookup over the network
