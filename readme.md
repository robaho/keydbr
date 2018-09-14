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

**TODOs**

Need to implement remote iteration
