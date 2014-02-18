# MPRPC #

MPRPC is a simple demonstration program for msgpack-based RPC.

## Installation ##

First run `./bootstrap.sh`, which will check out required helper
libraries ([Tamer](http://github.com/kohler/tamer)). Then run
`./configure`. You may need to specify a C++11 compiler explicitly,
for example with `./configure CXX='YOUR_COMPILER -std=gnu++0x
-stdlib=libc++'`. Then run `make`.

## RPC format ##

RPCs are formatted as [msgpack](http://msgpack.org) arrays. The first
array element must be an integer; it is positive for requests (client
to server) and negative for responses (server to client). The second
array element is expected to be an integer sequence number.

## Testing ##

Run `./mprpc -l` to start a server listening on port 18029.

Run `./mprpc -c` to start a client that connects to a server on port
18029 on localhost.

Use `-p PORT` to specify a different port. For clients, use `-h HOST`
to connect to a different IPv4 host. Use `-q` to turn off verbose
output.
