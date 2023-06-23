# Go queue with REST interface

Queue broker as a web service. The service handles 2 methods:

1. PUT /queue?v=message
    Put the message message in a queue named queue (the queue name can
    be any), example:


curl -XPUT http://127.0.0.1/pet?v=cat
curl -XPUT http://127.0.0.1/pet?v=dog
curl -XPUT http://127.0.0.1/role?v=manager
curl -XPUT http://127.0.0.1/role?v=executive

in response {empty body + status 200 (ok)}
in the absence of the v parameter - an empty body + status 400 (bad request)

2. GET /queue

Take (according to the FIFO principle) a message from the queue with the name queue
and return in the body of the http request, an example (the result that should be
with puts above):

curl http://127.0.0.1/pet => cat
curl http://127.0.0.1/pet => dog
curl http://127.0.0.1/pet => {empty body + status 404 (not found)}
curl http://127.0.0.1/pet => {empty body + status 404 (not found)}
curl http://127.0.0.1/role => manager
curl http://127.0.0.1/role => executive
curl http://127.0.0.1/role => {empty body + status 404 (not found)}

for GET requests, make it possible to set the timeout argument

curl http://127.0.0.1/pet?timeout=N

if there is no ready message in the queue, the recipient must wait either
until the message arrives or until the timeout expires (N - number of
seconds). If the message never appeared, return a 404 code.
Recipients must receive messages in the same order as from them
a request was made if 2 recipients are waiting for a message (use
timeout), then the first message should be received by the first
requested.

The port on which the service will listen is specified in the arguments
command line.
