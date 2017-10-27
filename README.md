# simple-paxos
## What's this ?
simple-paxos is a conceptual implementation of the Paxos algorithm focusing on the Paxos algorithm itself instead of the performance. 

## How simple it is?

simple-paxos choose python to implement the algorithm because python is a default script environment on almost linux machine, you can run the code directly without any lib deps. simple-paxos implements an simple event loop to handler network packet event and timeout event. These allow you quickly know how the network works with a little epoll knowledge. 

| complex things | why not      |
| ------------- |:-------------:|
| asyncio      | need python3.x and the coroutine conception should be learned  |
| C/C++      | need compile, os api is not in a nice form sepecially when set the ip address  |
| node.js | need node, javascript sepecially the newest version is not so popular as the python      |

## Runtime Requirements
* Linux with epoll
* Python2.7.x
