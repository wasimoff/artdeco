# First Goal: Working offloading

## Client library

- [x] connect to message broker
- [x] convert to using nats instead of rabbitmq
- [ ] subscribe to provider list and update local list
- [ ] 

### Sample flow
- startup service, listen for new providers
- provider update -> update local cache list of available providers
- schedule request -> select one provider from local cache list and offload task

## Two concurrent tasks
- update provider list
- read and iterate provider list

## Provider App
- [ ] announce with rabbitmq
- [ ] watch connection requests
- [ ] execute wasi task