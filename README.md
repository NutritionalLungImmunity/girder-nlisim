# NLI Simulation Runner

A Girder plugin to run and manage NLI simulations

## Running the server

For local installation, it is recommended developers use the
provide docker-compose configuration.  (Follow the instructions
at <https://docs.docker.com/compose/install/> to install or update
your docker environment.)

With a working docker environment, run the following commands in this
directory to build and run the containers.
```bash
docker-compose build
docker-compose up
```

## Setting up the server

Navigate to <http://localhost:8080> and register a new user.  The first new user registered
will become an administrator by default.

### Create the default assetstore

While logged, navigate to the `Admin console`, click on `Assetstores`, and then
`Create a new Filesystem assetstore`.  Give the assetstore a name (e.g. `nli`) and
the directory `/data/assetstore`.  (Note: the directory is *inside* the docker container,
not on the host system.)

### Enable CORS

Click on the `Admin console`, then `System configuration`.  At the bottom of the page,
select `Advanced Settings`.  Under `CORS Allowed Origins` enter the single character, `*`,
then click `Save`.
