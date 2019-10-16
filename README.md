# Meaning from Maps/Graphs - Entity Resolution in GraphFrames

[![CircleCI](https://circleci.com/gh/hendrikfrentrup/maps-meaning/tree/master.svg?style=svg)](https://circleci.com/gh/hendrikfrentrup/maps-meaning/tree/master)

Code examples for the Spark+AI Summit Europe 2019 talk "Maps and Meaning: Graph-bases Entity resolution". Dive into a toy example via the notebooks (ER-Graphframes & ER-GraphX).

For further details, or if you'd like to try this on a specific use case, please do get in touch.

Easy Ways to get started:

* Get the pyspark Docker container (with GraphFrames preinstalled). You can get it on Docker Hub [here](https://hub.docker.com/r/hendrikfrentrup/pyspark-graphframes)

* The `Dockerfile` explains the extra layer on top of the `jupyter/pyspark-notebook` base container (install of GraphFrames)

* The container can be launched via `docker-compose up`

* Jupyter notebooks are running on [`localhost:8888`](http://localhost:8888) (see `docker-compose.yml`)

* You can also run the test of the `gfresolver` to make sure everything works well from within the container.

