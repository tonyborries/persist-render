Sample code for running 'persist' effects on a video.

As used in https://youtu.be/pzYVqks5DaU

This is not intended to be an out-of-the-box tool, rather a framework that
you'll need to customize to generate the desired effects.

This uses docker-compose to build and run the containers needed on a
single machine.

Media files are placed in the media/ directory which is mounted into
the containers. Configuration is done in a `config.csv` file placed
into the media/ directory::

    #Input File, Speed Up, Persist Frames, Skip Level, Infinite Persist
    DSC_0507.MOV, 1, 1, 1, 1

* Input File:
      The filename in the media/ directory
* Speed Up:
      If > 1, run the speedup process.
* Persist Frames:
      If > 0, how many frames to persist.
* Skip Level:
      If > 0, activate the Skip Level flag
* Infinite Persist:
      If > 0, activate the Infinite Persist flag




