Sample code for running 'persist' effects on a video.

As used in https://youtu.be/pzYVqks5DaU

This is not intended to be an out-of-the-box tool, rather a framework that
you'll need to customize to generate the desired effects.

Run persist.py as a celery worker

    # celery -A persist worker -l info --concurrency 6

main.py configures the videos to convert.
