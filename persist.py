
# celery -A persist worker -l info --concurrency 6

from __future__ import absolute_import, unicode_literals
from celery import Celery
from subprocess import call
import logging
import os
import os.path

logger = logging.getLogger('persist.log')

###
# Setup Celery

app = Celery(
    'persist',
    broker='redis://guest@redis//',
    backend='rpc://',
)

if __name__ == '__main__':
    app.start()


# ImageMagick Convert Memory Limits (Add these together for the total)
CONVERT_MEM_LIMIT = '4000MiB'
CONVERT_MAP_LIMIT = '2500MiB'
MAX_FRAMES_PER_RUN = 200  # need about 3 GB per 100 1080x1920 images


###
# File Locations

BASE_DIR="/opt/persist/media/"


def get_src_frame_dir(project_name, persisted_frames):
    """
    Returns the directory used for storing the source frames.
    """
    return BASE_DIR + "%s%d/src-frames/" % (project_name, persisted_frames)

def get_src_frame_format(project_name, persisted_frames):
    """
    Returns the filename format used by ffmpeg for the source frames from
    the original video.
    """
    
    return get_src_frame_dir(project_name, persisted_frames) + "frame%08d.jpg"

def get_src_frame_name(project_name, persisted_frames, frame_num):
    return get_src_frame_format(project_name, persisted_frames) % frame_num

def get_persisted_frame_format(project_name, persisted_frames):
    persisted_frame_dir = BASE_DIR + "%s-%d/persist-frames/" % (project_name, persisted_frames)
    if not os.path.isdir(persisted_frame_dir):
        os.makedirs(persisted_frame_dir)
    return persisted_frame_dir + "persist%08d.jpg" 

def get_persisted_frame_name(project_name, persisted_frames, frame_num):
    return get_persisted_frame_format(project_name, persisted_frames) % frame_num

def get_temp_frame_name(project_name, frame_num, for_frame_num):
    temp_frame_dir = "/tmp/persist-temp/%s/" % project_name
    if not os.path.isdir(temp_frame_dir):
        os.makedirs(temp_frame_dir)
    return temp_frame_dir + "temp-frame-%d-%d.jpg" % (frame_num, for_frame_num)

def count_files_in_dir(directory):
    return len(
        [
            name for name in os.listdir(directory)
                if os.path.isfile(os.path.join(directory, name))
        ]
    )

###
# Celery Tasks

@app.task
def split_video_into_frames(project_name, rel_source_filename, persisted_frames):
    """
    Split a video up into it's frames to be persisted.

    rel_source_filename is relative to BASE_DIR
    Frames are placed in a directory as determined by get_src_frame_format
    """

    src_frame_dir = get_src_frame_dir(project_name, persisted_frames)
    if os.path.isdir(src_frame_dir):
        logger.info("Src Frame Directory for %s exists, skipping frame extraction (%s)", project_name, src_frame_dir)
        frames = count_files_in_dir(src_frame_dir)
        return {'frames': frames, 'status': "Already Exists"}
    os.makedirs(src_frame_dir)
    src_frame_format = get_src_frame_format(project_name, persisted_frames)
    input_filename = BASE_DIR + rel_source_filename
    if not os.path.isfile(input_filename):
        raise Exception("Filename %s does not exist" % repr(input_filename))
    
    args = ["ffmpeg", "-i", input_filename, "-f", "image2", src_frame_format]
    if call(args):
        raise Exception("Failed running convert: %s" % repr(args))

    frames = count_files_in_dir(src_frame_dir)
    return {'frames': frames, 'status': "Extracted"}


@app.task
def generate_persisted_frame(project_name, persisted_frames, generate_frame_num, min_frame_num, max_frame_num, skip_level=False, infinite_persist=False):

    SKIP_LEVEL = skip_level

    if infinite_persist:
        # special, we'll do the entire persist in one go for optimization
        # this can only be ran by a single host
        if generate_frame_num != min_frame_num:
            # only the first frame will run this
            return "Skipped - infinite persist"

        last_persisted_frame_name = None
        for frame_num in xrange(min_frame_num, max_frame_num + 1):
            if frame_num % 100 == 0:
                logger.info("Infinite Persist: %d", frame_num)
            persisted_frame_name = get_persisted_frame_name(project_name, persisted_frames, frame_num)
            src_frame_file_name = get_src_frame_name(project_name, persisted_frames, frame_num)

            args = [
                "convert",
                "-limit", "memory", CONVERT_MEM_LIMIT, "-limit", "map", CONVERT_MAP_LIMIT,
                "-evaluate-sequence", "Max",
                src_frame_file_name
            ]
            if last_persisted_frame_name:
                args.append(last_persisted_frame_name)
            args.append(persisted_frame_name)
            if call(args):
                raise Exception("Failed running convert")
            last_persisted_frame_name = persisted_frame_name

        return "Created - Infinite"

# TEMP
#    start_frame = 45
#    if generate_frame_num < 45:
#        persisted_frames = 1
#    else:
#        persisted_frames = int((generate_frame_num - start_frame) / 3) + 1

    persisted_frame_name = get_persisted_frame_name(project_name, persisted_frames, generate_frame_num)

    if os.path.isfile(persisted_frame_name):
        logger.warning("File Exists - skipping: %s", persisted_frame_name)
        return "Already Exists"

    # get the array of filenames we'll be working with
    oldest_frame = generate_frame_num - persisted_frames + 1
    if oldest_frame < min_frame_num:
        oldest_frame = min_frame_num

    frame_num = oldest_frame
    leveled_frame_names = []
    frame_num = oldest_frame

# Average Override Hack
#    n = generate_frame_num - min_frame_num
#    if (max_frame_num - generate_frame_num) < n:
#        n = max_frame_num - generate_frame_num
#    frame_num = generate_frame_num - n
#    if frame_num < min_frame_num:
#        frame_num = min_frame_num

    while (frame_num <= generate_frame_num):
        src_frame_file_name = get_src_frame_name(project_name, persisted_frames, frame_num)
        if SKIP_LEVEL:
            leveled_frame_names.append(src_frame_file_name)
        else:
            # the level computes the 'fade'
            level = 40 * (generate_frame_num - frame_num) / persisted_frames + 10
            leveled_frame_name = get_temp_frame_name(project_name, frame_num, generate_frame_num)
            leveled_frame_names.append(leveled_frame_name)

            args = ["convert", "-level", "%d%%,100%%" % level, src_frame_file_name, leveled_frame_name]
            if call(args):
                raise Exception("Failed running convert: %s" % repr(args))

        frame_num += 1


    # Now we merge the frames down
    # To conserve memory, do these in small segments
    while len(leveled_frame_names) > MAX_FRAMES_PER_RUN:
        args = [
            "convert",
            "-limit", "memory", CONVERT_MEM_LIMIT, "-limit", "map", CONVERT_MAP_LIMIT,
            "-evaluate-sequence", "Max"
        ]
        args.extend(leveled_frame_names[0:MAX_FRAMES_PER_RUN])
        args.append(persisted_frame_name)
        if call(args):
            raise Exception("Failed running convert")
        leveled_frame_names = [persisted_frame_name] + leveled_frame_names[MAX_FRAMES_PER_RUN:]
        if len(leveled_frame_names) == 1:
            # this was the last file, no need to convert more
            leveled_frame_names = []

    if len(leveled_frame_names) > 0:
        args = [
            "convert",
            "-limit", "memory", CONVERT_MEM_LIMIT, "-limit", "map", CONVERT_MAP_LIMIT,
            "-evaluate-sequence", "Max"
        ]
        args.extend(leveled_frame_names)
        args.append(persisted_frame_name)
        if call(args):
            raise Exception("Failed running convert")

    # cleanup
    if not SKIP_LEVEL:
        args = ["rm"]
        args.extend(leveled_frame_names)
        if call(args):
            raise Exception("Failed removing temp files")

    logging.info("Completed Frame: %d", generate_frame_num)

    return "Created"

@app.task
def assemble_frames_into_video(project_name, persisted_frames, output_filename):
    """
    Reassemble a video from the new persisted frames.

    output_filename is relative to BASE_DIR
    """

    output_filename = BASE_DIR + output_filename
    persisted_frame_format = get_persisted_frame_format(project_name, persisted_frames)

    args = ["ffmpeg", "-r", "30", "-f", "image2", "-i", persisted_frame_format, "-vcodec", "libx264", output_filename]
    if call(args):
        raise Exception("Failed running assemble: %s" % repr(args))

    return {'status': 'Success'}

@app.task
def merge_frames(project_name, out_frame_num, start_frame_num, end_frame_num, persisted_frames):
    '''
    persist_frames is ignored, just used for naming
    '''
    persisted_frame_name = get_persisted_frame_name(project_name, persisted_frames, out_frame_num)
    if os.path.isfile(persisted_frame_name):
        logger.warning("File Exists - skipping: %s", persisted_frame_name)
        return "Already Exists"
    src_frames = []
    for frame_num in xrange(start_frame_num, end_frame_num + 1):
        src_frame_file_name = get_src_frame_name(project_name, persisted_frames, frame_num)
        src_frames.append(src_frame_file_name)

    args = [
        "convert",
        "-limit", "memory", CONVERT_MEM_LIMIT, "-limit", "map", CONVERT_MAP_LIMIT,
        "-evaluate-sequence", "Max",
    ]
    args.extend(src_frames)
    args.append(persisted_frame_name)
    if call(args):
        raise Exception("Failed running convert")

    logging.info("Completed Frame: %d", out_frame_num)
    return "Created - Speed Frame"
