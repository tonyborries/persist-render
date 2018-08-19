from celery import group
import csv
import os.path
from tabulate import tabulate

import persist


BASE_DIR="/opt/persist/media/"

###
# Main Process - not an async task itself

def process_video(rel_source_filename, persist_frames, skip_level, infinite_persist, speedup):
    """
    rel_source_filename is relative to BASE_DIR
    """

    print "============\nProcessing %s - Persist %d frames\n" % (rel_source_filename, persist_frames)

    project_name = ''.join(x for x in rel_source_filename if x.isalnum())
    output_name = "{}-persisted-{}{}{}{}.mp4".format(
        project_name,
        persist_frames,
        'sl' if skip_level else '',
        'i' if infinite_persist else '',
        '{}x'.format(speedup) if speedup > 1 else ''
        )

    if os.path.isfile(os.path.join(BASE_DIR, output_name)):
        print "File Exists - skipping: %s" % output_name
        return output_name

    p = persist.Persist(
        input_filename = rel_source_filename,
        output_filename = output_name,
        persisted_frames = persist_frames,
        skip_level = skip_level,
        infinite_persist = infinite_persist,
        speedup = speedup,
    )


    print "Extracting Frames..."
    t = persist.split_video_into_frames.delay(rel_source_filename, persist_frames, p.getSerializedDict())
    result = t.get()
    frames = result['frames']
    print "Completed Frame Extraction - %d frames - %s\n" % (frames, result.get('status', "UNKNOWN"))

    print "Converting Frames..."
    res = group(
        persist.generate_persisted_frame.s(p.getSerializedDict(), rel_source_filename, persist_frames, i, 1, frames, skip_level=skip_level, infinite_persist=infinite_persist)
        for i in xrange(1, frames + 1)
    )()
    res.get()
    print "Done Converting Frames\n"

    print "Assembling Frames..."
    t = persist.assemble_frames_into_video.delay(rel_source_filename, persist_frames, skip_level, infinite_persist, output_name, p.getSerializedDict())
    result = t.get()
    print "Assembled: %s" % result
    print "Completed %s\n============" % rel_source_filename
    return output_name

def speedup_video(rel_source_filename, x_times):
    """
    Speedup a video by merging x_times of frames together into a single
    frame - does not do any persistence

    rel_source_filename is relative to BASE_DIR
    """

    print "============\nSpeed-up %dx %s\n" % (x_times, rel_source_filename)

    project_name = ''.join(x for x in rel_source_filename if x.isalnum()) + 'speed'
    output_name = project_name + "-speed-%dx.mp4" % x_times

    if os.path.isfile(os.path.join(BASE_DIR, output_name)):
        print "File Exists - skipping: %s" % output_name
        return output_name

    p = persist.Persist(
        input_filename = rel_source_filename,
        output_filename = output_name,
        persisted_frames = 0,
        skip_level = False,
        infinite_persist = False,
        speedup = x_times,
    )


    print "Extracting Frames..."
    t = persist.split_video_into_frames.delay(rel_source_filename, 0, p.getSerializedDict())
    result = t.get()
    frames = result['frames']
    print "Completed Frame Extraction - %d frames - %s\n" % (frames, result.get('status', "UNKNOWN"))

    print "Converting Frames..."
    res = group(
        persist.merge_frames.s(i, i * x_times, (i + 1) * x_times, p.getSerializedDict())
        for i in xrange(1, int(frames / x_times))
    )()
    res.get()
    print "Done Converting Frames\n"

    print "Assembling Frames..."
    t = persist.assemble_frames_into_video.delay(rel_source_filename, 0, False, False, output_name, p.getSerializedDict())
    result = t.get()
    print "Assembled: %s" % result


    print "Completed %s\n============" % rel_source_filename

    return output_name

def cut_video(rel_source_filename, starttime=0, endtime=0):
    """
    Cut a clip from a video into a new file

    rel_source_filename is relative to BASE_DIR
    timestamps are float seconds
    """

    project_name = ''.join(x for x in rel_source_filename if x.isalnum()) + 'clip'
    output_name = project_name + "-{}-{}.mp4".format(starttime, endtime)

    if os.path.isfile(os.path.join(BASE_DIR, output_name)):
        print "File Exists - skipping: %s" % output_name
        return output_name

    p = persist.Persist(
        input_filename = rel_source_filename,
        output_filename = output_name,
        persisted_frames = 0,
        skip_level = False,
        infinite_persist = False,
        speedup = 0,
    )

    print "Cutting Clip..."
    t = persist.cut_clip.delay(p.getSerializedDict(), starttime, endtime)
    result = t.get()
    print "Cut: %s" % result
    print "Completed %s\n============" % output_name
    return output_name


CSV_FILENAME = 'media/config.csv'

if __name__ == "__main__":

    def _decomment(csvfile):
        for row in csvfile:
            raw = row.split('#')[0].strip()
            if raw: yield raw

    tasks = []
    with open(CSV_FILENAME, 'rb') as csvfile:
        reader = csv.reader(_decomment(csvfile), skipinitialspace=True)
        for row in reader:
            tasks.append(row)

    completed_tasks = []
    for task in tasks:
        infile, speedup, persist_frames, skip_level, infinite_persist, starttime, endtime = task
        speedup = int(speedup)
        persist_frames = int(persist_frames)
        skip_level = bool(int(skip_level))
        infinite_persist = bool(int(infinite_persist))
        starttime = float(starttime)
        endtime = float(endtime)

        f = infile

        if starttime or endtime:
            print "Cutting Clip"
            f = cut_video(f, starttime, endtime)

        if speedup != 1:
            print "Speedup {} {}x".format(f, speedup)
            f = speedup_video(f, speedup)

        if persist_frames > 0:
            print "Process {} {} {} {}".format(f, persist_frames, skip_level, infinite_persist, speedup)
            f = process_video(
                f,
                persist_frames=persist_frames,
                skip_level=skip_level,
                infinite_persist=infinite_persist,
                speedup=speedup
            )
        completed_tasks.append((
            infile,
            starttime,
            endtime,
            speedup,
            persist_frames,
            skip_level,
            infinite_persist,
            f,
        ))

    print tabulate(completed_tasks, headers=['File', 'In', 'Out', 'Speedup', 'Persist Frames', 'Skip Level', 'Infinite', 'Output'])


