from celery import group
import csv
import os.path
from tabulate import tabulate

import persist 


BASE_DIR="/opt/persist/media/"

###
# Main Process - not an async task itself

def process_video(rel_source_filename, persist_frames, skip_level=False, infinite_persist=False):
    """
    rel_source_filename is relative to BASE_DIR
    """

    print "============\nProcessing %s - Persist %d frames\n" % (rel_source_filename, persist_frames)

    project_name = ''.join(x for x in rel_source_filename if x.isalnum())
    output_name = "{}-persisted-{}{}{}.mp4".format(
        project_name,
        persist_frames,
        'sl' if skip_level else '',
        'i' if infinite_persist else ''
        )

    if os.path.isfile(os.path.join(BASE_DIR, output_name)):
        print "File Exists - skipping: %s" % output_name
        return

    print "Extracting Frames..."
    t = persist.split_video_into_frames.delay(project_name, rel_source_filename, persist_frames)
    result = t.get()
    frames = result['frames']
    print "Completed Frame Extraction - %d frames - %s\n" % (frames, result.get('status', "UNKNOWN"))

    print "Converting Frames..."
    res = group(
        persist.generate_persisted_frame.s(project_name, persist_frames, i, 1, frames, skip_level=skip_level, infinite_persist=infinite_persist)
        for i in xrange(1, frames + 1)
    )()
    res.get()
    print "Done Converting Frames\n"

    print "Assembling Frames..."
    t = persist.assemble_frames_into_video.delay(project_name, persist_frames, output_name)
    result = t.get()
    print "Assembled: %s" % result


    print "Completed %s\n============" % rel_source_filename


def speedup_video(rel_source_filename, x_times):
    """
    Speedup a video by merging x_times of frames together into a single
    frame - does not do any persistence
    
    rel_source_filename is relative to BASE_DIR
    """

    print "============\nSpeed-up %dx %s\n" % (x_times, rel_source_filename)

    project_name = ''.join(x for x in rel_source_filename if x.isalnum()) + 'speed'
    output_name = project_name + "-speed-%dx.mp4" % x_times

    if os.path.isfile(output_name):
        print "File Exists - skipping: %s" % output_name
        return output_name

    print "Extracting Frames..."
    t = persist.split_video_into_frames.delay(project_name, rel_source_filename, 0)
    result = t.get()
    frames = result['frames']
    print "Completed Frame Extraction - %d frames - %s\n" % (frames, result.get('status', "UNKNOWN"))

    print "Converting Frames..."
    res = group(
        persist.merge_frames.s(project_name, i, i * x_times, (i + 1) * x_times, 0)
        for i in xrange(1, int(frames / x_times))
    )()
    res.get()
    print "Done Converting Frames\n"

    print "Assembling Frames..."
    t = persist.assemble_frames_into_video.delay(project_name, 0, output_name)
    result = t.get()
    print "Assembled: %s" % result


    print "Completed %s\n============" % rel_source_filename

    return output_name



#f = "0505dark.MOV"
#process_video(f, persist_frames=5)
#process_video(f, persist_frames=25)
#process_video(f, persist_frames=100)
#process_video(f, persist_frames=250)
#process_video(f, persist_frames=500)
#process_video(f, persist_frames=1000)
#process_video(f, persist_frames=10000)


#f = 'DSC_0532-average-hack.MOV'
#f = speedup_video(f, 60)
#process_video(f, persist_frames=1, skip_level=True, infinite_persist=False)

#f = "DSC_0502.MOV"
#process_video(f, persist_frames=1, skip_level=True, infinite_persist=True)


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
        infile, speedup, persist_frames, skip_level, infinite_persist = task
        speedup = int(speedup)
        persist_frames = int(persist_frames)
        skip_level = bool(skip_level)
        infinite_persist = bool(infinite_persist)

        f = infile
        if speedup != 1:
            print "Speedup {} {}x".format(f, speedup)
            f = speedup_video(f, speedup)

        if persist_frames > 0:
            print "Process {} {} {} {}".format(infile, persist_frames, skip_level, infinite_persist)
            process_video(
                infile,
                persist_frames=persist_frames,
                skip_level=skip_level,
                infinite_persist=infinite_persist
            )
        completed_tasks.append((
            infile,
            speedup,
            persist_frames,
            skip_level,
            infinite_persist
        ))

    print tabulate(completed_tasks, headers=['File', 'Speedup', 'Persist Frames', 'Skip Level', 'Infinite'])


