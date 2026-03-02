"""
ISX-specific helpers for the pipeline: metadata extraction, GPIO resolution, etc.

Used by ISX steps (e.g. e-focus / multiplane handling). May read or write
intermediate files (e.g. copying GPIO into the output folder).
"""

import os
import shutil
from pathlib import Path
import numpy as np
import warnings

def get_efocus(isx, file: str, outputfolder: str, video) -> list:
    """
    Resolve e-focus values for a given ISX file, from GPIO or acquisition metadata.

    Looks for existing converted/copied GPIO in the output folder, then the
    converted file next to the input, then the raw GPIO (copying it into the
    output folder if needed). Falls back to acquisition info when no GPIO is
    available.

    Parameters
    ----------
    isx
        ISX backend (same as passed to ISXModule), providing GpioSet, verify_deinterleave, etc.
    file : str
        Path to the ISX file (movie).
    outputfolder : str
        Output directory where intermediate GPIO may be written.
    video
        Opened ISX movie (or object with get_acquisition_info()) for metadata fallback.

    Returns
    -------
    list
        List of e-focus values (e.g. [0] or [0, 1, 2] for multiplane).
    """
    raw_gpio_file = os.path.splitext(file)[0] + ".gpio"
    updated_gpio_file = os.path.splitext(file)[0] + "_gpio.isxd"
    local_updated_gpio_file = os.path.join(outputfolder, Path(updated_gpio_file).name)

    if os.path.exists(local_updated_gpio_file):
        return get_efocus_from_gpio(isx, local_updated_gpio_file)
    if os.path.exists(updated_gpio_file):
        return get_efocus_from_gpio(isx, updated_gpio_file)
    if os.path.exists(raw_gpio_file):
        local_raw_gpio_file = os.path.join(outputfolder, Path(raw_gpio_file).name)
        shutil.copy2(raw_gpio_file, local_raw_gpio_file)
        return get_efocus_from_gpio(isx, local_raw_gpio_file)

    acquisition_info = video.get_acquisition_info().copy()
    if "Microscope Focus" in acquisition_info:
        if not isx.verify_deinterleave(file, acquisition_info["Microscope Focus"]):
            warnings.warn(
                f"Info {file}: Multiple Microscope Focus but no GPIO file",
                UserWarning,
                stacklevel=2,
            )
            return [0]
        return [acquisition_info["Microscope Focus"]]

    print(f"Info: Unable to verify Microscope Focus config in: {file}")
    return [0]


def get_efocus_from_gpio(isx, gpio_file: str) -> list:
    """
    Read e-focus channel from a GPIO file and return the video e-focus values.

    Parameters
    ----------
    isx
        ISX backend (same as passed to ISXModule), providing GpioSet.
    gpio_file : str
        Path to the GPIO file (.gpio or .isxd).

    Returns
    -------
    list
        List of integer e-focus values (one per plane) with at least
        min_frames_per_efocus frames.
    """
    gpio_set = isx.GpioSet.read(gpio_file)
    efocus_values = gpio_set.get_channel_data(gpio_set.channel_dict["e-focus"])[1]
    efocus_values, efocus_counts = np.unique(efocus_values, return_counts=True)
    min_frames_per_efocus = 100
    video_efocus = efocus_values[efocus_counts >= min_frames_per_efocus]
    if video_efocus.shape[0] >= 4:
        warnings.warn("Too many efocus detected, early frames issue.")
    return [int(v) for v in video_efocus]
