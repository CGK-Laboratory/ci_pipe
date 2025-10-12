class InMemoryISX():
    def __init__(self, file_system=None):
        self._file_system = file_system

    def preprocess(
        self,
        input_movie_files,
        output_movie_files,
        temporal_downsample_factor = 1,
        spatial_downsample_factor = 1,
        crop_rect = None,
        crop_rect_format = "tlbr",
        fix_defective_pixels = True,
        trim_early_frames = True
        ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")

    def spatial_filter(
        self,
        input_movie_files,
        output_movie_files,
        low_cutoff = 0.005,
        high_cutoff = 0.5,
        retain_mean = False,
        subtract_global_minimum = True
    ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")

    def motion_correct(
        self,
        input_movie_files,
        output_movie_files,
        max_translation = 20,
        low_bandpass_cutoff = 0.004,
        high_bandpass_cutoff = 0.016,
        roi = None,
        reference_segment_index = 0,
        reference_frame_index = 0,
        reference_file_name = '',
        global_registration_weight = 1,
        output_translation_files = None,
        output_crop_rect_file = None,
        preserve_input_dimensions = False
    ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")
        for output_file in output_translation_files or []:
            self._file_system.write(output_file, "")
        self._file_system.write(output_crop_rect_file, "")

    def project_movie(
        self,
        input_movie_files,
        output_image_file,
        stat_type = 'mean'
    ):
        self._file_system.write(output_image_file, "")

    def make_output_file_path(
        self,
        in_file,
        out_dir,
        suffix,
        ext="isxd"
    ):
        base = self._file_system.base_path(in_file)
        stem, _ = self._file_system.split_text(base)
        if suffix:
            stem = f"{stem}-{suffix}"
        new_filename = f"{stem}.{ext}"
        return self._file_system.join(out_dir, new_filename)

    def make_output_file_paths(
        self,
        in_files,
        out_dir,
        suffix,
        ext="isxd"
    ):

        return [
            self.make_output_file_path(in_file, out_dir, suffix, ext)
            for in_file in in_files
        ]