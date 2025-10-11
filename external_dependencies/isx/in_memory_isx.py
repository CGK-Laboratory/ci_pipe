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

        
