class InMemoryISX:
    def __init__(self, file_system=None):
        self._file_system = file_system

    def preprocess(
            self,
            input_movie_files,
            output_movie_files,
            temporal_downsample_factor=1,
            spatial_downsample_factor=1,
            crop_rect=None,
            crop_rect_format="tlbr",
            fix_defective_pixels=True,
            trim_early_frames=True
    ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")

    def spatial_filter(
            self,
            input_movie_files,
            output_movie_files,
            low_cutoff=0.005,
            high_cutoff=0.5,
            retain_mean=False,
            subtract_global_minimum=True
    ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")

    def motion_correct(
            self,
            input_movie_files,
            output_movie_files,
            max_translation=20,
            low_bandpass_cutoff=0.004,
            high_bandpass_cutoff=0.016,
            roi=None,
            reference_segment_index=0,
            reference_frame_index=0,
            reference_file_name='',
            global_registration_weight=1,
            output_translation_files=None,
            output_crop_rect_file=None,
            preserve_input_dimensions=False
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
            stat_type='mean'
    ):
        self._file_system.write(output_image_file, "")

    def dff(
            self,
            input_movie_files,
            output_movie_files,
            f0_type='mean'
    ):
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")

    def pca_ica(
            self,
            input_movie_files,
            output_cell_set_files,
            num_pcs,
            num_ics=120,
            unmix_type='spatial',
            ica_temporal_weight=0,
            max_iterations=100,
            convergence_threshold=0.00001,
            block_size=1000,
            auto_estimate_num_ics=False,
            average_cell_diameter=13
    ):
        for output_file in output_cell_set_files:
            self._file_system.write(output_file, "")

    def event_detection(
            self,
            input_cell_set_files,
            output_event_set_files,
            threshold=5,
            tau=0.2,
            event_time_ref='beginning',
            ignore_negative_transients=True,
            accepted_cells_only=False
    ):
        for output_file in output_event_set_files:
            self._file_system.write(output_file, "")

    def auto_accept_reject(
            self,
            input_cell_set_files,
            input_event_set_files,
            filters=None
    ):
        pass

    def longitudinal_registration(
        self,
        input_cell_set_files,
        output_cell_set_files,
        input_movie_files=[],
        output_movie_files=[],
        csv_file='',
        min_correlation=0.5,
        accepted_cells_only=False,
        transform_csv_file='',
        crop_csv_file=''
    ):
        for output_file in output_cell_set_files:
            self._file_system.write(output_file, "")
        for output_file in output_movie_files:
            self._file_system.write(output_file, "")
        if csv_file:
            self._file_system.write(csv_file, "")
        if transform_csv_file:
            self._file_system.write(transform_csv_file, "")
        if crop_csv_file:
            self._file_system.write(crop_csv_file, "")

    @property
    def CellSet(self):
        class CellSet:
            @staticmethod
            def read(p):
                if not self._file_system.exists(p):
                    raise IOError(f"Cannot read file: {p}")
                class Dummy:
                    @property
                    def num_cells(self):
                        return 1
                return Dummy()
        return CellSet

    def export_movie_to_tiff(self,
                             input_movie_files,
                             output_movie_file,
                             write_invalid_frames=False,
                             ):

        for input_file in input_movie_files:
            self._file_system.write(output_movie_file, "")

    def export_movie_to_nwb(self,
                            input_movie_files,
                            output_movie_file,
                            write_invalid_frames=False,
                            ):
        for input_file in input_movie_files:
            self._file_system.write(output_movie_file, "")

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

    @property
    def Movie(self):
        file_system = self._file_system

        class Movie:
            def __init__(self, path):
                self._path = path

            @staticmethod
            def read(path):
                if not file_system.exists(path):
                    raise IOError(f"Cannot read movie: {path}")
                return Movie(path)

            def get_frame_data(self, index):
                return [
                    [0.0, 1.0],
                    [2.0, 3.0],
                ]

        return Movie
