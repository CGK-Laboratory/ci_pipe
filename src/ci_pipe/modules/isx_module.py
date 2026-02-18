from pathlib import Path

import numpy as np

from ci_pipe.decorators import step
from ci_pipe.errors.isx_backend_not_configured_error import ISXBackendNotConfiguredError
from ci_pipe.utils.project_template import load_project_templates


class ISXModule:
    # TODO: Consider moving these constants to a different config file
    PREPROCESS_VIDEOS_STEP = "ISX Preprocess Videos"
    BANDPASS_FILTER_VIDEOS_STEP = "ISX Bandpass Filter Videos"
    MOTION_CORRECTION_VIDEOS_STEP = "ISX Motion Correction Videos"
    NORMALIZE_DFF_VIDEOS_STEP = "ISX Normalize DFF Videos"
    EXTRACT_NEURONS_PCA_ICA_STEP = "ISX Extract Neurons PCA ICA"
    DETECT_EVENTS_IN_CELLS_STEP = "ISX Detect Events In Cells"
    AUTO_ACCEPT_REJECT_CELLS_STEP = "ISX Auto Accept Reject Cells"
    EXPORT_MOVIE_TO_TIFF_STEP = "ISX Export movie to TIFF"
    EXPORT_MOVIE_TO_NWB_STEP = "ISX Export movie to NWB"
    LONGITUDINAL_REGISTRATION_STEP = "ISX Longitudinal Registration"
    PREPROCESS_VIDEOS_SUFFIX = "PP"
    BANDPASS_FILTER_VIDEOS_SUFFIX = "BP"
    MOTION_CORRECTION_VIDEOS_SUFFIX = "MC"
    MOTION_CORRECTION_VIDEOS_TRANSLATIONS_SUFFIX = "translations"
    MOTION_CORRECTION_VIDEOS_CROP_RECT_SUFFIX = "crop-rect"
    MOTION_CORRECTION_VIDEOS_MEAN_IMAGES_SUFFIX = "mean-image"
    NORMALIZE_DFF_VIDEOS_SUFFIX = "DFF"
    EXTRACT_NEURONS_PCA_ICA_VIDEOS_SUFFIX = "PCA-ICA"
    DETECT_EVENTS_IN_CELLS_SUFFIX = "ED"
    LONGITUDINAL_REGISTRATION_SUFFIX = "LR"
    LONGITUDINAL_REGISTRATION_CORRESPONDENCES_TABLE_NAME = "LR-correspondences-table"
    LONGITUDINAL_REGISTRATION_CROP_RECT_NAME = "LR-crop-rect"
    LONGITUDINAL_REGISTRATION_TRANSFORM_NAME = "LR-transform"
    GUI_VISUALIZATION_STEP = "ISX Gui Visualization"
    GUI_VISUALIZATION_SUFFIX = "GUI"

    def __init__(self, isx, ci_pipe):
        if isx is None:
            raise ISXBackendNotConfiguredError()
        self._isx = isx
        self._ci_pipe = ci_pipe

        self._lr_reference_selection_strategies = {
            'by_num_cells_desc': self._lr_by_num_cells_desc,
        }

    # TODO: Find the best way to remove repetition on these step methods, without losing clarity of what each step does

    @step(PREPROCESS_VIDEOS_STEP)
    def preprocess_videos(
            self,
            inputs,
            *,
            isx_pp_temporal_downsample_factor=1,
            isx_pp_spatial_downsample_factor=1,
            isx_pp_crop_rect=None,
            isx_pp_crop_rect_format="tlbr",
            isx_pp_fix_defective_pixels=True,
            isx_pp_trim_early_frames=True
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.PREPROCESS_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.PREPROCESS_VIDEOS_SUFFIX)

            self._isx.preprocess(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                temporal_downsample_factor=isx_pp_temporal_downsample_factor,
                spatial_downsample_factor=isx_pp_spatial_downsample_factor,
                crop_rect=isx_pp_crop_rect,
                crop_rect_format=isx_pp_crop_rect_format,
                fix_defective_pixels=isx_pp_fix_defective_pixels,
                trim_early_frames=isx_pp_trim_early_frames
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }

    @step(BANDPASS_FILTER_VIDEOS_STEP)
    def bandpass_filter_videos(
            self,
            inputs,
            *,
            isx_bp_low_cutoff=0.005,
            isx_bp_high_cutoff=0.5,
            isx_bp_retain_mean=False,
            isx_bp_subtract_global_minimum=True
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.BANDPASS_FILTER_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.BANDPASS_FILTER_VIDEOS_SUFFIX)

            self._isx.spatial_filter(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                low_cutoff=isx_bp_low_cutoff,
                high_cutoff=isx_bp_high_cutoff,
                retain_mean=isx_bp_retain_mean,
                subtract_global_minimum=isx_bp_subtract_global_minimum
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }

    @step(MOTION_CORRECTION_VIDEOS_STEP)
    def motion_correction_videos(
            self,
            inputs,
            *,
            isx_mc_series_name="series",
            isx_mc_max_translation=20,
            isx_mc_low_bandpass_cutoff=0.004,
            isx_mc_high_bandpass_cutoff=0.016,
            isx_mc_roi=None,
            isx_mc_reference_segment_index=0,
            isx_mc_reference_frame_index=0,
            isx_mc_global_registration_weight=1,
            isx_mc_preserve_input_dimensions=False
    ):
        output_videos = []
        output_translations = []
        output_crop_rects = []
        output_mean_images = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.MOTION_CORRECTION_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_video_path = self._isx.make_output_file_path(input_path, output_dir,
                                                                self.MOTION_CORRECTION_VIDEOS_SUFFIX)
            output_translations_path = self._isx.make_output_file_path(input_path, output_dir,
                                                                       self.MOTION_CORRECTION_VIDEOS_TRANSLATIONS_SUFFIX,
                                                                       ext='csv')
            output_crop_rect_path = self._isx.make_output_file_path(input_path, output_dir,
                                                                    f'{isx_mc_series_name}-{self.MOTION_CORRECTION_VIDEOS_CROP_RECT_SUFFIX}',
                                                                    ext='csv')
            output_mean_image_path = self._isx.make_output_file_path(input_path, output_dir,
                                                                     f'{isx_mc_series_name}-{self.MOTION_CORRECTION_VIDEOS_MEAN_IMAGES_SUFFIX}')

            self._isx.project_movie(
                input_movie_files=[input_path],
                output_image_file=output_mean_image_path
            )

            self._isx.motion_correct(
                input_movie_files=[input_path],
                output_movie_files=[output_video_path],
                max_translation=isx_mc_max_translation,
                low_bandpass_cutoff=isx_mc_low_bandpass_cutoff,
                high_bandpass_cutoff=isx_mc_high_bandpass_cutoff,
                roi=isx_mc_roi,
                reference_segment_index=isx_mc_reference_segment_index,
                reference_frame_index=isx_mc_reference_frame_index,
                reference_file_name=output_mean_image_path,
                global_registration_weight=isx_mc_global_registration_weight,
                output_translation_files=[output_translations_path],
                output_crop_rect_file=output_crop_rect_path,
                preserve_input_dimensions=isx_mc_preserve_input_dimensions
            )

            output_videos.append({'ids': input['ids'], 'value': output_video_path})
            output_translations.append({'ids': input['ids'], 'value': output_translations_path})
            output_crop_rects.append({'ids': input['ids'], 'value': output_crop_rect_path})
            output_mean_images.append({'ids': input['ids'], 'value': output_mean_image_path})

        return {
            'videos-isxd': output_videos,
            'motion-correction-translations': output_translations,
            'motion-correction-crop-rect': output_crop_rects,
            'motion-correction-mean-images': output_mean_images
        }

    @step(NORMALIZE_DFF_VIDEOS_STEP)
    def normalize_dff_videos(
            self,
            inputs,
            *,
            isx_dff_f0_type='mean'
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.NORMALIZE_DFF_VIDEOS_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.NORMALIZE_DFF_VIDEOS_SUFFIX)

            self._isx.dff(
                input_movie_files=[input_path],
                output_movie_files=[output_path],
                f0_type=isx_dff_f0_type
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'videos-isxd': output
        }

    @step(EXTRACT_NEURONS_PCA_ICA_STEP)
    def extract_neurons_pca_ica(
            self,
            inputs,
            *,
            isx_pca_ica_num_pcs=180,
            isx_pca_ica_num_ics=120,
            isx_pca_ica_unmix_type='spatial',
            isx_pca_ica_max_iterations=100,
            isx_pca_ica_convergence_threshold=0.00001,
            isx_pca_ica_block_size=1000,
            isx_pca_ica_auto_estimate_num_ics=False,
            isx_pca_ica_average_cell_diameter=13,
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.EXTRACT_NEURONS_PCA_ICA_STEP)

        for input in inputs('videos-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir,
                                                          self.EXTRACT_NEURONS_PCA_ICA_VIDEOS_SUFFIX)

            self._isx.pca_ica(
                input_movie_files=[input_path],
                output_cell_set_files=[output_path],
                num_pcs=isx_pca_ica_num_pcs,
                num_ics=isx_pca_ica_num_ics,
                unmix_type=isx_pca_ica_unmix_type,
                max_iterations=isx_pca_ica_max_iterations,
                convergence_threshold=isx_pca_ica_convergence_threshold,
                block_size=isx_pca_ica_block_size,
                auto_estimate_num_ics=isx_pca_ica_auto_estimate_num_ics,
                average_cell_diameter=isx_pca_ica_average_cell_diameter
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'cellsets-isxd': output
        }

    @step(DETECT_EVENTS_IN_CELLS_STEP)
    def detect_events_in_cells(
            self,
            inputs,
            *,
            isx_ed_threshold=5,
            isx_ed_tau=0.2,
            isx_ed_event_time_ref='beginning',
            isx_ed_ignore_negative_transients=True,
            isx_ed_accepted_cells_only=False
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.DETECT_EVENTS_IN_CELLS_STEP)

        for input in inputs('cellsets-isxd'):
            input_path = input['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, self.DETECT_EVENTS_IN_CELLS_SUFFIX)

            self._isx.event_detection(
                input_cell_set_files=[input_path],
                output_event_set_files=[output_path],
                threshold=isx_ed_threshold,
                tau=isx_ed_tau,
                event_time_ref=isx_ed_event_time_ref,
                ignore_negative_transients=isx_ed_ignore_negative_transients,
                accepted_cells_only=isx_ed_accepted_cells_only
            )

            output.append({'ids': input['ids'], 'value': output_path})

        return {
            'events-isxd': output
        }

    @step(AUTO_ACCEPT_REJECT_CELLS_STEP)
    def auto_accept_reject_cells(
            self,
            inputs,
            *,
            isx_acr_filters=None
    ):
        output = []
        self._ci_pipe.create_output_directory_for_next_step(self.AUTO_ACCEPT_REJECT_CELLS_STEP)

        pairs = self._ci_pipe.associate_keys_by_id('cellsets-isxd', 'events-isxd')

        for ids, cellset_path, event_path in pairs:
            output_path = self._ci_pipe.copy_file_to_output_directory(cellset_path, self.AUTO_ACCEPT_REJECT_CELLS_STEP)

            self._isx.auto_accept_reject(
                input_cell_set_files=[output_path],
                input_event_set_files=[event_path],
                filters=isx_acr_filters
            )
            output.append({'ids': ids, 'value': output_path})

        return {
            'cellsets-isxd': output
        }

    @step(EXPORT_MOVIE_TO_TIFF_STEP)
    def export_movie_to_tiff(
            self,
            inputs,
            *,
            isx_emt_write_invalid_frames=False
    ):

        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.EXPORT_MOVIE_TO_TIFF_STEP)

        for video in inputs('videos-isxd'):
            input_path = video['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, '', ext='tiff')

            self._isx.export_movie_to_tiff([input_path], output_path, write_invalid_frames=isx_emt_write_invalid_frames)

            output.append({'ids': video['ids'], 'value': output_path})

        return {
            'videos-tiff': output
        }

    @step(EXPORT_MOVIE_TO_NWB_STEP)
    def export_movie_to_nwb(
            self,
            inputs,
            *,
            isx_emn_write_invalid_frames=False
    ):

        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.EXPORT_MOVIE_TO_NWB_STEP)

        for video in inputs('videos-isxd'):
            input_path = video['value']
            output_path = self._isx.make_output_file_path(input_path, output_dir, '', ext='nwb')

            self._isx.export_movie_to_nwb([input_path], output_path, write_invalid_frames=isx_emn_write_invalid_frames)

            output.append({'ids': video['ids'], 'value': output_path})

        return {
            'videos-nwb': output
        }

    @step(LONGITUDINAL_REGISTRATION_STEP)
    def longitudinal_registration(
            self,
            inputs,
            *,
            isx_lr_reference_selection_strategy=None,
            isx_lr_min_correlation=0.5,
            isx_lr_accepted_cells_only=False
    ):
        output_videos = []
        output_cellsets = []
        input_video_paths = []
        output_video_paths = []
        input_cellset_paths = []
        output_cellset_paths = []

        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.LONGITUDINAL_REGISTRATION_STEP)
        all_ids = list(set(id for input in inputs('videos-isxd') for id in input['ids']))

        self._load_outputs_and_paths_from_inputs(inputs('videos-isxd'), output_dir, 'isxd', output_videos,
                                                 input_video_paths, output_video_paths)
        self._load_outputs_and_paths_from_inputs(inputs('cellsets-isxd'), output_dir, 'isxd', output_cellsets,
                                                 input_cellset_paths, output_cellset_paths)

        # TODO: Throw error if strategy name is invalid, consider using enum
        if isx_lr_reference_selection_strategy is not None and isx_lr_reference_selection_strategy in self._lr_reference_selection_strategies:
            input_cellset_paths, output_cellset_paths, input_video_paths, output_video_paths = self._apply_lr_reference_selection(
                isx_lr_reference_selection_strategy,
                input_cellset_paths,
                output_cellset_paths,
                input_video_paths,
                output_video_paths
            )

        output_correspondences_table_path, output_crop_rect_path, output_transform_path = self._generate_lr_output_file_paths()

        self._isx.longitudinal_registration(
            input_cell_set_files=input_cellset_paths,
            output_cell_set_files=output_cellset_paths,
            input_movie_files=input_video_paths,
            output_movie_files=output_video_paths,
            csv_file=output_correspondences_table_path,
            min_correlation=isx_lr_min_correlation,
            accepted_cells_only=isx_lr_accepted_cells_only,
            transform_csv_file=output_transform_path,
            crop_csv_file=output_crop_rect_path
        )

        return {
            'videos-isxd': output_videos,
            'cellsets-isxd': output_cellsets,
            'longitudinal-registration-correspondences-table': [
                {'ids': all_ids, 'value': output_correspondences_table_path}],
            'longitudinal-registration-crop-rect': [{'ids': all_ids, 'value': output_crop_rect_path}],
            'longitudinal-registration-transform': [{'ids': all_ids, 'value': output_transform_path}]
        }

    # Special Gui Visualizer function
    @step(GUI_VISUALIZATION_STEP)
    def create_inscopix_project(self, inputs, *, isx_cellsetname="pca-ica"):
        file_system = self._ci_pipe.file_system()
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.GUI_VISUALIZATION_STEP)

        dff_by_ids = self._group_by_ids(inputs("videos-isxd"))
        cell_by_ids = self._group_by_ids(inputs("cellsets-isxd"))
        ev_by_ids = self._group_by_ids(inputs("events-isxd"))

        common_unique_ids = sorted(set(dff_by_ids) & set(cell_by_ids) & set(ev_by_ids))
        outputs = []

        plane_template, project_template = load_project_templates()

        for ids_key in common_unique_ids:
            # sort to create stable "plane order"
            dffs = sorted(dff_by_ids[ids_key])
            cellsets = sorted(cell_by_ids[ids_key])
            events = sorted(ev_by_ids[ids_key])

            # pick base name from first dff file
            base_path = Path(dffs[0]).name
            original_movie_name = base_path.replace(f"-{self.NORMALIZE_DFF_VIDEOS_SUFFIX}.isxd", "")
            project_file = file_system.join(output_dir, f"{original_movie_name}-{self.GUI_VISUALIZATION_SUFFIX}.isxp")

            data_folder = project_file[:-5] + "_data"
            file_system.makedirs(data_folder, exist_ok=True)

            planes = []
            for dff, cellset, event in zip(dffs, cellsets, events):
                dmin, dmax = self._movie_first_frame_min_max(dff)

                parsed = plane_template
                replacements = {
                    "{eventdet_path}": event.replace("\\", "/"),
                    "{eventdet_name}": Path(event).name,
                    "{cellset_path}": cellset.replace("\\", "/"),
                    "{cellset_name}": Path(cellset).name,
                    "{DFF_path}": dff.replace("\\", "/"),
                    "{DFF_name}": Path(dff).name,
                    "{dmax}": str(dmax),
                    "{dmin}": str(dmin),
                }
                for k, v in replacements.items():
                    parsed = parsed.replace(k, v)

                planes.append(parsed)

            project_text = project_template.replace(
                "{prj_name}", Path(project_file).name).replace(
                "{plane_1ist}", ", ".join(planes)
            )

            file_system.write(project_file, project_text)
            outputs.append({"ids": list(ids_key), "value": project_file})

        return {"inscopix-projects": outputs}

    # LR reference selection

    def _lr_by_num_cells_desc(self, input_cellsets):
        cell_counts = [self._isx.CellSet.read(path).num_cells for path in input_cellsets]
        return sorted(range(len(cell_counts)), key=lambda i: cell_counts[i], reverse=True)

    # Private methods

    def _load_outputs_and_paths_from_inputs(self, inputs, output_dir, ext, outputs, input_paths, output_paths):
        for input in inputs:
            input_paths.append(input['value'])
            output_path = self._isx.make_output_file_path(input['value'], output_dir,
                                                          self.LONGITUDINAL_REGISTRATION_SUFFIX, ext=ext)
            outputs.append({'ids': input['ids'], 'value': output_path})
            output_paths.append(output_path)

    def _generate_lr_output_file_paths(self):
        output_correspondences_table_path = self._ci_pipe.file_in_output_directory(
            f"{self.LONGITUDINAL_REGISTRATION_CORRESPONDENCES_TABLE_NAME}.csv",
            self.LONGITUDINAL_REGISTRATION_STEP
        )
        output_crop_rect_path = self._ci_pipe.file_in_output_directory(
            f"{self.LONGITUDINAL_REGISTRATION_CROP_RECT_NAME}.csv",
            self.LONGITUDINAL_REGISTRATION_STEP
        )
        output_transform_path = self._ci_pipe.file_in_output_directory(
            f"{self.LONGITUDINAL_REGISTRATION_TRANSFORM_NAME}.csv",
            self.LONGITUDINAL_REGISTRATION_STEP
        )

        return output_correspondences_table_path, output_crop_rect_path, output_transform_path

    def _apply_lr_reference_selection(self, select_reference_lambda, input_cellsets, output_cellsets, input_movies,
                                      output_movies):
        result = self._lr_reference_selection_strategies[select_reference_lambda](input_cellsets)

        if all(isinstance(_, int) for _ in result):
            order = result
        else:
            order = [input_cellsets.index(p) for p in result]

        reorder = lambda lst: [lst[i] for i in order]

        return (
            reorder(input_cellsets),
            reorder(output_cellsets),
            reorder(input_movies),
            reorder(output_movies),
        )

    def _group_by_ids(self, items):
        groups = {}
        for item in items:
            groups.setdefault(tuple(item["ids"]), []).append(item["value"])
        return groups

    def _movie_first_frame_min_max(self, movie_path):
        movie = None
        try:
            movie = self._isx.Movie.read(str(movie_path))
            frame0 = movie.get_frame_data(0)
            if frame0 is None:
                raise ValueError(f"Could not read first frame from: {movie_path}")

            arr = np.asarray(frame0)
            if arr.size == 0:
                raise ValueError(f"First frame is empty for: {movie_path}")

            dmin = float(np.nanmin(arr))
            dmax = float(np.nanmax(arr))

            if not np.isfinite(dmin) or not np.isfinite(dmax):
                raise ValueError(
                    f"Non-finite min/max from first frame for: {movie_path} (dmin={dmin}, dmax={dmax})"
                )

            return dmin, dmax

        except ValueError:
            raise
        except Exception as e:
            raise RuntimeError(f"Failed to compute first-frame min/max for {movie_path}: {e}") from e
        finally:
            try:
                del movie
            except Exception:
                pass
