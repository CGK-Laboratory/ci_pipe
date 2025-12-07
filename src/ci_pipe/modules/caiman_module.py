from ci_pipe.decorators import step
from ci_pipe.errors.caiman_backend_not_configured_error import CaimanBackendNotConfiguredError


class CaimanModule:
    MOTION_CORRECTION_STEP = "Caiman Motion Correction"
    MOTION_CORRECTION_VIDEOS_SUFFIX = "MC"
    CNMF_STEP = "Caiman Constrained Non-negative Matrix Factorization"
    CNMF_VIDEOS_SUFFIX = "CNMF"

    def __init__(self, caiman, ci_pipe):
        if caiman is None:
            raise CaimanBackendNotConfiguredError()
        self._caiman = caiman
        self._ci_pipe = ci_pipe

    @step(MOTION_CORRECTION_STEP)
    def motion_correction(
            self,
            inputs,
            *,
            caiman_strides=(48, 48),
            caiman_overlaps=(24, 24),
            caiman_max_shifts=(6, 6),
            caiman_max_deviation_rigid=3,
            caiman_pw_rigid=True,
            caiman_shifts_opencv=True,
            caiman_border_nan='copy',
            caiman_save_movie=True
    ):
        # TODO: Think if we should grab all potential extensions accepted by motion correction
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.MOTION_CORRECTION_STEP)

        for input_data in inputs('videos-tif'):
            motion_correct_handler = self._caiman.motion_correction.MotionCorrect(
                fname=input_data['value'],
                strides=caiman_strides,
                overlaps=caiman_overlaps,
                max_shifts=caiman_max_shifts,
                max_deviation_rigid=caiman_max_deviation_rigid,
                pw_rigid=caiman_pw_rigid,
                shifts_opencv=caiman_shifts_opencv,
                border_nan=caiman_border_nan,
            )
            motion_correct_handler.motion_correct(save_movie=caiman_save_movie)
            mmap_files = motion_correct_handler.mmap_file
            mmap_path = mmap_files[0] # we are processing only one at a time, that's why we can unpack it like this

            memmapped_movie = self._caiman.load(mmap_path)

            tif_output_path = self._ci_pipe.make_output_file_path(
                mmap_path,
                output_dir,
                self.MOTION_CORRECTION_VIDEOS_SUFFIX,
                ext="tif",
            )

            memmapped_movie.save(tif_output_path)
            output.append({'ids': input_data['ids'], 'value': tif_output_path})

        return {"videos-tif": output}

    @step(CNMF_STEP)
    def cnmf(
            self,
            inputs,
            *,
            caiman_n_processes=None,
            caiman_k=5,
            caiman_gSig=[4, 4],
            caiman_gSiz=None,
            caiman_merge_thresh=0.8,
            caiman_p=2,
            caiman_dview=None,
            caiman_Ain=None,
            caiman_Cin=None,
            caiman_b_in=None,
            caiman_f_in=None,
            caiman_do_merge=True,
            caiman_ssub=2,
            caiman_tsub=2,
            caiman_p_ssub=1,
            caiman_p_tsub=1,
            caiman_method_init='greedy_roi',
            caiman_alpha_snmf=0.5,
            caiman_rf=None,
            caiman_stride=None,
            caiman_memory_fact=1,
            caiman_gnb=1,
            caiman_nb_patch=1,
            caiman_only_init_patch=False,
            caiman_method_deconvolution='oasis',
            caiman_n_pixels_per_process=4000,
            caiman_block_size_temp=5000,
            caiman_num_blocks_per_run_temp=20,
            caiman_num_blocks_per_run_spat=20,
            caiman_check_nan=True,
            caiman_skip_refinement=False,
            caiman_normalize_init=True,
            caiman_options_local_NMF=None,
            caiman_minibatch_shape=100,
            caiman_minibatch_suff_stat=3,
            caiman_update_num_comps=True,
            caiman_rval_thr=0.9,
            caiman_thresh_fitness_delta=-20,
            caiman_thresh_fitness_raw=None,
            caiman_thresh_overlap=.5,
            caiman_batch_update_suff_stat=False,
            caiman_s_min=None,
            caiman_remove_very_bad_comps=False,
            caiman_border_pix=0,
            caiman_low_rank_background=True,
            caiman_update_background_components=True,
            caiman_rolling_sum=True,
            caiman_rolling_length=100,
            caiman_min_corr=.85,
            caiman_min_pnr=20,
            caiman_ring_size_factor=1.5,
            caiman_center_psf=False,
            caiman_use_dense=True,
            caiman_deconv_flag=True,
            caiman_simultaneously=False,
            caiman_n_refit=0,
            caiman_del_duplicates=False,
            caiman_N_samples_exceptionality=None,
            caiman_max_num_added=3,
            caiman_min_num_trial=2,
            caiman_thresh_CNN_noisy=0.5,
            caiman_fr=30,
            caiman_decay_time=0.4,
            caiman_min_SNR=2.5,
            caiman_ssub_B=2,
            caiman_init_iter=2,
            caiman_sniper_mode=False,
            caiman_use_peak_max=False,
            caiman_test_both=False,
            caiman_expected_comps=500,
            caiman_params=None
    ):
        output = []
        output_dir = self._ci_pipe.create_output_directory_for_next_step(self.CNMF_STEP)

        for input_data in inputs('videos-tif'):
            cnmf_model = self._caiman.source_extraction.cnmf.CNMF(
                n_processes=caiman_n_processes,
                k=caiman_k,
                gSig=caiman_gSig,
                gSiz=caiman_gSiz,
                merge_thresh=caiman_merge_thresh,
                p=caiman_p,
                dview=caiman_dview,
                Ain=caiman_Ain,
                Cin=caiman_Cin,
                b_in=caiman_b_in,
                f_in=caiman_f_in,
                do_merge=caiman_do_merge,
                ssub=caiman_ssub,
                tsub=caiman_tsub,
                p_ssub=caiman_p_ssub,
                p_tsub=caiman_p_tsub,
                method_init=caiman_method_init,
                alpha_snmf=caiman_alpha_snmf,
                rf=caiman_rf,
                stride=caiman_stride,
                memory_fact=caiman_memory_fact,
                gnb=caiman_gnb,
                nb_patch=caiman_nb_patch,
                only_init_patch=caiman_only_init_patch,
                method_deconvolution=caiman_method_deconvolution,
                n_pixels_per_process=caiman_n_pixels_per_process,
                block_size_temp=caiman_block_size_temp,
                num_blocks_per_run_temp=caiman_num_blocks_per_run_temp,
                num_blocks_per_run_spat=caiman_num_blocks_per_run_spat,
                check_nan=caiman_check_nan,
                skip_refinement=caiman_skip_refinement,
                normalize_init=caiman_normalize_init,
                options_local_NMF=caiman_options_local_NMF,
                minibatch_shape=caiman_minibatch_shape,
                minibatch_suff_stat=caiman_minibatch_suff_stat,
                update_num_comps=caiman_update_num_comps,
                rval_thr=caiman_rval_thr,
                thresh_fitness_delta=caiman_thresh_fitness_delta,
                thresh_fitness_raw=caiman_thresh_fitness_raw,
                thresh_overlap=caiman_thresh_overlap,
                batch_update_suff_stat=caiman_batch_update_suff_stat,
                s_min=caiman_s_min,
                remove_very_bad_comps=caiman_remove_very_bad_comps,
                border_pix=caiman_border_pix,
                low_rank_background=caiman_low_rank_background,
                update_background_components=caiman_update_background_components,
                rolling_sum=caiman_rolling_sum,
                rolling_length=caiman_rolling_length,
                min_corr=caiman_min_corr,
                min_pnr=caiman_min_pnr,
                ring_size_factor=caiman_ring_size_factor,
                center_psf=caiman_center_psf,
                use_dense=caiman_use_dense,
                deconv_flag=caiman_deconv_flag,
                simultaneously=caiman_simultaneously,
                n_refit=caiman_n_refit,
                del_duplicates=caiman_del_duplicates,
                N_samples_exceptionality=caiman_N_samples_exceptionality,
                max_num_added=caiman_max_num_added,
                min_num_trial=caiman_min_num_trial,
                thresh_CNN_noisy=caiman_thresh_CNN_noisy,
                fr=caiman_fr,
                decay_time=caiman_decay_time,
                min_SNR=caiman_min_SNR,
                ssub_B=caiman_ssub_B,
                init_iter=caiman_init_iter,
                sniper_mode=caiman_sniper_mode,
                use_peak_max=caiman_use_peak_max,
                test_both=caiman_test_both,
                expected_comps=caiman_expected_comps,
                params=caiman_params
            )

            # Note: Values for this algorithm are changed within estimates object of cnmf model
            memmapped_movie = self._caiman.load(input_data['value'])
            cnmf_model.fit(images=memmapped_movie)

            hdf5_output_path = self._ci_pipe.make_output_file_path(
                input_data['value'],
                output_dir,
                self.CNMF_VIDEOS_SUFFIX,
                ext="hdf5",
            )

            cnmf_model.save(hdf5_output_path)
            output.append({'ids': input_data['ids'], 'value': hdf5_output_path})

        return {"files-hdf5": output}
