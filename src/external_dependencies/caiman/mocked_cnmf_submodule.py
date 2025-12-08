class MockedCNMFModule:
    def __init__(self, file_system):
        self._file_system = file_system

    def CNMF(
            self,
            n_processes=None,
            k=5,
            gSig=[4, 4],
            gSiz=None,
            merge_thresh=0.8,
            p=2,
            dview=None,
            Ain=None,
            Cin=None,
            b_in=None,
            f_in=None,
            do_merge=True,
            ssub=2,
            tsub=2,
            p_ssub=1,
            p_tsub=1,
            method_init='greedy_roi',
            alpha_snmf=0.5,
            rf=None,
            stride=None,
            memory_fact=1,
            gnb=1,
            nb_patch=1,
            only_init_patch=False,
            method_deconvolution='oasis',
            n_pixels_per_process=4000,
            block_size_temp=5000,
            num_blocks_per_run_temp=20,
            num_blocks_per_run_spat=20,
            check_nan=True,
            skip_refinement=False,
            normalize_init=True,
            options_local_NMF=None,
            minibatch_shape=100,
            minibatch_suff_stat=3,
            update_num_comps=True,
            rval_thr=0.9,
            thresh_fitness_delta=-20,
            thresh_fitness_raw=None,
            thresh_overlap=.5,
            batch_update_suff_stat=False,
            s_min=None,
            remove_very_bad_comps=False,
            border_pix=0,
            low_rank_background=True,
            update_background_components=True,
            rolling_sum=True,
            rolling_length=100,
            min_corr=.85,
            min_pnr=20,
            ring_size_factor=1.5,
            center_psf=False,
            use_dense=True,
            deconv_flag=True,
            simultaneously=False,
            n_refit=0,
            del_duplicates=False,
            N_samples_exceptionality=None,
            max_num_added=3,
            min_num_trial=2,
            thresh_CNN_noisy=0.5,
            fr=30,
            decay_time=0.4,
            min_SNR=2.5,
            ssub_B=2,
            init_iter=2,
            sniper_mode=False,
            use_peak_max=False,
            test_both=False,
            expected_comps=500,
            params=None
    ):
        return MockedCNMFModule(
            file_system=self._file_system
        )

    def fit(self, images=None):
        pass

    def estimates(self):
        pass

    def save(self, path):
        self._file_system.write(path, "")
