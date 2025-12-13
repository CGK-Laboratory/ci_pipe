class MultiModuleProxy:
    """
    A transparent proxy that intercepts method calls and broadcasts them 
    to the specific module of every pipeline in the MultiCIPipe.
    """
    def __init__(self, multi_pipe, module_name):
        self._multi_pipe = multi_pipe
        self._module_name = module_name

    def __getattr__(self, name):
        def dynamic_dispatch(*args, **kwargs):
            
            def action(pipeline):
                module = getattr(pipeline, self._module_name)
                
                if not hasattr(module, name):
                     raise AttributeError(f"Module '{self._module_name}' has no attribute '{name}'")
                
                method_to_call = getattr(module, name)
                method_to_call(*args, **kwargs)

            self._multi_pipe.with_pipelines_do(action)
            return self._multi_pipe

        return dynamic_dispatch