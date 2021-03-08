class DataFrame:
    """A dummy DataFrame object to satisfy typing, and allow for
    dask_cudf code be written alongside dask code.
    """

    def __init__(self, *args, **kwargs):
        pass

    # def __call__(self, *args, **kwargs):
    #     return self

    # def __getattr__(self, *args, **kwargs):
    #     return self
