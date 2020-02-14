from datashader.reductions import Reduction, FloatingReduction, category_codes, Preprocess
from datashader.utils import Expr, ngjit
from datashape import dshape, isnumeric, Record, Option
from datashape import coretypes as ct
from datashader.glyphs.glyph import isnull
from datashader import core
from collections import OrderedDict
import pandas as pd
import numpy as np


import xarray as xr
try:
    import cudf
except ImportError:
    cudf = None

def my_cols_to_keep(columns, glyph, agg):
    cols_to_keep = OrderedDict({col: False for col in columns})
    for col in glyph.required_columns():
        cols_to_keep[col] = True

    if hasattr(agg, 'values'):
        for subagg in agg.values:
            if subagg.column is not None:
                cols_to_keep[subagg.column] = True
    elif hasattr(agg, 'columns'):
        for column in agg.columns:
            cols_to_keep[column] = True
    elif agg.column is not None:
        cols_to_keep[agg.column] = True
    return [col for col, keepit in cols_to_keep.items() if keepit]

core._cols_to_keep = my_cols_to_keep

class category_values(Preprocess):
    """Extract multiple columns from a dataframe as a numpy array of values."""
    def __init__(self, columns):
        self.columns = list(columns)
      
    @property
    def inputs(self):
        return self.columns
    
    def apply(self, df):
        if cudf and isinstance(df, cudf.DataFrame):
            raise NotImplementedError("Need someone who understands cudf to fix this")

            import cupy
            if df[self.columns].dtype.kind == 'f':
                nullval = np.nan
            else:
                nullval = 0
            return cupy.array(df[self.columns].to_gpu_array(fillna=nullval))
        elif isinstance(df, xr.Dataset):
            raise NotImplementedError("Need someone who understands Dask to fix this")
            # DataArray could be backed by numpy or cupy array
            return df[self.columns].data
        else:
            a = df[self.columns[0]].cat.codes.values
            b = df[self.columns[1]].values
            return np.stack((a, b), axis=-1)
        
class sum_cat(Reduction):
    """Count of all elements in ``column``, grouped by category.
    Parameters
    ----------
    column : str
        Name of the column to aggregate over. Column data type must be
        categorical. Resulting aggregate has a outer dimension axis along the
        categories present.
    """
    def __init__(self, cat_column, val_column):
        self.columns = (cat_column, val_column)
        
    @property
    def cat_column(self):
        return self.columns[0]
    
    @property
    def val_column(self):
        return self.columns[1]   
    
    def validate(self, in_dshape):
        if not self.cat_column in in_dshape.dict:
            raise ValueError("specified column not found")
        if not self.val_column in in_dshape.dict:
            raise ValueError("specified column not found")

        if not isinstance(in_dshape.measure[self.cat_column], ct.Categorical):
            raise ValueError("input must be categorical")
        if not isnumeric(in_dshape.measure[self.val_column]):
            raise ValueError("input must be numeric")

    def out_dshape(self, input_dshape):
        cats = input_dshape.measure[self.cat_column].categories
        return dshape(Record([(c, ct.int32) for c in cats]))

    @property
    def inputs(self):
        return (category_values(self.columns),)

    def _build_create(self, out_dshape):
        n_cats = len(out_dshape.measure.fields)
        return lambda shape, array_module: array_module.zeros(
            shape + (n_cats,), dtype='i4'
        )

    def _build_append(self, dshape, schema, cuda=False):
        if cuda:
            if self.columns is None:
                return self._append_no_field_cuda
            else:
                return self._append_cuda
        else:
            if self.columns is None:
                return self._append_no_field
            else:
                return self._append
            
    @staticmethod
    @ngjit
    def _append(x, y, agg, field):
        agg[y, x, field[0]] += field[1]

    @staticmethod
    @ngjit
    def _append_cuda(x, y, agg, field):
        nb_cuda.atomic.add(agg, (y, x, field), 1)

    @staticmethod
    def _combine(aggs):
        return aggs.sum(axis=0, dtype='i4')

    def _build_finalize(self, dshape):
        cats = list(dshape[self.cat_column].categories)

        def finalize(bases, cuda=False, **kwargs):
            dims = kwargs['dims'] + [self.cat_column]

            coords = kwargs['coords']
            coords[self.cat_column] = cats
            return xr.DataArray(bases[0], dims=dims, coords=coords)
        return finalize