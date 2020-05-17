#!/usr/bin/env python3
from typing import Dict, List, Tuple, Union

import pandas as pd


def simplify_categories(
    df: pd.DataFrame,
    col: str,
    color_key: Union[Dict[str, str], List[str]],
    inplace: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Simplify categories in a Pandas dataframe

    :param df: Dataframe to simplify categories
    :param col: Column name of categorical series in dataframe
                that will be simplified
    :param color_key: Color mapping dictionary or list
    :param inplace: Whether to mutate ``df`` in place or not
    :return: Tuple of updated dataframe and dict containing color mapping
    :raises ValueError: If column is not categorical or if
                        fewer colors than categories
    :raises TypeError: If color_key isn't a list or dict
    """
    if not isinstance(df.dtypes[col], pd.CategoricalDtype):
        raise ValueError("selected column must be categorical")
    cats = df[col].cat.categories

    if isinstance(color_key, dict):
        missing_colors = set(cats) - color_key.keys()
        # Without simplification, datashader requires that a color_key
        # dictionary contain a color for each category.  Therefore,
        # when we simplify we will require the same.  In other words
        # the set of keys() in color_key needs to be a superset.
        if missing_colors:
            raise ValueError(
                "insufficient colors provided (%s) for the categorical fields availabile (%s)"
                % (len(cats) - len(missing_colors), len(cats))
            )
    elif isinstance(color_key, list):
        ncolors = len(color_key)
        color_key = {k: color_key[i % ncolors] for i, k in enumerate(cats)}
    else:
        raise ValueError("color_key must be dict or list")

    # TODO - benchmark/consider alternatives
    if not inplace:
        df = df.copy()
    df[col] = df[col].map(color_key)
    df[col] = df[col].astype("category")
    df[col].cat.remove_unused_categories(inplace=True)
    # at this point, categories and colors are the same thing
    # return a new color key that can be passed to shade()
    new_color_key = {x: x for x in df[col].cat.categories}
    return df, new_color_key


def replace_low_freq_inplace(
    s: pd.Series,
    threshold: int = None,
    last: int = None,
    replacement: str = "Other"
) -> None:
    """Replace low frequency categories in place

    :param s: Pandas Series or Index object
    :param threshold: Threshold below which category will be removed
    :param last: Last N categories to remove
    :param replacement: String with which to replace categories
    :raises ValueError: if ``threshold`` and ``last`` are both ``None``
                        or both not ``None``
    """
    c = s.value_counts()
    if (threshold is not None) and (last is None):
        s.cat.remove_categories(c.index[c < threshold], inplace=True)
    elif (threshold is None) and (last is not None):
        s.cat.remove_categories(c.index[last:], inplace=True)
    elif (threshold is None) and (last is None):
        raise ValueError("either threshold or last can be provided")
    else:
        raise ValueError("only threshold or last can be provided")
    s.cat.add_categories(["Other"], inplace=True)
    s.fillna("Other", inplace=True)
    s.cat.remove_unused_categories(inplace=True)
