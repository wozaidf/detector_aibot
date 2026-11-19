"""Backward-compatible CLI and pickle import path.

Use train_iforest.py for the clearly named training/evaluation entry point.
This module remains because existing joblib bundles reference
``iforest_tool.MedianPreprocessor``.
"""
from train_iforest import *  # noqa: F401,F403
from train_iforest import main


if __name__ == "__main__":
    main()
